"""Execute one schedule entry under one named configuration, and write its evidence.

PB-T2 injects no fault of any kind. This driver runs the apparatus end to end and
records what it saw, which for the control run at run_id 0 must be nothing: zero
duplicated, zero lost, 600 expected records matched, under both configurations.
That result is an apparatus check and never a claim result. If it is ever
non-zero, that is a harness defect and it blocks the matrix (ADR-0002, ADR-0003).

Four phases, each a separate callable, so PB-T3 can host any of them in a
subprocess and SIGKILL it without restructuring this module:

1. **ingest**  produce the run's side effects to the input topic
2. **process** consume them and write sink topic A then sink topic B, committing
   offsets the way the configuration says to
3. **verify**  read both sink topics back under read_committed
4. **diff**    compare each sink ledger against the expected ledger

Sinks A and B are Kafka topics, not local stores. That is what puts the measured
effect inside the transaction's reach, and ADR-0003 records why it has to be: with
the sinks outside Kafka, a kill between them either duplicates under both
configurations or is absorbed under both, and either way the number measures the
sink design rather than the delivery configuration.

INV-P3 governs everything below. There is one sink-writing path and one
verification path, and neither branches on the configuration. The only
configuration-dependent code is the transaction bracket around a saga, which is
the difference CLAIMS.md names.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from proofbench.config import Settings, repo_root
from proofbench.core.configs import RunConfiguration, build_both, build_configuration
from proofbench.core.evidence import ledger_jsonable, write_json, write_json_gz
from proofbench.core.faults import FaultInjectingWriter, NoFault, SagaFaultInjector
from proofbench.core.ledger_diff import KeyedLedgerDiffer
from proofbench.core.recovery import (
    ApparatusFailure,
    RecoveryBudget,
    TransactionOutcome,
    classify,
    resume_saga_index,
)
from proofbench.core.saga import Saga, expand_sagas, expected_ledger, observed_record
from proofbench.core.state import unattributable_losses
from proofbench.core.topics import (
    delete_consumer_groups,
    provision,
    read_to_end,
    read_to_end_with_offsets,
)
from proofbench.core.trace import load_trace
from proofbench.core.txn import (
    PHASE_INGEST,
    PHASE_PROCESS,
    ROLE_INGEST,
    ROLE_SINK,
    AccountedProducer,
    TransactionLedger,
)
from proofbench.core.window import (
    WindowFacts,
    WindowState,
    assert_boundary_discriminates,
    is_within_fault_window,
    why_apparatus_failure,
)
from proofbench.interfaces.ledger import LedgerDiff, SideEffectRecord

# How long a transactional call may take before the client gives up. Passed
# explicitly because the alternative is a default that differs between client
# versions, and this harness pins its inputs rather than inheriting them.
_TXN_TIMEOUT_S = 60.0

# How long flush may take to drain the producer queue. Exceeding it is an
# apparatus failure rather than a loss: records still in the queue never reached
# the broker, and counting them as lost would attribute a client-side stall to
# the delivery configuration.
_FLUSH_TIMEOUT_S = 60.0

# The batch wait and the stall budget are tunables, so they live in Settings rather
# than here, and tests/unit/test_timeout_relationships.py gates the relationships
# between them and the fault durations. Two different quantities that PB-T2 conflated
# because with ``poll`` they coincided; see the comments in config.py for the measured
# reason they cannot be one number.

STATUS_CLEAN = "clean"
STATUS_NOT_CLEAN = "not_clean"
STATUS_APPARATUS_FAILURE = "apparatus_failure"

# How many times the recovery path serves the consumer's queue to let a stale group
# membership be restored. A rejoin is a JoinGroup and a SyncGroup round trip once the
# broker is answering, so a few short polls are ample; the bound exists so a consumer
# that cannot rejoin fails the run rather than spinning.
_REJOIN_POLL_ATTEMPTS = 5

EVIDENCE_DISCLAIMER = "apparatus check, not a claim result"

# The window state a run with no fault has: none of the four conditions hold, so every
# delivery failure on such a run is an apparatus failure. Named rather than inlined so
# the default is visibly the strict one.
_NO_WINDOW = WindowFacts(
    entry_names_a_fault=False,
    fault_has_fired=False,
    window_closed=True,
    budget_exhausted=True,
)


@dataclass(frozen=True, slots=True)
class SinkOutcome:
    """What one sink topic held, and how it compared to what was expected."""

    name: str
    topic: str
    records_sent: int
    records_visible: int
    diff: LedgerDiff
    observed: tuple[SideEffectRecord, ...]

    @property
    def is_clean(self) -> bool:
        return self.diff.is_clean

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "sink": self.name,
            "topic": self.topic,
            "records_sent": self.records_sent,
            "records_visible": self.records_visible,
            "duplicated": len(self.diff.duplicated),
            "lost": len(self.diff.lost),
            "is_clean": self.is_clean,
        }


@dataclass(frozen=True, slots=True)
class RunResult:
    """Everything one run of one configuration produced."""

    run_id: int
    configuration: RunConfiguration
    schedule_entry: dict[str, Any]
    expected: tuple[SideEffectRecord, ...]
    sinks: tuple[SinkOutcome, ...]
    budget: RecoveryBudget
    transactions: TransactionLedger
    process_stats: dict[str, int]
    status: str

    @property
    def is_clean(self) -> bool:
        return self.status == STATUS_CLEAN

    def summary(self) -> dict[str, Any]:
        """The run summary written into evidence and printed by the operator script.

        ``transactions_committed`` and ``transactions_aborted`` keep the names
        ADR-0003 section 3 gives them, but they are now totals over calls the run
        actually made rather than a formula applied to the saga count. The
        ``transactions`` block carries the per-phase, per-role breakdown, because
        an abort in ingest and an abort in process say different things about what
        the fault did and a single integer cannot tell them apart.
        """
        return {
            "artifact": EVIDENCE_DISCLAIMER,
            "run_id": self.run_id,
            "configuration": self.configuration.name,
            "fault_type": self.schedule_entry["fault_type"],
            "fault_injected": self.schedule_entry["fault_point"] is not None,
            "status": self.status,
            "is_clean": self.is_clean,
            "expected_records": len(self.expected),
            "transactions_committed": self.transactions.committed,
            "transactions_aborted": self.transactions.aborted,
            "transactions": self.transactions.to_jsonable(),
            "recovery": self.budget.to_jsonable(),
            # What the consume loop actually saw. largest_batch is the one that
            # matters for interpreting a C2 number: consumer_max_batch_records is
            # only "the direct determinant of C2 loss" if the client really handed
            # over batches of that size, and a run whose batches came back as single
            # records would have a one-record commit-ahead window while looking
            # identical in every other field.
            "process": dict(self.process_stats),
            "sinks": [sink.to_jsonable() for sink in self.sinks],
        }


class DeliveryError(Exception):
    """Records did not reach the broker. Deliberately says nothing about whose fault.

    Neutral on purpose, and that is the whole point of the type. Whether a failed
    send is part of an injected fault or a break in the apparatus is not a property
    of the send: it is a property of when the send happened relative to the fault
    window, and ``_Sender`` cannot know that. So the sender reports the facts and
    the caller, which holds the window state and the recovery budget, decides.

    Carries the client's error objects rather than strings, because the ADR-0003
    section 6 contract branches on ``retriable()``, ``txn_requires_abort()`` and
    ``fatal()``, and a formatted message cannot be asked those questions.
    """

    def __init__(
        self,
        message: str,
        errors: tuple[Any, ...],
        still_queued: int,
        failed_keys: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.errors = errors
        self.still_queued = still_queued
        # Which records failed, not merely how many. The attributability invariant's
        # third route keys on the specific record, so a window-level "something failed
        # here" would not be enough to distinguish an explained loss from an apparatus
        # break that happened to coincide with the outage.
        self.failed_keys = failed_keys


class DeliveryFailure(ApparatusFailure):
    """A record never reached the broker, and it was not part of any injected fault.

    Distinct from loss on purpose. Loss is a measured outcome of a delivery
    configuration under an injected fault. This is the harness failing to send at
    all, and folding it into loss would attribute a client-side problem to the
    configuration under test, which would inflate C2 for free.

    No longer raised by the sender. It is raised by the caller, and only after the
    fault-window boundary in ``core/window.py`` has said the failure lies outside
    every expected window. PB-T2 raised it unconditionally, which would have ended
    all twelve broker executions unscored.
    """


class _Sender:
    """A producer plus the delivery errors it reported.

    The callback exists because ``produce`` is asynchronous and, without a
    transaction to fail, a rejected record under the baseline would otherwise
    vanish silently and be counted as loss. That would be an apparatus artifact
    wearing the costume of a finding, so it is caught here and raised instead.
    """

    def __init__(
        self,
        conf: dict[str, Any],
        ledger: TransactionLedger,
        phase: str,
        role: str,
    ) -> None:
        from confluent_kafka import Producer

        self.producer = Producer(conf)
        # The client's error objects, not strings: the recovery contract branches on
        # retriable(), txn_requires_abort() and fatal(), and a formatted message
        # cannot be asked those questions.
        self.errors: list[Any] = []
        self.error_topics: list[str] = []
        # The idempotency keys of records whose delivery failed permanently. The
        # attributability invariant's third route is RECORD-LEVEL, so it needs to know
        # which record failed and not merely that something did. The Kafka message key
        # is the idempotency key (see ingest and write_saga_to_sinks), so the callback
        # already has it and PB-T2 simply discarded it.
        self.failed_keys: list[str] = []
        self.sent = 0
        # Constructed for both configurations, exercised only by the transactional
        # one. Keeping it unconditional is what lets the two configurations share
        # one sender: under the baseline nothing ever calls it and its counts stay
        # at zero, which is then an observation rather than an assumption.
        self.txn = AccountedProducer(self.producer, ledger, phase, role, _TXN_TIMEOUT_S)

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            self.errors.append(error)
            self.error_topics.append(str(message.topic()) if message else "unknown")
            if message is not None and message.key() is not None:
                self.failed_keys.append(bytes(message.key()).decode("utf-8", "replace"))

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self.producer.produce(
            topic, key=key.encode("utf-8"), value=value, on_delivery=self._on_delivery
        )
        self.sent += 1

    def flush(self) -> None:
        """Drain the queue and raise on anything that did not make it.

        The error list is cleared on the way out, which PB-T2 never did. Left
        uncleared, one delivery error made every later ``flush`` on the same sender
        raise for the rest of the run: harmless while no run was expected to survive
        a delivery error, and fatal now that the broker-outage runs are expected to
        do exactly that. A single transient error would have poisoned every
        subsequent saga and the run would have reported total loss.
        """
        remaining = self.producer.flush(_FLUSH_TIMEOUT_S)
        errors = tuple(self.errors)
        failed = tuple(self.failed_keys)
        self.errors.clear()
        self.error_topics.clear()
        self.failed_keys.clear()

        if remaining:
            # Not routed through the fault-window boundary, in either direction.
            # message.timeout.ms is set below the flush timeout and a gate holds it
            # there, so librdkafka must resolve every queued record, by delivery or
            # by a reported error, well inside this window. Records still queued
            # afterwards mean the client itself is stuck, which is an apparatus
            # break whether or not a fault window happens to be open.
            raise DeliveryFailure(
                f"{remaining} record(s) were still queued after {_FLUSH_TIMEOUT_S:.0f}s, "
                f"which is longer than message.timeout.ms; the client is stuck rather "
                f"than the broker being absent, so this run reports no result"
            )
        if errors:
            raise DeliveryError(
                f"{len(errors)} record(s) failed delivery, starting with {errors[0]}",
                errors=errors,
                still_queued=0,
                failed_keys=failed,
            )


def resolve_delivery_error(
    failure: DeliveryError,
    state: WindowState,
    budget: RecoveryBudget,
) -> TransactionOutcome:
    """Decide what a failed delivery is, and return what to do about it.

    The boundary the T-prompt requires, in one place, with both branches visible
    beside each other:

    - **Inside an expected fault window**, on a run whose schedule entry names a
      fault, the failure is part of the fault. It goes to the ADR-0003 section 6
      contract: ``classify`` returns retry, abort and replay, or discard and re-init
      with the same transactional id, and the choice is recorded on the budget.
    - **Outside a fault window, or once the recovery budget is exhausted**, it is an
      apparatus failure. ``DeliveryFailure`` propagates, the run records
      ``run_status: apparatus_failure``, writes whatever evidence it holds, and is
      never scored.

    Getting this backwards in either direction is fatal to the result. Too strict
    and the twelve broker executions vanish, taking the most interesting rows of the
    matrix with them. Too loose and a genuine apparatus break is scored as a
    finding, which is worse: a client-side stall would surface as lost side effects
    and inflate C2 invisibly.

    Under the baseline this is the only error surface there is, because the baseline
    makes no transactional call at all, so ``classify`` never sees anything else. A
    delivery error inside the window whose offset the consumer already committed is
    a lost side effect, and the loss is recorded by the ledger diff rather than
    here. This function's job is only to keep the run alive so that the diff can
    see it.
    """
    # The boundary fails open and silently, so it is asked two questions with known
    # answers before it is trusted with a real one.
    assert_boundary_discriminates()
    if not is_within_fault_window(state):
        raise DeliveryFailure(
            f"{failure}. This is outside any expected fault window, because "
            f"{why_apparatus_failure(state)}. The run reports no result."
        ) from failure

    # The first reported error decides, and the rest are recorded. The contract is
    # per-call rather than per-record: one failing produce inside one transaction is
    # one recovery decision, and taking the cheapest correct response to the first
    # error is what ADR-0003 section 6 fixes the predicate order for.
    outcome = classify(failure.errors[0])
    budget.record(outcome, f"delivery failed inside the fault window: {failure}")
    return outcome


def resolve_transactional_error(
    exc: Any,
    state: WindowState,
    budget: RecoveryBudget,
) -> TransactionOutcome:
    """The same boundary, for a failed transactional call rather than a failed send.

    ADR-0003 section 6 is written almost entirely about this case: "a broker restart
    makes ``commit_transaction`` retriable while the coordinator re-elects", and fencing
    surfaces as a fatal error on a transactional call. But
    ``send_offsets_to_transaction`` and ``commit_transaction`` raise ``KafkaException``
    directly rather than reporting through the delivery callback, so they took a
    completely different route out of the phase and never reached the contract at all.

    The boundary is identical, deliberately. Whether a failed transactional call is
    part of an injected fault or a break in the apparatus is the same question with the
    same four conditions, and answering it twice in two places is how the two answers
    start to differ.
    """
    assert_boundary_discriminates()
    if not is_within_fault_window(state):
        raise DeliveryFailure(
            f"a transactional call failed: {exc}. This is outside any expected fault "
            f"window, because {why_apparatus_failure(state)}. The run reports no result."
        ) from exc

    error = exc.args[0] if exc.args else None
    if error is None or not hasattr(error, "retriable"):
        # Nothing classifiable. ADR-0003 section 6: an error matching none of the three
        # classes is treated as fatal rather than retried, because in a measurement
        # harness an unclassified condition must not be folded into a number.
        budget.record(TransactionOutcome.REINIT_PRODUCER, f"unclassifiable failure: {exc}")
        return TransactionOutcome.REINIT_PRODUCER

    outcome = classify(error)
    budget.record(outcome, f"transactional call failed inside the fault window: {exc}")
    return outcome


def load_schedule_entry(run_id: int, settings: Settings) -> dict[str, Any]:
    """Read one entry from the committed, frozen schedule.

    Read from disk rather than regenerated, so a run consumes the artifact a
    reader can inspect. tests/unit/test_schedule_frozen.py is what proves the two
    are the same thing.
    """
    payload = json.loads((repo_root() / settings.schedule_path).read_text(encoding="utf-8"))
    for entry in payload["runs"]:
        if int(entry["run_id"]) == run_id:
            return dict(entry)
    raise ApparatusFailure(
        f"run_id {run_id} is not in the committed schedule; it holds {len(payload['runs'])} runs"
    )


# --------------------------------------------------------------------------
# Phase 1: ingest
# --------------------------------------------------------------------------


def durable_saga_indices(
    configuration: RunConfiguration, steps_per_saga: int
) -> tuple[set[int], dict[str, int]]:
    """Read the input topic back and report which sagas are durably complete.

    The ingest phase's half of the ADR-0003 section 7 resume contract: "The durable
    state is the input topic, read back at startup." A saga counts as complete only
    when all ``steps_per_saga`` of its steps are visible, so a half-sent saga is
    resumed rather than skipped, and ``resume_saga_index`` then picks the first index
    not known to be complete without stepping over a gap.

    Read through the **verifier** configuration, which is identical in both
    configurations once ``group.id`` is stripped. That is deliberate and it is not an
    INV-P3 widening: no client map changes, an existing identical map is used at a new
    call site, and it guarantees the durability decision is ``read_committed`` in both
    configurations. Under the good configuration that hides the killed producer's
    aborted transaction, so a half-sent saga is correctly seen as incomplete; under
    the baseline ``read_committed`` returns the non-transactional writes in full, so
    the baseline sees everything that landed. One rule, one semantics, two outcomes
    produced by the allow-listed settings rather than by a branch.

    Also returns the idempotency key to offset map, which the attributability
    invariant consumes. Reading it here costs nothing extra: the records are already
    in hand.
    """
    records = read_to_end_with_offsets(dict(configuration.verifier), configuration.topics.input)

    seen: dict[int, set[int]] = {}
    key_offsets: dict[str, int] = {}
    for offset, value in records:
        payload: dict[str, Any] = json.loads(value.decode("utf-8"))
        saga_index = int(payload["saga_index"])
        seen.setdefault(saga_index, set()).add(int(payload["step_index"]))
        # First occurrence wins: a baseline re-send duplicates a key at a later
        # offset, and the earlier one is the record whose fate the gap explains.
        key_offsets.setdefault(str(payload["idempotency_key"]), offset)

    complete = {index for index, steps in seen.items() if len(steps) >= steps_per_saga}
    return complete, key_offsets


def ingest(
    configuration: RunConfiguration,
    sagas: tuple[Saga, ...],
    ledger: TransactionLedger,
    injector: SagaFaultInjector | None = None,
) -> dict[str, int]:
    """Produce the run's side effects to the input topic, resuming if restarted.

    Transactional under the good configuration, one transaction per saga, because
    CLAIMS.md names an idempotent transactional producer and the fault menu
    includes producer_sigkill_mid_send, which kills exactly this producer. A
    half-sent saga has to abort rather than half-land.

    Every transactional call goes through ``sender.txn``, which counts it. There is
    no path here that brackets a transaction without recording that it did.

    **The ordering of the first two steps is load-bearing, and it is the one that is
    easy to get backwards.** ``init_transactions`` runs *before* the durability read,
    not after. The reason is termination rather than visibility: a producer killed
    mid-transaction leaves that transaction open, and an open transaction parks the
    partition's Last Stable Offset at its first offset, which a ``read_committed``
    consumer cannot advance past. The read-back would then stall and die with a
    message naming neither the transaction nor the cause. ``init_transactions`` with
    the same transactional id bumps the epoch and aborts what the dead one left open,
    the Last Stable Offset advances, and the question stops arising. The visibility
    argument, that ``read_committed`` hides uncommitted records either way, is true
    and is not the reason.

    The read-back is unconditional rather than restricted to restarts. On a fresh run
    the topic was just provisioned and is empty, so it costs one cheap read and
    returns a resume point of 0, and the alternative would be a branch on "is this a
    restart" that the two configurations would have to agree about.
    """
    # Inert unless a caller supplies a live one, so every existing call site and every
    # non-fault run behaves exactly as it did.
    injector = injector if injector is not None else NoFault()

    sender = _Sender(dict(configuration.ingest_producer), ledger, PHASE_INGEST, ROLE_INGEST)
    transactional = configuration.transactional
    if transactional:
        sender.txn.init()

    steps_per_saga = len(sagas[0].steps) if sagas else 0
    complete, _ = durable_saga_indices(configuration, steps_per_saga)
    start = resume_saga_index(complete)

    for saga in sagas[start:]:
        if transactional:
            sender.txn.begin()
        for step_index, step in enumerate(saga.steps):
            sender.produce(
                configuration.topics.input, step.record.idempotency_key, step.payload_bytes()
            )
            # The mid-send window, and the only fault seam in this phase. Inert on
            # every run whose entry names no producer fault, which is 34 of the 42
            # executions.
            injector.step_produced(saga.saga_index, step_index)
        sender.flush()
        if transactional:
            sender.txn.commit()

    return {
        "records_sent": sender.sent,
        "resumed_at_saga": start,
        "durable_before": len(complete),
    }


# --------------------------------------------------------------------------
# Phase 2: process
# --------------------------------------------------------------------------


class SinkWriter(Protocol):
    """What ``write_saga_to_sinks`` needs, so the ordering is testable offline.

    Structural rather than the concrete ``_Sender``, because the property below
    is the frozen sink ordering, and a test that needed a broker to check it
    would be a test nobody runs.
    """

    def produce(self, topic: str, key: str, value: bytes) -> None: ...

    def flush(self) -> None: ...


def write_saga_to_sinks(
    writer: SinkWriter, sinks: tuple[str, str], records: Sequence[tuple[str, bytes]]
) -> None:
    """Write one saga's side effects to sink A, then to sink B. Never the reverse.

    The frozen ordering, fixed in ADR-0002 and retained by ADR-0003: sink A is
    durable before sink B is attempted. It is a function rather than a loop
    inlined into ``process`` for one reason. On a run with no fault the ordering
    is unobservable, because both sinks end up holding everything either way, so
    nothing in the control run can catch it being reversed. PB-T3's
    consumer_sigkill_between_sinks depends on it entirely: the kill has to leave A
    present and B absent, and with the order reversed that fault would produce the
    mirror image of the case CLAIMS.md names.

    So the ordering is pinned here, by a unit test with a recording writer, rather
    than left to a comment and an integration run that cannot see it.

    The flush between the two sinks is what makes the ordering real rather than
    nominal. Without it both sets of records would sit in one producer queue and
    reach the broker together, and "A before B" would describe the order of two
    function calls rather than the order of two writes.
    """
    first, second = sinks
    for topic in (first, second):
        for key, value in records:
            writer.produce(topic, key, value)
        writer.flush()


def process(
    configuration: RunConfiguration,
    settings: Settings,
    expected_sagas: int,
    budget: RecoveryBudget,
    ledger: TransactionLedger,
    is_control: bool = False,
    injector: SagaFaultInjector | None = None,
    window: WindowState | None = None,
    progress: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    """Consume the input topic and write both sinks. Returns records sent per sink.

    One sink-writing path, shared by both configurations: produce every step of
    the saga to sink A, flush, then produce every step to sink B, flush. That
    ordering is the frozen one (ADR-0002, retained by ADR-0003). Under the
    baseline it yields a genuinely observable partial write when a kill lands
    between the two; under the good configuration the abort makes the partial
    write unobservable, and preventing it is the property being demonstrated.

    The only configuration-dependent code here is the transaction bracket, which
    is the difference CLAIMS.md names. Commit placement is not a branch: it is
    enable.auto.offset.store plus enable.auto.commit, both client settings.

    **Records arrive by ``consume``, not by ``poll``, and that is the whole point of
    the frozen batch size.** ``consumer_max_batch_records`` is frozen at 100, is
    emitted into ``docs/run_schedule.json``, and ``config.py`` describes it as "the
    direct determinant of C2 loss" on the grounds that it bounds what has been
    committed but not yet applied at the kill instant. ADR-0002's cross-client table
    maps it to "the ``num_messages`` argument to ``Consumer.consume()``". PB-T2 used
    ``poll``, which hands over one record at a time, so with
    ``enable.auto.offset.store=true`` the stored offset ran at most a single record
    ahead of applied work rather than up to a hundred. The frozen artifact described
    a window the code did not have, and the constant that decides C2's loss
    mechanism reached no client at all. Using ``consume`` moves the code toward what
    was already frozen.

    **Sagas are grouped by ``saga_id``, not by counting to M.** Identical behaviour
    on an aligned stream, so the control run is unchanged, and correct behaviour on a
    resumed one. After a kill the baseline's committed offset can sit mid-saga, so a
    restarted consumer starts mid-saga and a count-of-M grouping would staple the
    tail of one saga to the head of the next. Grouping on the identity the payload
    carries cannot be misaligned by where the stream happens to resume.
    """
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    injector = injector if injector is not None else NoFault()
    # No fault window on a run that names no fault, which is what makes every delivery
    # failure on such a run an apparatus failure.
    window = window if window is not None else _NO_WINDOW

    consumer = Consumer(dict(configuration.consumer))
    sender = _Sender(dict(configuration.sink_producer), ledger, PHASE_PROCESS, ROLE_SINK)
    # The writer the sink path sees is the decorated one. write_saga_to_sinks is
    # untouched, and so is the unit test that pins the frozen ordering it implements:
    # that test is the only guard on a decision the control run cannot observe, so the
    # fault seam goes around the function rather than into it.
    writer = FaultInjectingWriter(sender, injector, lambda: sender.txn.transaction_open)
    transactional = configuration.transactional
    if transactional:
        sender.txn.init()

    sink_a, sink_b = configuration.topics.sinks
    per_sink: dict[str, int] = {sink_a: 0, sink_b: 0}
    buffered: list[tuple[str, bytes]] = []
    buffered_saga: str | None = None
    buffered_index: int | None = None
    last_position: TopicPartition | None = None
    sagas_done = 0
    partial_groups = 0
    # Where this attempt started and where it stopped applying work. The gap between
    # one attempt's last_applied and the next attempt's resumed_at is the set of
    # input-topic offsets nothing ever processed, which is what makes a lost side
    # effect attributable to a specific restart rather than merely reported.
    resumed_at: int | None = None
    last_applied: int | None = None
    # The largest batch the client actually handed over. Recorded because the frozen
    # consumer_max_batch_records is only "the direct determinant of C2 loss" if the
    # client really does deliver batches of that size: a run whose batches all came
    # back as one record would have a one-record commit-ahead window and would look
    # exactly like a run with the constant wired up. Measured, not assumed.
    largest_batch = 0
    # Idempotency keys whose delivery failed permanently INSIDE an open fault window.
    permanently_failed: set[str] = set()

    def rebuild_sender() -> None:
        """Discard a dead producer and construct a new one with the SAME id.

        ADR-0003 section 6's third response, and the reason the transactional id is
        stable per run, configuration and role. ``init_transactions`` bumps the epoch
        for that id and aborts whatever the previous epoch left open, which is the only
        mechanism that cleans up after a producer that died mid-transaction. Minting a
        new id would leave the dead epoch's transaction open until
        ``transaction.timeout.ms`` expired, with the Last Stable Offset parked behind it
        and every read_committed consumer blocked on that partition.
        """
        nonlocal sender, writer
        sender.txn.forget_open_transaction()
        sender = _Sender(dict(configuration.sink_producer), ledger, PHASE_PROCESS, ROLE_SINK)
        writer = FaultInjectingWriter(sender, injector, lambda: sender.txn.transaction_open)
        if transactional:
            sender.txn.init()

    def write_group_with_recovery() -> None:
        """Write one saga's group, applying the ADR-0003 section 6 contract on failure.

        This is where the fault-window boundary is actually consulted, and it was
        missing until a broker-fault smoke run walked into it: ``resolve_delivery_error``
        existed, was tested from both directions, and was called from nowhere at all, so
        a ``DeliveryError`` propagated straight out of the phase and every broker
        execution ended unscored. Exactly the defect the boundary was written to
        prevent, reintroduced by not wiring it up.

        The boundary decides whether a failure is part of the fault or a break in the
        apparatus; the contract decides what to do about the ones that are part of it.
        Out-of-window failures raise through, which is what ends the run honestly.
        """
        from confluent_kafka import KafkaException

        def rejoin_consumer() -> None:
            """Service the client so a stale group membership is restored, and take
            nothing from the queue while doing it.

            The cycle 1 void: a broker restart invalidates group membership,
            ``send_offsets_to_transaction`` reads membership from
            ``consumer_group_metadata()``, and a consumer rejoins only when polled.
            ADR-0003 section 6 fixes the response to an abortable error as "abort, then
            replay that saga"; under the good configuration a replay commits offsets
            inside the transaction, so membership is a mechanical precondition of the
            frozen action. Unconditional, with no branch on the configuration.

            **The cycle 2 artifact, and why this shape.** The first repair queued
            whatever the poll returned and let the main loop drain it. That was wrong:
            the recovery runs from inside the batch loop while ``buffered`` is
            mid-accumulation, so re-delivered records were appended a second time and a
            saga went out as a five-record group. The repair manufactured the
            duplication it was written to avoid.

            The partitions are therefore paused first, so the poll cannot hand over a
            record at all and the queue that caused the artifact does not exist. Two
            measured facts behind that choice: ``poll(0)`` still returns whatever is
            already buffered locally, so a zero timeout reduces the risk without
            removing it; and a paused partition returns nothing even across a rebalance.

            The pause is not trusted on its own. If a record is returned anyway the run
            is abandoned rather than the record being queued or dropped, because either
            of those is the apparatus inventing a number and neither is worth the
            convenience.
            """
            assignment = consumer.assignment()
            consumer.pause(assignment)
            try:
                for _ in range(_REJOIN_POLL_ATTEMPTS):
                    message = consumer.poll(settings.consume_batch_wait_ms / 1000.0)
                    if message is not None and message.error() is None:
                        raise ApparatusFailure(
                            "the consumer returned a record while its partitions were "
                            "paused for a rejoin. Queueing it would duplicate work the "
                            "batch loop has already buffered, which is the cycle 2 "
                            "artifact, and dropping it would manufacture loss. The run "
                            "reports no result rather than either."
                        )
            finally:
                consumer.resume(assignment)

        def close_any_open_transaction() -> None:
            """Leave the producer with no transaction open, so a replay can begin one.

            Mechanics, not reclassification. The recovery budget records what
            ``classify`` decided, because that is the contract's answer and it is
            evidence. This is what makes a replay possible at all, and the transaction
            ledger counts it because it is an abort that actually happened. Two records
            of two different things, both true.
            """
            if not sender.txn.transaction_open:
                return
            try:
                sender.txn.abort()
            except KafkaException:
                # The producer could not even abort, so it is past recovering through
                # this object. ADR-0003 section 6's third response: discard it and
                # rebuild with the same transactional id.
                rebuild_sender()

        for _ in range(budget.max_reinits + 2):
            try:
                write_group()
                return
            except KafkaException as exc:
                # A failed transactional call, which ADR-0003 section 6 is mostly
                # about: "a broker restart makes commit_transaction retriable while the
                # coordinator re-elects". send_offsets_to_transaction and
                # commit_transaction raise here rather than through the delivery
                # callback, so without this branch they bypassed the contract entirely
                # and ended the run unscored. Found by a broker smoke run, not by
                # reading the code.
                outcome = resolve_transactional_error(exc, window, budget)
            except DeliveryError as failure:
                outcome = resolve_delivery_error(failure, window, budget)
                # Recorded only AFTER the boundary accepted the failure as in-window,
                # so the third attribution route can never be reached by a failure the
                # boundary would have called an apparatus break.
                permanently_failed.update(failure.failed_keys)

            if outcome is TransactionOutcome.REINIT_PRODUCER:
                rebuild_sender()
                continue

            # Deliberately NO branch on the configuration here, and that is a decision
            # rather than an omission. An earlier version returned early for the
            # baseline, on the reasoning that a non-transactional producer has nothing
            # to abort and nothing to replay, so the records simply do not land and the
            # loss is the measurement. That reasoning is defensible and the code was
            # wrong anyway: choosing to retry produces duplication and choosing not to
            # produces loss, so a configuration branch here would be the HARNESS
            # deciding which failure mode the baseline exhibits.
            #
            # INV-P3 settles it. The recovery code is identical in both configurations
            # and the outcomes diverge only through the allow-listed settings: under the
            # good configuration the replay is covered by a transaction and an idempotent
            # producer, so nothing duplicates; under the baseline it is covered by
            # neither, so it does. That divergence is produced by enable.idempotence and
            # transactional.id, which is exactly where CLAIMS.md puts it.
            #
            # Any transaction left open by the failed attempt has to be closed before
            # the group is written again, or the replay begins one inside another.
            # Found by a broker smoke run: a retriable failure after begin left the
            # transaction open and the retry hit the saga-boundary invariant.
            close_any_open_transaction()
            # And the consumer is given the chance to rejoin its group before the
            # replay, because a saga replay commits offsets and that needs membership.
            # Unconditional: see rejoin_consumer. This is the cycle 1 void's repair.
            rejoin_consumer()
            continue

        raise DeliveryFailure(
            "the sink write did not succeed within the recovery budget, so the run "
            "reports no result rather than a partial one"
        )

    def assert_group_shape() -> None:
        """A group is one saga's steps, at most once each. Checked before it is written.

        The gate that would have made cycle 2 impossible, and it is cheaper and strictly
        earlier than any analysis of what landed in the sink. The cycle 2 artifact was a
        five-record group carrying saga 166's first two steps twice; by the time it
        reached the sink it looked like two commits and took a live reproduction to
        diagnose. Here it raises at the write, naming the saga.

        A short group is legitimate and is not an error: a baseline resume can begin
        mid-saga, and the trailing partial group at EOF is written rather than raised
        because dropping it would manufacture loss. What is never legitimate is more
        records than the saga has steps, a repeated step name, or two saga ids in one
        group. Each of those means the buffer accumulated something twice.
        """
        if not buffered:
            return
        keys = [key for key, _ in buffered]
        saga_ids = {key.rsplit(":", 1)[0] for key in keys}
        steps = [key.rsplit(":", 1)[1] for key in keys]

        if len(buffered) > settings.steps_per_saga:
            raise ApparatusFailure(
                f"a saga group holds {len(buffered)} records where a saga has "
                f"{settings.steps_per_saga} steps: {keys}. The buffer accumulated the "
                f"same records more than once, which would write a duplicate the "
                f"delivery configuration did not cause."
            )
        if len(set(steps)) != len(steps):
            raise ApparatusFailure(
                f"a saga group repeats a step name: {keys}. Each step appears at most "
                f"once in a saga, so a repeat means the buffer accumulated it twice."
            )
        if len(saga_ids) != 1:
            raise ApparatusFailure(
                f"a saga group spans {len(saga_ids)} saga ids: {sorted(saga_ids)}. The "
                f"frozen transaction boundary is one saga, so a group covering two is "
                f"not the boundary the contract names."
            )

    def write_group() -> None:
        """Write the buffered saga to both sinks and commit the way the config says.

        One path for both configurations. The transaction bracket is the only
        configuration-dependent code, and the offsets travel inside it under the good
        configuration because that is what C1 names.
        """
        nonlocal buffered, buffered_saga, buffered_index, last_applied
        # Tells the injector which saga this is, and resets the writer's per-saga
        # flush counter so "the first flush is sink A's" stays true.
        assert_group_shape()
        writer.saga_started(buffered_index if buffered_index is not None else -1)
        if transactional:
            sender.txn.begin()
        # Sink A is durable before sink B is attempted. One path, no branch.
        write_saga_to_sinks(writer, (sink_a, sink_b), buffered)
        per_sink[sink_a] += len(buffered)
        per_sink[sink_b] += len(buffered)
        if transactional:
            assert last_position is not None, "a group cannot close before a record arrived"
            sender.producer.send_offsets_to_transaction(
                [last_position], consumer.consumer_group_metadata(), _TXN_TIMEOUT_S
            )
            sender.txn.commit()
        # Recorded only once the group is durable, so a gap can never be explained by
        # work that was written but never committed.
        assert last_position is not None, "a group cannot close before a record arrived"
        last_applied = last_position.offset
        # Recorded durably as the phase goes, because a SIGKILLed phase cannot write
        # down how far it got and the attributability invariant needs exactly that.
        # Without it the gap between a killed attempt and its restart is unknown, every
        # baseline loss is unattributable, and a real measurement is reported as an
        # apparatus failure. Found by running a consumer-kill run: 76 lost side effects,
        # the exact committed-but-not-applied window, with no gap recorded to explain
        # them.
        if progress is not None:
            progress(last_position.offset)
        buffered = []
        buffered_saga = None
        buffered_index = None

    try:
        consumer.subscribe([configuration.topics.input])
        reached_end = False
        idle_since: float | None = None
        while not reached_end:
            batch = consumer.consume(
                num_messages=settings.consumer_max_batch_records,
                timeout=settings.consume_batch_wait_ms / 1000.0,
            )
            if not batch:
                # No progress on this call. That is ordinary while the broker is down
                # during an injected outage, or while a restarted consumer waits out
                # the dead member's session, so it is only a failure once the phase
                # has made no progress for the whole stall budget.
                #
                # One rule, one branch, no configuration anywhere in it. The stopping
                # rule is shared code in both configurations, which INV-P3 requires of
                # the run path and which the allow-list gate cannot see because it
                # compares settings rather than control flow.
                idle_since = time.monotonic() if idle_since is None else idle_since
                idle_for = time.monotonic() - idle_since
                if idle_for > settings.consume_stall_budget_ms / 1000.0:
                    raise ApparatusFailure(
                        f"the input topic made no progress for {idle_for:.0f}s after "
                        f"{sagas_done} of {expected_sagas} sagas; the run reports no result"
                    )
                continue
            idle_since = None
            largest_batch = max(largest_batch, len(batch))

            for message in batch:
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        reached_end = True
                        break
                    raise ApparatusFailure(f"consuming the input topic failed: {error}")

                if resumed_at is None:
                    # Where Kafka's own mechanism put us. ADR-0003 section 7 forbids
                    # calling seek, so this is an observation of where the committed
                    # offset left the group, not a decision the harness made.
                    resumed_at = int(message.offset())

                payload: dict[str, Any] = json.loads(bytes(message.value()).decode("utf-8"))
                saga_id = str(payload["saga_id"])
                if buffered and saga_id != buffered_saga:
                    write_group_with_recovery()
                    sagas_done += 1

                buffered.append((str(payload["idempotency_key"]), bytes(message.value())))
                buffered_saga = saga_id
                buffered_index = int(payload["saga_index"])
                last_position = TopicPartition(
                    message.topic(), message.partition(), message.offset() + 1
                )

        if buffered:
            # Written, never raised. PB-T2 raised here on the grounds that a stream
            # ending mid-saga is malformed, which is true of a stream read from its
            # beginning and false of one resumed from a committed offset that landed
            # mid-saga. Raising would turn the baseline's own commit placement into an
            # apparatus failure and manufacture loss out of a real measurement.
            if len(buffered) < settings.steps_per_saga:
                partial_groups += 1
            write_group_with_recovery()
            sagas_done += 1

        # PB-T2 asserted sagas_done == expected_sagas here. That assertion could not
        # survive a restart under EITHER configuration, because a resumed phase
        # processes only the remainder, so it would have apparatus-failed every
        # process-phase kill run and gutted C1's coverage rather than only C2's. It is
        # not simply deleted: it was protecting against an apparatus bug that silently
        # dropped sagas, and removing the guard without replacing it would leave a
        # harness defect free to surface as loss. The replacement is the
        # attributability invariant in core/state.py, which is config-neutral and
        # strictly stronger: every lost side effect must fall inside a recorded offset
        # gap that a specific restart produced.
        #
        # The count is retained for the control run, where the whole stream is read
        # from offset 0 and nothing may be missing. That branches on the run's identity
        # in the frozen schedule, never on the configuration under test.
        if is_control and sagas_done != expected_sagas:
            raise ApparatusFailure(
                f"the control run processed {sagas_done} sagas where the schedule says "
                f"{expected_sagas}; nothing was killed, so the run is incomplete for an "
                f"apparatus reason and reports no result"
            )
    finally:
        consumer.close()

    # PB-T2 deleted the budget here, because nothing consumed it: no fault was
    # injected, so no recovery could happen and an empty history was the only possible
    # outcome. It is consumed now, by the recovery loop above, and deleting it would
    # unbind the name for that closure.
    return {
        "sink_a": per_sink[sink_a],
        "sink_b": per_sink[sink_b],
        "sagas_processed": sagas_done,
        "partial_groups": partial_groups,
        "largest_batch": largest_batch,
        "permanently_failed_keys": sorted(permanently_failed),
        "resumed_at_offset": -1 if resumed_at is None else resumed_at,
        "last_applied_offset": -1 if last_applied is None else last_applied,
    }


# --------------------------------------------------------------------------
# Phase 3: verify
# --------------------------------------------------------------------------


def verify(
    configuration: RunConfiguration, topic: str, steps_per_saga: int
) -> list[SideEffectRecord]:
    """Read one sink topic back and rebuild its ledger.

    Always read_committed, for both configurations. read_committed filters only
    aborted transactional messages, so it returns the baseline's
    non-transactional writes in full, and this stays one path with one set of
    settings for both.
    """
    values = read_to_end(dict(configuration.verifier), topic)
    return [observed_record(json.loads(value.decode("utf-8")), steps_per_saga) for value in values]


# --------------------------------------------------------------------------
# The whole run
# --------------------------------------------------------------------------


def reported_loss_count(sinks: Sequence[SinkOutcome]) -> int:
    """How many records the diffs say were lost, counted per sink.

    One of two deliberately separate routes to the same fact. This one counts records
    without looking at their contents; ``lost_keys_of`` builds a set of identities
    across sinks. A single bug is unlikely to move both the same way, which is what
    makes comparing them worth the two lines.
    """
    return sum(len(sink.diff.lost) for sink in sinks)


def lost_keys_of(sinks: Sequence[SinkOutcome]) -> list[str]:
    """The distinct idempotency keys the diffs report as lost.

    Named rather than inlined so the attributability control has something to compare
    against, and so the two routes are visibly different code rather than the same
    comprehension written twice.
    """
    return sorted({record.idempotency_key for sink in sinks for record in sink.diff.lost})


def assert_losses_are_attributable(
    configuration: RunConfiguration,
    sinks: Sequence[SinkOutcome],
    gaps: list[tuple[int, int]],
    steps_per_saga: int,
    permanently_failed_keys: Sequence[str] = (),
) -> None:
    """Refuse to report a loss that no recorded offset gap explains.

    The replacement for PB-T2's ``sagas_done == expected_sagas`` assertion, and
    strictly stronger than it. The old check asked whether the right number of sagas
    went through, which a restarted phase can never satisfy. This asks the question
    the old one was really protecting: is every missing side effect accounted for by a
    specific range of input-topic offsets that a specific restart skipped?

    Under the good configuration the gap list is empty, because offsets travel inside
    the transaction and an aborted attempt commits neither the work nor the offsets.
    So under `good` any loss whatsoever is unattributable and the run ends as an
    apparatus failure rather than as a C1 failure. That is the direction that matters:
    a harness defect must not be allowed to ship as "exactly-once did not hold".

    Under the baseline the gaps are the committed-but-not-applied windows that
    commit-before-processing produced, so the loss C2 measures is explained by the
    mechanism CLAIMS.md names rather than merely observed.
    """
    # The positive control, and the reason it is here rather than in a test.
    #
    # This invariant is SUPPOSED to be vacuous when nothing was lost: no loss means
    # nothing to attribute, and under the good configuration that is the expected
    # shape of every run. The hazard is that a broken extraction produces exactly the
    # same emptiness. A wrong attribute, a diff shape that changed, a comprehension
    # that ranges over the wrong thing: each yields no keys, the early return fires,
    # and a run with real unattributable loss sails through reporting a claim result.
    #
    # So the extraction is cross-checked against a count computed a different way. The
    # diff already knows how many records it lost; if it says some and the extraction
    # says none, the extraction is broken rather than the run being clean. That is
    # what distinguishes "nothing was lost" from "the walk cannot see loss".
    reported = reported_loss_count(sinks)
    lost_keys = lost_keys_of(sinks)

    if reported and not lost_keys:
        raise ApparatusFailure(
            f"the sink diffs report {reported} lost record(s) but the attributability "
            f"check extracted no keys from them, so the check cannot see the loss it "
            f"exists to explain. That is a defect in the check rather than a result, "
            f"and the run reports no result rather than a clean one."
        )
    if not lost_keys:
        return

    _, key_offsets = durable_saga_indices(configuration, steps_per_saga)
    unexplained = unattributable_losses(lost_keys, key_offsets, gaps, permanently_failed_keys)
    if unexplained:
        raise ApparatusFailure(
            f"{len(unexplained)} lost side effect(s) are explained by no recorded offset "
            f"gap and by no recorded per-record delivery failure, starting with "
            f"{unexplained[0]!r}. Recorded gaps: {gaps or 'none'}. A "
            f"loss the harness cannot attribute to a specific skipped offset range is an "
            f"apparatus defect, not a measurement, so this run reports no result rather "
            f"than a claim outcome it cannot explain."
        )


def prepare_topics(configuration: RunConfiguration, settings: Settings) -> None:
    """Bring one execution's topics and consumer groups to a known-empty state.

    Separated from ``execute_run`` because it must happen **once per execution, in the
    parent**, and never in a restarted phase. A resumed phase that deleted and
    recreated the topics it was resuming into would destroy the durable state the
    ADR-0003 section 7 resume contract reads, and would report total loss for an
    apparatus reason. tests/unit/test_state.py pins that the phase worker cannot
    reach this function.

    The consumer groups go with the topics, and for the same reason: the group id is
    stable per run and configuration so a restarted phase resumes where the killed one
    stopped, which across two matrix executions means a recreated topic can be paired
    with a stale committed offset.
    """
    bootstrap = settings.broker_bootstrap_servers
    assert bootstrap, "build_configuration already refused a missing broker address"
    provision(bootstrap, configuration.topics.all_topics())
    delete_consumer_groups(
        bootstrap,
        [str(configuration.consumer["group.id"]), str(configuration.verifier["group.id"])],
    )


def execute_run(
    run_id: int,
    configuration_name: str,
    settings: Settings,
    provision_topics: bool = True,
) -> RunResult:
    """Run one schedule entry under one configuration, in one process.

    Injects no fault: this is the single-process path that ``scripts/run_one.py`` and
    the control-run integration test use. The matrix drives the phases individually
    through ``scripts/run_phase.py`` so they can be killed, and passes
    ``provision_topics=False`` because the parent has already done it.
    """
    entry = load_schedule_entry(run_id, settings)
    configuration = build_configuration(configuration_name, run_id, settings)
    bootstrap = settings.broker_bootstrap_servers
    assert bootstrap, "build_configuration already refused a missing broker address"

    trace = load_trace(repo_root() / settings.trace_path)
    sagas = expand_sagas(str(entry["seed"]), settings, trace)
    expected = expected_ledger(sagas)
    budget = RecoveryBudget()
    transactions = TransactionLedger()

    if provision_topics:
        prepare_topics(configuration, settings)
    ingest(configuration, sagas, transactions)
    sent = process(
        configuration,
        settings,
        len(sagas),
        budget,
        transactions,
        is_control=bool(entry["control"]),
    )

    differ = KeyedLedgerDiffer()
    sinks: list[SinkOutcome] = []
    for name, topic in (
        ("sink_a", configuration.topics.sink_a),
        ("sink_b", configuration.topics.sink_b),
    ):
        observed = verify(configuration, topic, int(entry["steps_per_saga"]))
        sinks.append(
            SinkOutcome(
                name=name,
                topic=topic,
                records_sent=sent[name],
                records_visible=len(observed),
                diff=differ.diff(expected, observed),
                observed=tuple(observed),
            )
        )

    # The attributability invariant. In this single-process path no phase was
    # restarted, so there are no offset gaps and any loss at all is unattributable,
    # which is the correct and strictest reading: nothing was killed, so nothing may
    # be missing. The matrix path passes the gaps its restarts actually produced.
    assert_losses_are_attributable(
        configuration, sinks, gaps=[], steps_per_saga=len(sagas[0].steps)
    )

    status = STATUS_CLEAN if all(sink.is_clean for sink in sinks) else STATUS_NOT_CLEAN
    return RunResult(
        run_id=run_id,
        configuration=configuration,
        schedule_entry=entry,
        expected=expected,
        sinks=tuple(sinks),
        budget=budget,
        transactions=transactions,
        process_stats={key: value for key, value in sent.items() if not key.startswith("sink_")},
        status=status,
    )


# --------------------------------------------------------------------------
# Evidence
# --------------------------------------------------------------------------


def write_evidence(result: RunResult, settings: Settings) -> Path:
    """Write one run's evidence and return the directory it went to.

    Both configurations are written into resolved_config.json, not only the one
    that ran, so a reader can see the allow-listed difference without holding two
    files side by side. Every file carries the disclaimer, because a number in a
    file outlives the sentence that qualified it.

    Two changes at PB-T3, both forced by what this run has to survive and what it
    has to prove.

    **Every write is atomic.** ``Path.write_text`` truncates and then writes, and
    this evidence is now written by processes that get SIGKILLed, so a torn file is
    a real outcome rather than a theoretical one.

    **The ledgers are committed, gzipped, with computed digests.** PB-T2 committed
    digests alone and left the ledgers in a git-ignored directory, which was the
    right call for an apparatus check: the question there was whether the harness
    read zero. It is the wrong call for a published claim, where the question is
    whether the counts follow from the records, and CLAIMS.md's selling point is
    that a stranger can recompute them. JSON compresses roughly tenfold, so the
    whole 42-execution matrix is a couple of megabytes. The digests are retained
    and are now produced by the writer rather than typed by hand.
    """
    directory = (
        repo_root()
        / settings.run_output_dir
        / f"run_{result.run_id:02d}"
        / result.configuration.name
    )
    directory.mkdir(parents=True, exist_ok=True)

    both = build_both(result.run_id, settings)
    write_json(
        directory / "resolved_config.json",
        {
            "artifact": EVIDENCE_DISCLAIMER,
            "executed": result.configuration.name,
            "client": _client_versions(),
            "configurations": {name: conf.to_jsonable() for name, conf in both.items()},
        },
    )
    write_json(
        directory / "schedule_entry.json",
        {"artifact": EVIDENCE_DISCLAIMER, **result.schedule_entry},
    )

    # The digests are collected as the ledgers are written, so ledger_checksums.json
    # describes the bytes that went to disk rather than a second serialization of
    # the same records that happened to agree with them.
    digests: dict[str, str] = {}
    digests["expected_ledger.json"] = write_json_gz(
        directory / "expected_ledger.json.gz",
        {"artifact": EVIDENCE_DISCLAIMER, "records": ledger_jsonable(result.expected)},
    )
    for sink in result.sinks:
        digests[f"observed_{sink.name}.json"] = write_json_gz(
            directory / f"observed_{sink.name}.json.gz",
            {
                "artifact": EVIDENCE_DISCLAIMER,
                "topic": sink.topic,
                "records": ledger_jsonable(sink.observed),
            },
        )
        # The diffs stay uncompressed. They are the part a reader opens first, they
        # are small on a clean run, and on a fault run their size is itself the
        # headline. A gzipped headline is a headline nobody reads.
        write_json(
            directory / f"diff_{sink.name}.json",
            {
                "artifact": EVIDENCE_DISCLAIMER,
                **sink.to_jsonable(),
                "duplicated_records": ledger_jsonable(sink.diff.duplicated),
                "lost_records": ledger_jsonable(sink.diff.lost),
            },
        )

    write_json(
        directory / "ledger_checksums.json",
        {
            "artifact": EVIDENCE_DISCLAIMER,
            "note": (
                "SHA-256 of each ledger's uncompressed JSON, which is the document "
                "inside the .json.gz beside it. The digest covers the plain bytes "
                "rather than the compressed ones, so any gzip tool reproduces it. "
                "The expected ledger is also rebuildable from the run seed and the "
                "committed trace, so a reader who trusts neither the author nor this "
                "file can regenerate it and check."
            ),
            "records_per_ledger": len(result.expected),
            "sha256": digests,
        },
    )
    write_json(directory / "run_summary.json", result.summary())
    return directory


def _client_versions() -> dict[str, str]:
    """The client and librdkafka versions this run actually used.

    transaction.timeout.ms and every other librdkafka default the harness does
    not restate are owned by the pin, so the pin has to appear in the evidence
    for those inputs to be declared (ADR-0003).
    """
    import confluent_kafka

    return {
        "confluent_kafka": str(confluent_kafka.version()[0]),
        "librdkafka": str(confluent_kafka.libversion()[0]),
    }
