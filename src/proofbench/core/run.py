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
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from proofbench.config import Settings, repo_root
from proofbench.core.configs import RunConfiguration, build_both, build_configuration
from proofbench.core.evidence import ledger_jsonable, write_json, write_json_gz
from proofbench.core.ledger_diff import KeyedLedgerDiffer
from proofbench.core.recovery import (
    ApparatusFailure,
    RecoveryBudget,
    TransactionOutcome,
    classify,
)
from proofbench.core.saga import Saga, expand_sagas, expected_ledger, observed_record
from proofbench.core.topics import provision, read_to_end
from proofbench.core.trace import load_trace
from proofbench.core.txn import (
    PHASE_INGEST,
    PHASE_PROCESS,
    ROLE_INGEST,
    ROLE_SINK,
    AccountedProducer,
    TransactionLedger,
)
from proofbench.core.window import WindowState, is_within_fault_window, why_apparatus_failure
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

STATUS_CLEAN = "clean"
STATUS_NOT_CLEAN = "not_clean"
STATUS_APPARATUS_FAILURE = "apparatus_failure"

EVIDENCE_DISCLAIMER = "apparatus check, not a claim result"


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

    def __init__(self, message: str, errors: tuple[Any, ...], still_queued: int) -> None:
        super().__init__(message)
        self.errors = errors
        self.still_queued = still_queued


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
        self.errors.clear()
        self.error_topics.clear()

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


def ingest(
    configuration: RunConfiguration,
    sagas: tuple[Saga, ...],
    ledger: TransactionLedger,
) -> int:
    """Produce the run's side effects to the input topic. Returns records sent.

    Transactional under the good configuration, one transaction per saga, because
    CLAIMS.md names an idempotent transactional producer and the fault menu
    includes producer_sigkill_mid_send, which kills exactly this producer. A
    half-sent saga has to abort rather than half-land.

    Every transactional call goes through ``sender.txn``, which counts it. There is
    no path here that brackets a transaction without recording that it did.
    """
    sender = _Sender(dict(configuration.ingest_producer), ledger, PHASE_INGEST, ROLE_INGEST)
    transactional = configuration.transactional
    if transactional:
        sender.txn.init()

    for saga in sagas:
        if transactional:
            sender.txn.begin()
        for step in saga.steps:
            sender.produce(
                configuration.topics.input, step.record.idempotency_key, step.payload_bytes()
            )
        sender.flush()
        if transactional:
            sender.txn.commit()

    return sender.sent


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
) -> dict[str, int]:
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
    """
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    consumer = Consumer(dict(configuration.consumer))
    sender = _Sender(dict(configuration.sink_producer), ledger, PHASE_PROCESS, ROLE_SINK)
    transactional = configuration.transactional
    if transactional:
        sender.txn.init()

    sink_a, sink_b = configuration.topics.sinks
    per_sink: dict[str, int] = {sink_a: 0, sink_b: 0}
    buffered: list[tuple[str, bytes]] = []
    last_position: TopicPartition | None = None
    sagas_done = 0

    try:
        consumer.subscribe([configuration.topics.input])
        while True:
            message = consumer.poll(_TXN_TIMEOUT_S)
            if message is None:
                raise ApparatusFailure(
                    f"the input topic stalled for {_TXN_TIMEOUT_S:.0f}s after "
                    f"{sagas_done} of {expected_sagas} sagas; the run reports no result"
                )
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    break
                raise ApparatusFailure(f"consuming the input topic failed: {error}")

            payload: dict[str, Any] = json.loads(bytes(message.value()).decode("utf-8"))
            buffered.append((str(payload["idempotency_key"]), bytes(message.value())))
            last_position = TopicPartition(
                message.topic(), message.partition(), message.offset() + 1
            )

            if len(buffered) < settings.steps_per_saga:
                continue

            if transactional:
                sender.txn.begin()
            # Sink A is durable before sink B is attempted. One path, no branch.
            write_saga_to_sinks(sender, (sink_a, sink_b), buffered)
            per_sink[sink_a] += len(buffered)
            per_sink[sink_b] += len(buffered)
            if transactional:
                sender.producer.send_offsets_to_transaction(
                    [last_position], consumer.consumer_group_metadata(), _TXN_TIMEOUT_S
                )
                sender.txn.commit()

            buffered = []
            sagas_done += 1

        if buffered:
            raise ApparatusFailure(
                f"the input topic ended mid-saga with {len(buffered)} of "
                f"{settings.steps_per_saga} steps buffered; the stream is malformed"
            )
        if sagas_done != expected_sagas:
            raise ApparatusFailure(
                f"processed {sagas_done} sagas where the schedule says {expected_sagas}; "
                f"the run is incomplete and reports no result"
            )
    finally:
        consumer.close()

    # The budget is threaded through so a fault run can record what it had to do
    # to get here. PB-T2 injects no fault, so it stays empty, and an empty
    # recovery history in the control evidence is itself worth recording.
    del budget
    return {"sink_a": per_sink[sink_a], "sink_b": per_sink[sink_b]}


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


def execute_run(run_id: int, configuration_name: str, settings: Settings) -> RunResult:
    """Run one schedule entry under one configuration. Injects no fault."""
    entry = load_schedule_entry(run_id, settings)
    configuration = build_configuration(configuration_name, run_id, settings)
    bootstrap = settings.broker_bootstrap_servers
    assert bootstrap, "build_configuration already refused a missing broker address"

    trace = load_trace(repo_root() / settings.trace_path)
    sagas = expand_sagas(str(entry["seed"]), settings, trace)
    expected = expected_ledger(sagas)
    budget = RecoveryBudget()
    transactions = TransactionLedger()

    provision(bootstrap, configuration.topics.all_topics())
    ingest(configuration, sagas, transactions)
    sent = process(configuration, settings, len(sagas), budget, transactions)

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

    status = STATUS_CLEAN if all(sink.is_clean for sink in sinks) else STATUS_NOT_CLEAN
    return RunResult(
        run_id=run_id,
        configuration=configuration,
        schedule_entry=entry,
        expected=expected,
        sinks=tuple(sinks),
        budget=budget,
        transactions=transactions,
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
