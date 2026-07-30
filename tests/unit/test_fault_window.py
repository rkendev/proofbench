"""The fault-window boundary, from both sides.

PB-T2 escalated every delivery failure to ApparatusFailure, and an apparatus
failure is never scored. Under broker_stop_start a produce during the outage
genuinely fails, so all twelve broker executions would have ended unscored, taking
the most interesting rows of the matrix and quite possibly C1 and C2 with them.

Both directions are tested here because both are fatal, and they fail differently:

- Too strict loses the broker runs. Visible as a hole in the matrix.
- Too loose scores a genuine apparatus break as a finding. A client-side stall
  becomes "lost side effects" and C2 inflates for free. Worse, because the number
  looks like evidence.

Every test uses a stub state and a fake error. The boundary imports no client and
touches no marker file, which is what makes both branches reachable offline rather
than only during the matrix they exist to make trustworthy.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from proofbench.core.recovery import (
    MAX_PRODUCER_REINITS,
    ApparatusFailure,
    RecoveryBudget,
    TransactionOutcome,
)
from proofbench.core.run import DeliveryError, DeliveryFailure, resolve_delivery_error
from proofbench.core.window import (
    DeliveryDisposition,
    classify_delivery_failure,
    facts_from,
    is_within_fault_window,
    why_apparatus_failure,
)


@dataclass(frozen=True)
class State:
    """The four facts the boundary needs, all defaulting to "inside the window".

    Defaulting to the permissive state on purpose: each test then names exactly the
    one fact it is flipping, so what the test is about is the deviation rather than
    the setup.
    """

    entry_names_a_fault: bool = True
    fault_has_fired: bool = True
    window_closed: bool = False
    budget_exhausted: bool = False


class FakeError:
    """Stands in for the client's KafkaError. Same shape recovery.py branches on."""

    def __init__(
        self, *, retriable: bool = False, requires_abort: bool = False, fatal: bool = False
    ) -> None:
        self._retriable = retriable
        self._requires_abort = requires_abort
        self._fatal = fatal

    def retriable(self) -> bool:
        return self._retriable

    def txn_requires_abort(self) -> bool:
        return self._requires_abort

    def fatal(self) -> bool:
        return self._fatal

    def __str__(self) -> str:
        return "Broker: Local: Broker transport failure"


def _delivery_error(error: FakeError | None = None) -> DeliveryError:
    err = error if error is not None else FakeError(retriable=True)
    return DeliveryError("1 record(s) failed delivery", errors=(err,), still_queued=0)


# --------------------------------------------------------------------------
# Branch one: inside the window, it is part of the fault
# --------------------------------------------------------------------------


def test_a_delivery_failure_inside_the_window_is_part_of_the_fault() -> None:
    """The branch whose absence would have voided every broker-fault run."""
    assert is_within_fault_window(State())
    assert classify_delivery_failure(State()) is DeliveryDisposition.PART_OF_THE_FAULT


def test_an_in_window_failure_reaches_the_recovery_contract() -> None:
    """It does not merely survive: it is handed to ADR-0003 section 6 and recorded.

    A boundary that swallowed the failure silently would keep the run alive and
    leave the evidence unable to say what the apparatus had to do to get there,
    which ADR-0003 section 3 exists to prevent.
    """
    budget = RecoveryBudget()
    outcome = resolve_delivery_error(_delivery_error(), State(), budget)

    assert outcome is TransactionOutcome.RETRY
    assert budget.retries == 1
    assert budget.reasons and "inside the fault window" in budget.reasons[0]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (FakeError(retriable=True), TransactionOutcome.RETRY),
        (FakeError(requires_abort=True), TransactionOutcome.ABORT_AND_REPLAY),
        (FakeError(fatal=True), TransactionOutcome.REINIT_PRODUCER),
        (FakeError(), TransactionOutcome.REINIT_PRODUCER),
    ],
)
def test_the_contract_decides_which_recovery_an_in_window_failure_gets(
    error: FakeError, expected: TransactionOutcome
) -> None:
    """The boundary decides whether, the contract decides which. Two questions.

    A broker restart makes a commit retriable while the coordinator re-elects; an
    abortable transaction is replayed with unchanged idempotency keys; a fenced
    producer is discarded and rebuilt with the same transactional id.
    """
    assert resolve_delivery_error(_delivery_error(error), State(), RecoveryBudget()) is expected


def test_an_in_window_abort_is_counted_where_the_evidence_can_see_it() -> None:
    """ADR-0003 section 3: aborted counts are load-bearing evidence."""
    budget = RecoveryBudget()
    resolve_delivery_error(_delivery_error(FakeError(requires_abort=True)), State(), budget)
    assert budget.to_jsonable()["aborts"] == 1


# --------------------------------------------------------------------------
# Branch two: outside the window, it is an apparatus failure
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("state", "because"),
    [
        (State(entry_names_a_fault=False), "names no fault"),
        (State(fault_has_fired=False), "has not fired yet"),
        (State(window_closed=True), "already closed"),
        (State(budget_exhausted=True), "budget is exhausted"),
    ],
    ids=["no_fault_in_entry", "fault_not_yet_fired", "window_already_closed", "budget_exhausted"],
)
def test_each_condition_alone_makes_it_an_apparatus_failure(state: State, because: str) -> None:
    """The boundary is a conjunction: any one condition failing is enough.

    Written as four separate cases rather than one, because a boundary that only
    checked three of them would pass a single combined test while leaving one route
    open, and it is the fourth condition that stops an exhausted budget absorbing
    an unbounded number of real failures.
    """
    assert not is_within_fault_window(state)
    assert classify_delivery_failure(state) is DeliveryDisposition.APPARATUS_FAILURE
    assert because in why_apparatus_failure(state)


def test_an_out_of_window_failure_raises_and_is_never_scored() -> None:
    """DeliveryFailure is an ApparatusFailure, which the matrix never scores."""
    budget = RecoveryBudget()
    with pytest.raises(DeliveryFailure) as caught:
        resolve_delivery_error(_delivery_error(), State(fault_has_fired=False), budget)

    assert isinstance(caught.value, ApparatusFailure)
    assert "reports no result" in str(caught.value)
    # And nothing was recorded as recovery, because no recovery happened.
    assert budget.retries == 0
    assert budget.aborts == 0
    assert budget.reasons == []


def test_naming_a_fault_in_the_entry_is_not_sufficient_on_its_own() -> None:
    """The condition that does the most work, stated as its own rule.

    A delivery failure on a broker-fault run that happens before the injector has
    fired is not part of the fault: nothing has been injected yet. Treating "the
    entry names a fault" as sufficient would make every failure anywhere in a
    kill run recoverable, which is the too-loose direction.
    """
    state = State(entry_names_a_fault=True, fault_has_fired=False)
    assert not is_within_fault_window(state)


def test_the_exhausted_budget_condition_is_what_bounds_absorption() -> None:
    """Past the budget, a failure cannot be absorbed as recovery any more.

    Without this condition a run could recover indefinitely and look exactly like a
    run that never failed, which is the reason RecoveryBudget counts rather than
    trusts in the first place.
    """
    budget = RecoveryBudget()
    for _ in range(MAX_PRODUCER_REINITS):
        budget.record(TransactionOutcome.REINIT_PRODUCER, "producer fenced")

    exhausted = State(budget_exhausted=budget.reinits >= budget.max_reinits)
    assert not is_within_fault_window(exhausted)
    with pytest.raises(DeliveryFailure, match="budget is exhausted"):
        resolve_delivery_error(_delivery_error(), exhausted, budget)


# --------------------------------------------------------------------------
# A queue that never drained is apparatus in both directions
# --------------------------------------------------------------------------


def test_a_stuck_queue_never_reaches_the_boundary_at_all() -> None:
    """Deliberately not routed through the window, in either direction.

    message.timeout.ms is set below the flush timeout and a gate holds it there, so
    librdkafka must resolve every queued record, by delivery or by a reported error,
    well inside the flush window. Records still queued afterwards mean the client is
    stuck rather than the broker being absent, and that is an apparatus break whether
    or not a fault window happens to be open. It is raised by the sender as a
    DeliveryFailure directly, so this asserts the type relationship the sender relies
    on rather than a dispatch that does not happen.
    """
    assert issubclass(DeliveryFailure, ApparatusFailure)
    assert not issubclass(DeliveryError, ApparatusFailure), (
        "DeliveryError must stay neutral: making it an ApparatusFailure would make "
        "every delivery failure unscoreable again, which is the PB-T2 defect"
    )


# --------------------------------------------------------------------------
# The facts come from the frozen schedule, not from a hand-set flag
# --------------------------------------------------------------------------


def test_the_control_run_has_no_fault_window_at_all() -> None:
    """Read from the entry's fault type, so a control run cannot acquire a window."""
    control = {"fault_type": "none", "fault_point": None, "control": True}
    facts = facts_from(control, fault_has_fired=True, window_closed=False, budget_exhausted=False)
    assert not facts.entry_names_a_fault
    assert not is_within_fault_window(facts)


def test_a_kill_run_entry_names_a_fault() -> None:
    kill = {"fault_type": "broker_stop_start", "fault_point": {"saga_index": 149, "step_index": 2}}
    facts = facts_from(kill, fault_has_fired=True, window_closed=False, budget_exhausted=False)
    assert facts.entry_names_a_fault
    assert is_within_fault_window(facts)


def test_fault_type_and_fault_point_agree_across_the_frozen_schedule() -> None:
    """Nothing depends on which of the two the boundary reads, and this pins that.

    ``facts_from`` reads ``fault_type`` because that is what the fault menu
    enumerates and what the matrix groups by. If an entry ever carried a fault type
    without a fault point, or the reverse, the choice of field would silently change
    the boundary's answer.
    """
    import json

    from proofbench.config import Settings, repo_root

    settings = Settings(_env_file=None)
    payload = json.loads((repo_root() / settings.schedule_path).read_text(encoding="utf-8"))
    for entry in payload["runs"]:
        names_a_fault = entry["fault_type"] != settings.no_fault_label
        has_a_point = entry["fault_point"] is not None
        assert names_a_fault == has_a_point, (
            f"run {entry['run_id']} carries fault_type {entry['fault_type']!r} and "
            f"fault_point {entry['fault_point']!r}, which disagree"
        )


# --------------------------------------------------------------------------
# The sender's error list is cleared, which PB-T2 never did
# --------------------------------------------------------------------------


def test_a_reported_delivery_error_does_not_poison_every_later_flush() -> None:
    """The defect that only bites once a run is expected to survive a failure.

    PB-T2 never cleared ``_Sender.errors``, so the first delivery error made every
    later ``flush`` on the same sender raise for the rest of the run. Harmless while
    no run was expected to survive one, and fatal now: the broker-outage runs are
    expected to take a delivery error, recover, and carry on, and an uncleared list
    would turn one transient error into total loss for every saga after it.

    Offline: the placeholder bootstrap is never connected to, and ``flush`` returns
    immediately because nothing was produced. What is exercised is the bookkeeping.
    """
    import logging

    from proofbench.core.run import _Sender
    from proofbench.core.txn import PHASE_PROCESS, ROLE_SINK, TransactionLedger

    silent = logging.getLogger("proofbench.tests.silent_sender")
    silent.addHandler(logging.NullHandler())
    silent.propagate = False

    sender = _Sender(
        {"bootstrap.servers": "broker-placeholder:1", "logger": silent},
        TransactionLedger(),
        PHASE_PROCESS,
        ROLE_SINK,
    )
    sender.errors.append(FakeError(retriable=True))
    sender.error_topics.append("proofbench.r03.good.sink_a")

    with pytest.raises(DeliveryError):
        sender.flush()

    # The saga was recovered and the run continues. The next flush must be clean.
    sender.flush()
    assert sender.errors == []
    assert sender.error_topics == []


def test_asking_why_about_an_in_window_state_is_a_bug_not_a_description() -> None:
    """The diagnosis writer and the boundary must not be able to disagree.

    Matrix-validity rule 2 requires every apparatus failure to be diagnosed in
    writing, and the diagnosis is generated by the same code that made the decision
    so the two cannot drift. Asking for a reason when there is none means the caller
    and the boundary disagree, which is worth raising over.
    """
    with pytest.raises(ValueError, match="inside the fault window"):
        why_apparatus_failure(State())
