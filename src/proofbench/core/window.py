"""The fault window: where a delivery failure is part of the fault, and where it is not.

PB-T2 escalated every delivery failure to ``ApparatusFailure``, and an apparatus
failure is never scored. Under ``broker_stop_start`` a produce during the outage
genuinely fails to reach the broker, so as it stood all twelve broker executions
would have ended unscored, gutting the matrix and quite possibly C1 and C2 with it.

Getting the boundary wrong in either direction is fatal to the result, and the two
directions fail differently:

- **Too strict**, which is PB-T2's behaviour: the broker runs vanish. A fault the
  contract asked for produces no measurement, and the matrix has a hole where its
  most interesting rows should be.
- **Too loose**: a genuine apparatus break gets scored as a finding. A client-side
  stall, a queue that never drained, a topic that was never provisioned, all become
  "lost side effects" and C2 inflates for free. That is worse, because it is
  invisible: the number looks like evidence.

So the boundary is explicit, narrow, and stated as a conjunction. **All four
conditions must hold** for a delivery failure to be treated as part of the fault:

1. the run's schedule entry names a fault (a control run has no window, ever)
2. the durable marker records that the fault actually fired, not merely that the
   entry asked for one
3. the marker has not recorded the window closed
4. the recovery budget is not exhausted

Any one of them failing makes it an apparatus failure. Condition 2 is the one that
does the most work: it means "the entry names a fault" is never sufficient on its
own, so a delivery failure that happens before the injector fires, or on a run
whose injector never fired at all, is still an apparatus failure. Condition 4 is
what stops an unbounded recovery loop from absorbing an unbounded number of real
failures and calling the result a measurement.

The window opens when the injector fires and closes when the phase hosting the
fault has resolved the affected saga or terminated. Both are recorded in the
durable marker rather than held in memory, because the process that opened the
window is, for two of the three fault types, the process that gets SIGKILLed.

Deliberately not in this module: any decision about *what* to do with an in-window
failure. That is the ADR-0003 section 6 recovery contract in ``recovery.py``, which
already exists and is already tested. This module answers one question, returns one
enum, and imports no client.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol


class DeliveryDisposition(Enum):
    """What a delivery failure is, before anything is done about it."""

    # Inside an expected fault window on a run that named a fault. Hand it to the
    # ADR-0003 section 6 contract: classify, then retry, abort and replay, or
    # discard and re-init with the same transactional id.
    PART_OF_THE_FAULT = "part_of_the_fault"

    # Outside any fault window, or after the recovery budget is exhausted. The run
    # records apparatus_failure, writes whatever evidence it holds, and is never
    # scored as a claim result.
    APPARATUS_FAILURE = "apparatus_failure"


class WindowState(Protocol):
    """What the boundary needs to know, which is exactly four facts.

    Structural so the boundary is testable without a marker file, a broker, or a
    subprocess. The real implementation is the durable fault marker in
    ``faults.py``; ``tests/unit/test_fault_window.py`` uses a plain stub.
    """

    @property
    def entry_names_a_fault(self) -> bool: ...

    @property
    def fault_has_fired(self) -> bool: ...

    @property
    def window_closed(self) -> bool: ...

    @property
    def budget_exhausted(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class WindowFacts:
    """A plain carrier for the four facts, for callers that hold them separately."""

    entry_names_a_fault: bool
    fault_has_fired: bool
    window_closed: bool
    budget_exhausted: bool


# Each condition paired with the reason its failure means apparatus rather than
# fault. Kept as data rather than as a chain of ifs so that the explanation written
# into the evidence is the same one the branch used, and so a reader of a
# apparatus_failure diagnosis learns which condition fired.
_CONDITIONS: tuple[tuple[str, str], ...] = (
    (
        "entry_names_a_fault",
        "the schedule entry names no fault, so this run has no fault window at all "
        "and nothing here can be part of one",
    ),
    (
        "fault_has_fired",
        "the injector has not fired yet, so the window is not open; a delivery "
        "failure before the fault is a delivery failure, not the fault",
    ),
    (
        "window_closed_is_false",
        "the fault window has already closed, so the phase had resolved the affected "
        "saga and this failure is a second, unexplained one",
    ),
    (
        "budget_not_exhausted",
        "the recovery budget is exhausted, and past that point a failure cannot be "
        "absorbed as recovery without absorbing an unbounded number of real ones",
    ),
)


def _unmet(state: WindowState) -> str | None:
    """The name of the first unmet condition, or None when all four hold."""
    if not state.entry_names_a_fault:
        return _CONDITIONS[0][0]
    if not state.fault_has_fired:
        return _CONDITIONS[1][0]
    if state.window_closed:
        return _CONDITIONS[2][0]
    if state.budget_exhausted:
        return _CONDITIONS[3][0]
    return None


def is_within_fault_window(state: WindowState) -> bool:
    """True when all four conditions hold. The conjunction, and nothing else."""
    return _unmet(state) is None


def classify_delivery_failure(state: WindowState) -> DeliveryDisposition:
    """Return whether a delivery failure is part of the fault or an apparatus break."""
    if is_within_fault_window(state):
        return DeliveryDisposition.PART_OF_THE_FAULT
    return DeliveryDisposition.APPARATUS_FAILURE


def why_apparatus_failure(state: WindowState) -> str:
    """The reason a delivery failure was not treated as part of the fault.

    Written into the evidence beside ``run_status: apparatus_failure``. Rule 2 of
    the matrix-validity rule requires every apparatus failure to be diagnosed in
    writing, and a diagnosis generated by the same code that made the decision
    cannot disagree with it.
    """
    unmet = _unmet(state)
    if unmet is None:
        raise ValueError(
            "why_apparatus_failure was asked about a state that is inside the fault "
            "window; the caller and the boundary disagree, which is a bug rather "
            "than a condition to describe"
        )
    for name, reason in _CONDITIONS:
        if name == unmet:
            return reason
    raise AssertionError(f"unmet condition {unmet!r} has no recorded reason")


def facts_from(
    entry: dict[str, Any],
    fault_has_fired: bool,
    window_closed: bool,
    budget_exhausted: bool,
) -> WindowFacts:
    """Assemble the four facts, taking the first from the frozen schedule entry.

    ``fault_type`` rather than ``fault_point`` is what decides whether an entry
    names a fault, because ``fault_type`` is what the fault menu enumerates and
    what the matrix groups by. The two agree in the committed schedule (the control
    is the only entry with a null fault point and it is also the only one whose
    fault type is the no-fault label), and a test pins that they agree, so nothing
    depends on which is read.
    """
    from proofbench.config import Settings

    no_fault_label = Settings.model_fields["no_fault_label"].default
    return WindowFacts(
        entry_names_a_fault=str(entry["fault_type"]) != no_fault_label,
        fault_has_fired=fault_has_fired,
        window_closed=window_closed,
        budget_exhausted=budget_exhausted,
    )
