"""The two contracts ADR-0003 freezes, exercised before PB-T3 depends on them.

PB-T2 injects no fault, so none of this runs during the control run. It is tested
now, against a fake error object rather than a broker, so the branches are not
first executed in the middle of the matrix they exist to make trustworthy.

The fake is the point. The contract branches on three predicates, and a test that
needed a real fenced producer to reach the third branch would never be run, which
is how a recovery path rots.
"""

from __future__ import annotations

import pytest

from proofbench.core.recovery import (
    MAX_PRODUCER_REINITS,
    ApparatusFailure,
    RecoveryBudget,
    TransactionOutcome,
    classify,
    resume_saga_index,
)


class FakeError:
    """Stands in for the client's KafkaError, which the contract never imports.

    Structural typing is what makes this possible, and it is deliberate: the
    recovery contract is policy, and policy that could only be tested with a
    broker attached would not be tested.
    """

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


# --------------------------------------------------------------------------
# The transactional recovery contract
# --------------------------------------------------------------------------


def test_a_retriable_error_is_retried() -> None:
    """A broker restart makes commit retriable while the coordinator re-elects."""
    assert classify(FakeError(retriable=True)) is TransactionOutcome.RETRY


def test_an_abortable_error_aborts_and_replays_the_saga() -> None:
    """The saga is the transaction boundary, so a replay is well defined.

    Its idempotency keys are unchanged, so a successful replay produces exactly
    the expected records rather than a second set of them.
    """
    assert classify(FakeError(requires_abort=True)) is TransactionOutcome.ABORT_AND_REPLAY


def test_a_fatal_error_re_initialises_the_producer() -> None:
    """Where fencing lands. The producer object is dead and cannot be reused."""
    assert classify(FakeError(fatal=True)) is TransactionOutcome.REINIT_PRODUCER


def test_an_unclassified_error_is_not_silently_retried() -> None:
    """The conservative branch, and the one worth stating.

    In a measurement harness an unknown condition must not be folded into a
    number. Retrying forever would stall the matrix; retrying a few times and
    carrying on would report a count that had absorbed something nobody
    classified. Re-initialising is bounded, and exhausting the bound ends the run
    honestly.
    """
    assert classify(FakeError()) is TransactionOutcome.REINIT_PRODUCER


def test_the_order_of_the_predicates_is_fixed() -> None:
    """Retriable wins over abortable, which wins over fatal.

    An error that reported more than one would otherwise be handled differently
    depending on the order the branches happened to be written in, and the
    cheapest correct response is the one to prefer.
    """
    assert classify(FakeError(retriable=True, requires_abort=True)) is TransactionOutcome.RETRY
    assert classify(FakeError(requires_abort=True, fatal=True)) is (
        TransactionOutcome.ABORT_AND_REPLAY
    )


# --------------------------------------------------------------------------
# The recovery budget
# --------------------------------------------------------------------------


def test_retries_and_aborts_are_counted_but_not_bounded() -> None:
    """They cost no producer, and the counts are evidence in their own right.

    ADR-0003 records aborted-transaction counts per run: under the good
    configuration an abort is the mechanism that keeps a partial write
    unobservable, so how often it fired is part of what a run shows.
    """
    budget = RecoveryBudget()
    for _ in range(50):
        budget.record(TransactionOutcome.RETRY, "coordinator re-electing")
        budget.record(TransactionOutcome.ABORT_AND_REPLAY, "transaction unusable")

    assert budget.retries == 50
    assert budget.aborts == 50
    assert budget.reinits == 0


def test_producer_re_initialisation_is_bounded_and_ends_the_run() -> None:
    """An unbounded recovery loop would look like a run that never failed."""
    budget = RecoveryBudget()
    for _ in range(MAX_PRODUCER_REINITS):
        budget.record(TransactionOutcome.REINIT_PRODUCER, "producer fenced")
    assert budget.reinits == MAX_PRODUCER_REINITS

    with pytest.raises(ApparatusFailure, match="the run is abandoned and reports no result"):
        budget.record(TransactionOutcome.REINIT_PRODUCER, "producer fenced again")


def test_the_failure_carries_the_recovery_history() -> None:
    """A run that was abandoned has to say what it was fighting.

    Otherwise apparatus_failure is indistinguishable from a crash, and the next
    person cannot tell whether the harness or the broker was at fault.
    """
    budget = RecoveryBudget(max_reinits=1)
    budget.record(TransactionOutcome.RETRY, "coordinator re-electing")
    budget.record(TransactionOutcome.REINIT_PRODUCER, "producer fenced")

    with pytest.raises(ApparatusFailure) as caught:
        budget.record(TransactionOutcome.REINIT_PRODUCER, "producer fenced again")
    assert "coordinator re-electing" in str(caught.value)
    assert "producer fenced" in str(caught.value)


def test_the_budget_serializes_into_evidence() -> None:
    budget = RecoveryBudget()
    budget.record(TransactionOutcome.ABORT_AND_REPLAY, "transaction unusable")
    payload = budget.to_jsonable()

    assert payload["aborts"] == 1
    assert payload["producer_reinits"] == 0
    assert payload["max_reinits"] == MAX_PRODUCER_REINITS
    assert payload["history"] == ["abort_and_replay: transaction unusable"]


# --------------------------------------------------------------------------
# The restart and resume contract
# --------------------------------------------------------------------------


def test_a_fresh_run_resumes_at_the_beginning() -> None:
    assert resume_saga_index([]) == 0


def test_a_restart_resumes_after_the_durable_prefix() -> None:
    """The ordinary case: sagas 0 to 4 are durable, so 5 is next."""
    assert resume_saga_index(range(5)) == 5


def test_a_gap_is_not_skipped_over() -> None:
    """The case the wording refinement exists for.

    Sagas 0, 1, and 3 durable with 2 missing resumes at 2, not at 4. Skipping to
    4 would step past unrecorded work and report a loss the configuration did not
    cause, which is exactly how a kill run's number stops being true.
    """
    assert resume_saga_index([0, 1, 3]) == 2
    assert resume_saga_index([0, 1, 3, 4, 5]) == 2


def test_it_never_replays_from_the_start_when_work_is_durable() -> None:
    """The other half of the bound.

    A whole-run replay would make the baseline duplicate hundreds of side effects
    and C2 would pass for a crude reason that has nothing to do with commit
    placement.
    """
    assert resume_saga_index(range(199)) == 199


def test_order_and_repetition_in_the_input_do_not_matter() -> None:
    """Callers learn completeness from a topic or an offset, not a sorted list."""
    assert resume_saga_index([3, 1, 0, 1, 3]) == 2
    assert resume_saga_index({4, 2, 0, 1, 3}) == 5


def test_the_resume_point_is_at_most_one_saga_behind_the_durable_frontier() -> None:
    """The stated bound, asserted rather than asserted about.

    Whatever the durable set, the work re-processed is the single saga at the
    resume index, so the duplication a kill can cause is bounded by one saga's
    worth of side effects per sink.
    """
    for durable in ([], [0], range(7), [0, 1, 2, 5, 6]):
        index = resume_saga_index(durable)
        assert index not in set(durable)
        assert all(earlier in set(durable) for earlier in range(index))
