"""The transactional recovery contract and the restart and resume contract.

Both are frozen at PB-T2, dated, before any kill result exists. That timing is the
point. Between them they decide how much the baseline duplicates after a kill,
which is the quantity C2's 80 percent floor is measured against. Left unstated
until PB-T3 they would be free knobs available after results were visible.

PB-T2 injects no fault, so nothing here fires during the control run. It is
written and tested now anyway, against a fake client, so that the code is not
first executed in the middle of the matrix it is supposed to make trustworthy.

**The restart and resume contract.** On restart, a phase resumes at the first saga
index not known to be durably complete, and re-processes from there. It never
replays from the start of the run, and it never skips forward past unrecorded
work. Identical code in both configurations, per INV-P3.

The bound that follows is what makes the contract worth freezing: at most one
saga's worth of side effects per sink per kill. A whole-run replay would make the
baseline duplicate hundreds of side effects and C2 would pass for a crude reason
that has nothing to do with commit placement.

Note on wording, recorded rather than applied silently. The owner's ruling said
"resumes from the last saga index it durably recorded, and re-processes that
saga". For the process phase the two formulations coincide, because the committed
consumer offset already points at the first unprocessed record. For the ingest
phase under the good configuration they diverge, and the literal version is
unsafe: if saga L committed and saga L+1 aborted, re-processing L re-sends a saga
that is already durable. Producer idempotence does not suppress that, because it
deduplicates retries within a producer epoch and a restart bumps the epoch. The
result would be a genuine duplicate under the good configuration, and C1 would
fail for an apparatus reason. Resuming at the first saga not known to be complete
preserves the intent, including the one-saga bound, without that hazard.

**The transactional recovery contract.** Three error classes, three different
responses, rather than a blanket retry. See ``classify``.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

# How many times a run may discard a dead producer and re-initialise it with the
# same transactional id before the run is abandoned. Apparatus tuning, not a claim
# constant: it does not enter docs/run_schedule.json and it cannot change a
# measured outcome, because exhausting it produces no measurement at all.
MAX_PRODUCER_REINITS = 3


class ApparatusFailure(Exception):
    """The run could not be completed, so it has no result to report.

    Deliberately not a measurement. A run that hit this writes whatever evidence
    it holds with run_status apparatus_failure and is never scored as a claim
    result. The alternative, scoring an incomplete run, would let an apparatus
    problem masquerade as loss and inflate C2.
    """


class TransactionOutcome(Enum):
    """What the harness does about one failed transactional call."""

    # The call may simply be tried again. A broker restart makes commit
    # retriable while the coordinator is re-electing.
    RETRY = "retry"

    # The transaction is unusable. Abort it, then replay the saga from its start.
    # The saga is the transaction boundary, so a replay is well defined and its
    # idempotency keys are unchanged, so a successful replay produces exactly the
    # expected records.
    ABORT_AND_REPLAY = "abort_and_replay"

    # The producer object is dead, which is where fencing lands. Discard it and
    # construct a new one with the SAME transactional id, then init_transactions,
    # which bumps the epoch and fences the dead one. Never mint a new id: that
    # would abandon the dead epoch's open transaction, and the Last Stable Offset
    # would sit behind it until transaction.timeout.ms expired, blocking every
    # read_committed consumer on the partition.
    REINIT_PRODUCER = "reinit_producer"


class TransactionError(Protocol):
    """The shape of the client error object the contract branches on.

    Structural rather than an import of the client's KafkaError, so this module
    stays free of the client and the contract is testable without one. The three
    predicates are verified to exist on the pinned client by
    tests/unit/test_client_contract.py.
    """

    def retriable(self) -> bool: ...

    def txn_requires_abort(self) -> bool: ...

    def fatal(self) -> bool: ...


def classify(error: TransactionError) -> TransactionOutcome:
    """Return what to do about one failed transactional call.

    Order matters and is fixed here. ``retriable`` is asked first because it is
    the cheapest correct response and the one a coordinator re-election wants.
    ``txn_requires_abort`` next, because an abortable transaction is recoverable
    within the same producer. ``fatal`` last, because it is the only one that
    costs a producer.

    An error matching none of the three is treated as fatal rather than retried.
    In a measurement harness an unclassified error must not be silently retried:
    retrying forever would stall the matrix, and retrying a few times and
    continuing would fold an unknown condition into a number. Re-initialising is
    bounded, and exhausting the bound ends the run honestly.
    """
    if error.retriable():
        return TransactionOutcome.RETRY
    if error.txn_requires_abort():
        return TransactionOutcome.ABORT_AND_REPLAY
    if error.fatal():
        return TransactionOutcome.REINIT_PRODUCER
    return TransactionOutcome.REINIT_PRODUCER


@dataclass(slots=True)
class RecoveryBudget:
    """Bounds how much recovery one run is allowed before it is abandoned.

    Counting rather than trusting: a run that recovered indefinitely would look
    like a run that never failed, and the evidence would say nothing about how
    hard the apparatus had to work to produce it. The counts are written into the
    run summary for exactly that reason.
    """

    max_reinits: int = MAX_PRODUCER_REINITS
    reinits: int = 0
    retries: int = 0
    aborts: int = 0
    reasons: list[str] = field(default_factory=list)

    def record(self, outcome: TransactionOutcome, reason: str) -> None:
        """Record one recovery action, or raise when the budget is exhausted."""
        self.reasons.append(f"{outcome.value}: {reason}")
        if outcome is TransactionOutcome.RETRY:
            self.retries += 1
            return
        if outcome is TransactionOutcome.ABORT_AND_REPLAY:
            self.aborts += 1
            return

        self.reinits += 1
        if self.reinits > self.max_reinits:
            raise ApparatusFailure(
                f"the producer was re-initialised {self.reinits} times, past the "
                f"limit of {self.max_reinits}; the run is abandoned and reports no "
                f"result. Recovery history: {self.reasons}"
            )

    def assert_consistent(self) -> None:
        """Refuse to report a budget whose counters and history disagree.

        The positive control for the bound, and it is needed because the bound fails
        **open**. ``record`` is the only thing that increments ``reinits``, and the
        bound only fires when ``reinits`` exceeds the limit. If some path ever
        recovered without going through ``record``, the counter would stay at zero,
        the bound would never fire, and a run that re-initialised its producer a
        hundred times would be indistinguishable from a run that never needed to.

        That is precisely the failure this class was written to prevent: its own
        docstring says a run that recovered indefinitely would look like a run that
        never failed. Zero reinits on a clean run and zero reinits because nothing is
        counting are the same evidence.

        The check is that every recorded action appended exactly one reason, so the
        history length and the counter total have to agree. A recovery that bypassed
        ``record`` moves neither, but a recovery that moved a counter without a reason,
        or a reason without a counter, is caught here. Combined with the run summary
        carrying both, a reader can see the two agree rather than trusting that they do.
        """
        counted = self.retries + self.aborts + self.reinits
        if len(self.reasons) != counted:
            raise ApparatusFailure(
                f"the recovery budget recorded {counted} action(s) across its counters "
                f"but holds {len(self.reasons)} reason(s) in its history. The two are "
                f"written by the same call, so they cannot disagree unless something "
                f"recovered without being counted, in which case the three-re-init "
                f"bound is not being enforced and this run reports no result."
            )
        if self.max_reinits <= 0:
            raise ApparatusFailure(
                f"the producer re-initialisation limit is {self.max_reinits}, so the "
                f"bound ADR-0003 section 6 sets cannot be enforced"
            )

    def to_jsonable(self) -> dict[str, Any]:
        """Return the form written into a run's evidence.

        Consistency is checked on the way out, so every run in the matrix exercises the
        control rather than only the runs that happened to need recovery.
        """
        self.assert_consistent()
        return {
            "producer_reinits": self.reinits,
            "retries": self.retries,
            "aborts": self.aborts,
            "max_reinits": self.max_reinits,
            "history": list(self.reasons),
        }


def resume_saga_index(durably_complete: Collection[int]) -> int:
    """Return the saga index a restarted phase resumes at.

    The first index not known to be durably complete. Gaps matter: if sagas 0, 1,
    and 3 are durable and 2 is not, the answer is 2, not 4. Skipping to 4 would
    step past unrecorded work and report a loss the configuration did not cause,
    which is the precise failure that would make a kill run's number untrue.

    Pure, and deliberately ignorant of how completeness was determined. The
    process phase learns it from the committed consumer offset, the ingest phase
    by reading the input topic back. Both then ask the same question here, which
    is what keeps the contract identical in both configurations.
    """
    complete = set(durably_complete)
    index = 0
    while index in complete:
        index += 1
    return index
