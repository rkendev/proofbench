"""The failure-evidence matrix, and the rules that decide whether it may ship.

CLAIMS.md calls the harness and this matrix the product. Everything here is about
keeping that product honest under one specific pressure: apparatus failures are not
scored, so letting an inconvenient run break is a way to remove it from a denominator
without anyone editing a number. The rules below were pre-registered in ADR-0004
before the matrix executed, which is the only time they could be written without
knowing what they would exclude.

Six rules, and each closes a different route to the same abuse:

1. A partial matrix never ships. All 42 executions are attempted or there is no result.
2. Every apparatus failure is diagnosed in writing. None is absorbed.
3. More than 2 apparatus failures out of 42 voids the entire matrix.
4. Independently of the count: C1 and C2 are evaluated over exactly 20 kill runs per
   configuration, so any apparatus failure among those 20 makes that claim
   not-evaluable and voids the matrix. Rule 3's allowance therefore covers only the two
   control executions. Scoring 19 of 20 against a floor written for 20 is the same
   cherry-picking by another route.
5. No run is re-executed to see whether it was a flake.
6. At most 3 void-and-rerun cycles. Each cycle is individually legitimate, but the
   aggregate is a slow route to overfitting the apparatus until it yields a clean
   matrix, which is the failure the floors exist to prevent one level up.

**The loss-possibility predicate is here rather than in the evaluator**, because it is
a property of the frozen schedule rather than of any result. It is what makes ADR-0004's
C2 arithmetic checkable by a reader instead of assertable by the author: the ceiling of
13 is computed from the committed schedule, not typed into a document.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from proofbench.core.faults import FAULT_PRODUCER_SIGKILL

STATUS_CLEAN = "clean"
STATUS_NOT_CLEAN = "not_clean"
STATUS_APPARATUS_FAILURE = "apparatus_failure"

SCOREABLE_STATUSES = frozenset({STATUS_CLEAN, STATUS_NOT_CLEAN})
ALL_STATUSES = SCOREABLE_STATUSES | {STATUS_APPARATUS_FAILURE}

# Rule 3. Low on purpose, and the T-prompt's own view: any apparatus failure at all
# must be diagnosed in writing rather than absorbed, so the threshold exists to bound
# how much diagnosis is tolerable before the apparatus itself is the finding.
MAX_APPARATUS_FAILURES = 2

# Rule 6. After the third, the project ships the incompletable outcome, naming every
# void and its diagnosis. "The matrix could not be completed and here is the full
# record of why" is publishable under the same logic that makes a FAILED C1
# publishable.
MAX_VOID_AND_RERUN_CYCLES = 3


class MatrixVoid(Exception):
    """The matrix may not ship, and no claim may be read from it."""


def loss_structurally_possible(entry: dict[str, Any]) -> bool:
    """Can a lost side effect occur in this run at all, under either configuration?

    Not a prediction about what will happen. A statement about what the frozen
    contract makes possible, decided by the resume rule rather than by any result.

    ``producer_sigkill_mid_send`` targets the ingest producer, and ADR-0003 section 7
    freezes the ingest resume rule: the durable state is the input topic read back at
    startup, a saga counts as complete only when all M steps are visible, and the
    resume point is the first index not known to be complete with gaps never skipped.
    After any number of ingest kills the visible input topic therefore contains every
    saga at least once, the process phase writes all of it, and nothing can be missing.
    The baseline duplicates across such a resume; neither configuration loses.

    So those runs can duplicate but cannot lose, in either configuration, by
    construction rather than by luck. The other two fault types can: a consumer kill
    leaves the committed offset ahead of applied work, and a broker outage makes an
    in-flight send fail permanently after the offset was committed.

    This is the function behind ADR-0004's prediction that C2 cannot reach its floor.
    Computed from the committed schedule so a reader can check the arithmetic without
    trusting the prose, and carried as a column in the matrix for the same reason.
    """
    if entry["fault_point"] is None:
        return False
    return str(entry["fault_type"]) != FAULT_PRODUCER_SIGKILL


def loss_capable_runs(entries: Sequence[dict[str, Any]]) -> list[int]:
    """The run ids where loss is structurally possible, from the frozen schedule."""
    return [int(entry["run_id"]) for entry in entries if loss_structurally_possible(entry)]


@dataclass(frozen=True, slots=True)
class Execution:
    """One run under one configuration: the unit the matrix is made of."""

    run_id: int
    configuration: str
    fault_type: str
    is_control: bool
    status: str
    duplicated: int
    lost: int
    loss_possible: bool
    transactions_committed: int
    transactions_aborted: int
    max_open_transaction_ms: float
    recovery: dict[str, Any]
    diagnosis: str = ""

    @property
    def is_scoreable(self) -> bool:
        return self.status in SCOREABLE_STATUSES

    @property
    def exhibited_loss(self) -> bool:
        return self.lost > 0

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "configuration": self.configuration,
            "fault_type": self.fault_type,
            "control": self.is_control,
            "status": self.status,
            "duplicated": self.duplicated,
            "lost": self.lost,
            "loss_structurally_possible": self.loss_possible,
            "transactions_committed": self.transactions_committed,
            "transactions_aborted": self.transactions_aborted,
            "max_open_transaction_ms": self.max_open_transaction_ms,
            "recovery": self.recovery,
            "diagnosis": self.diagnosis,
        }


@dataclass(slots=True)
class Matrix:
    """Every execution, plus how many times this matrix has been voided and re-run."""

    executions: list[Execution] = field(default_factory=list)
    expected_executions: int = 42
    kill_runs_per_configuration: int = 20
    cycle: int = 1

    def for_configuration(self, configuration: str) -> list[Execution]:
        return [e for e in self.executions if e.configuration == configuration]

    def kill_runs(self, configuration: str) -> list[Execution]:
        return [e for e in self.for_configuration(configuration) if not e.is_control]

    def apparatus_failures(self) -> list[Execution]:
        return [e for e in self.executions if e.status == STATUS_APPARATUS_FAILURE]

    def configurations(self) -> list[str]:
        return sorted({e.configuration for e in self.executions})

    # ------------------------------------------------------------------
    # The validity rule
    # ------------------------------------------------------------------

    def assert_shippable(self) -> None:
        """Refuse to let a matrix be read as a result unless every rule holds.

        Raises ``MatrixVoid`` rather than returning a verdict, because a caller that
        could ignore a boolean is a caller that will. Every rule names the abuse it
        closes, so a reader of the failure knows which one fired.
        """
        self._assert_the_shape_is_real()

        # Rule 1: a partial matrix never ships.
        if len(self.executions) != self.expected_executions:
            raise MatrixVoid(
                f"the matrix holds {len(self.executions)} executions where "
                f"{self.expected_executions} were pre-registered. A partial matrix never "
                f"ships: the missing executions are exactly the ones a reader cannot see, "
                f"and no claim may be read from a denominator that quietly shrank."
            )

        # Rule 2: every apparatus failure carries a written diagnosis.
        undiagnosed = [e for e in self.apparatus_failures() if not e.diagnosis.strip()]
        if undiagnosed:
            raise MatrixVoid(
                f"{len(undiagnosed)} apparatus failure(s) carry no written diagnosis, "
                f"starting with run {undiagnosed[0].run_id} under "
                f"{undiagnosed[0].configuration}. An undiagnosed apparatus failure is "
                f"indistinguishable from a run that was allowed to break."
            )

        # Rule 3: the count threshold.
        failures = self.apparatus_failures()
        if len(failures) > MAX_APPARATUS_FAILURES:
            raise MatrixVoid(
                f"{len(failures)} of {len(self.executions)} executions ended as apparatus "
                f"failures, past the pre-registered limit of {MAX_APPARATUS_FAILURES}. The "
                f"matrix is void: nothing ships, the cause is fixed, and it is re-run from "
                f"scratch with the void recorded."
            )

        # Rule 4: the claim denominators are exact, independently of the count.
        for configuration in self.configurations():
            kills = self.kill_runs(configuration)
            scoreable = [e for e in kills if e.is_scoreable]
            if len(scoreable) != self.kill_runs_per_configuration:
                raise MatrixVoid(
                    f"the {configuration} configuration has {len(scoreable)} scoreable kill "
                    f"runs where C1 and C2 are pre-registered over exactly "
                    f"{self.kill_runs_per_configuration}. Scoring a smaller denominator "
                    f"against a floor written for 20 is cherry-picking by another route, so "
                    f"the claim is not evaluable and the matrix is void."
                )

        # Rule 6: the void-and-rerun cap.
        if self.cycle > MAX_VOID_AND_RERUN_CYCLES:
            raise MatrixVoid(
                f"this is void-and-rerun cycle {self.cycle}, past the pre-registered cap of "
                f"{MAX_VOID_AND_RERUN_CYCLES}. Each cycle is individually legitimate and the "
                f"aggregate is a slow route to overfitting the apparatus until it yields a "
                f"clean matrix. The project ships the incompletable outcome instead, naming "
                f"every void and its diagnosis."
            )

    def _assert_the_shape_is_real(self) -> None:
        """The positive control, carried from this rule's first commit.

        Every rule above is a refusal, and a refusal that can never fire is
        indistinguishable from a matrix with nothing wrong. That failure mode has
        already appeared twice in this repository, so this rule does not get to acquire
        a control later.

        Three things are checked before any rule is allowed to pass. The collection is
        not empty, because every rule below reads as satisfied over no executions. Every
        status is one the rules know about, because a typo would make an apparatus
        failure invisible to the count in rule 3 and to the denominator in rule 4 at the
        same time. And both configurations are present, because a matrix holding only
        one would satisfy rule 4 for the one it holds.
        """
        if not self.executions:
            raise MatrixVoid(
                "the matrix holds no executions at all. Every rule below is satisfied by "
                "an empty matrix, so an empty matrix must be refused before they are "
                "consulted rather than passing all of them."
            )

        unknown = sorted({e.status for e in self.executions} - ALL_STATUSES)
        if unknown:
            raise MatrixVoid(
                f"execution status(es) {unknown} are not among {sorted(ALL_STATUSES)}. An "
                f"unrecognised status is counted as neither scoreable nor as an apparatus "
                f"failure, so it would vanish from the rule 3 count and the rule 4 "
                f"denominator simultaneously."
            )

        configurations = self.configurations()
        if len(configurations) != 2:
            raise MatrixVoid(
                f"the matrix holds {len(configurations)} configuration(s), {configurations}. "
                f"C2 compares two, and a matrix with one satisfies the denominator rule for "
                f"whichever one it happens to hold."
            )

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "cycle": self.cycle,
            "expected_executions": self.expected_executions,
            "executions": [e.to_jsonable() for e in self.executions],
        }
