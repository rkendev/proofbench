"""C1, C2 and C3 computed by committed code against the frozen floors.

No verdict in this project is read off a matrix by eye. That is INV-P2's rule applied
to the claims themselves: the counts come out of a differ, and the verdicts come out of
here.

**The denominators are asserted before anything is evaluated, and that ordering is the
most important thing in this module.** C1 is a universal quantifier: ``all(execution is
clean for execution in good_kill_runs)``. Over an empty collection that is True, and a
false C1 PASS is the worst outcome this project could ship: a claim that exactly-once
held under kill, published on no evidence, from a harness whose entire argument is that
its numbers are checkable. A mis-filtered collection produces the same True just as
quietly.

So ``assert_denominators`` runs first, refuses anything but exactly the pre-registered
counts, and is called from the evaluation path rather than only from a test. A control
nobody calls is not a control, and this repository has already found that failure twice.

The floors are CLAIMS.md's, restated here only as the numbers the evaluator compares
against, never reinterpreted:

- **C1**: zero duplicated and zero lost across all 20 kill runs under good. One of
  either means FAILED.
- **C2**: at least one lost side effect in at least 80 percent of the same 20 runs
  under baseline, so at least 16 of 20. Below it the harness is declared insensitive
  and every result ships report-only.
- **C3**: a replay rebuilds the sink ledger to the same checksum. Any difference ships
  as a documented negative.

The loss-capable subset figure is computed here too, and it is **not a claim**. It has
no floor, nothing passes or fails on it, and ADR-0004 says so in as many words.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from proofbench.core.matrix import Execution, Matrix
from proofbench.core.replay import ReplayOutcome

GOOD = "good"
BASELINE = "baseline"

# CLAIMS.md's floors, as numbers. C1 tolerates nothing; C2 needs 80 percent of 20.
C1_MAX_DUPLICATED = 0
C1_MAX_LOST = 0
C2_REQUIRED_LOSS_RUNS = 16
C2_DENOMINATOR = 20

VERDICT_HOLDS = "HOLDS"
VERDICT_FAILED = "FAILED"
VERDICT_NOT_EVALUABLE = "NOT_EVALUABLE"


class DenominatorError(Exception):
    """The matrix cannot support a verdict, so none is produced.

    Distinct from a failed claim. A failed claim is a result and ships as one; this is
    the evaluator refusing to answer a question the evidence cannot support.
    """


@dataclass(frozen=True, slots=True)
class Verdict:
    """One claim's outcome, and the numbers behind it."""

    claim: str
    verdict: str
    floor: str
    observed: str
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def holds(self) -> bool:
        return self.verdict == VERDICT_HOLDS

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "claim": self.claim,
            "verdict": self.verdict,
            "floor": self.floor,
            "observed": self.observed,
            **self.detail,
        }


def assert_denominators(matrix: Matrix) -> None:
    """Refuse to evaluate anything unless the evidence is exactly what was registered.

    Called from ``evaluate`` before any verdict is computed, not merely tested. The
    order is the point: C1 is ``all(...)`` over a collection, which is True when the
    collection is empty, so a matrix that lost its executions would produce a C1 PASS
    rather than an error. Nothing downstream could tell that verdict from a real one.

    Every count is exact rather than a minimum. "At least 20" would let a 21st run in
    and "at most 20" would let 19 through, and both are ways for a denominator to stop
    being the one the floor was written against.
    """
    matrix.assert_shippable()

    for configuration in (GOOD, BASELINE):
        kills = [e for e in matrix.kill_runs(configuration) if e.is_scoreable]
        if len(kills) != C2_DENOMINATOR:
            raise DenominatorError(
                f"C1 and C2 are pre-registered over exactly {C2_DENOMINATOR} kill runs, and "
                f"the {configuration} configuration offers {len(kills)}. No verdict is "
                f"produced: an all() over the wrong collection returns a pass, and a false "
                f"C1 pass is the worst thing this project could publish."
            )

    capable = [e for e in matrix.kill_runs(BASELINE) if e.loss_possible]
    if not capable:
        raise DenominatorError(
            "no kill run is marked loss-capable, so the subset figure would be computed "
            "over an empty collection and would read as zero of zero"
        )


def evaluate_c1(matrix: Matrix) -> Verdict:
    """Zero duplicated and zero lost across all 20 kill runs under good."""
    kills = [e for e in matrix.kill_runs(GOOD) if e.is_scoreable]
    duplicated = sum(e.duplicated for e in kills)
    lost = sum(e.lost for e in kills)
    offenders = [e.run_id for e in kills if e.duplicated or e.lost]

    holds = duplicated <= C1_MAX_DUPLICATED and lost <= C1_MAX_LOST
    return Verdict(
        claim="C1",
        verdict=VERDICT_HOLDS if holds else VERDICT_FAILED,
        floor="zero duplicated and zero lost across all 20 kill runs under good",
        observed=f"{duplicated} duplicated, {lost} lost across {len(kills)} kill runs",
        detail={
            "denominator": len(kills),
            "duplicated": duplicated,
            "lost": lost,
            "runs_not_clean": offenders,
        },
    )


def evaluate_c2(matrix: Matrix) -> Verdict:
    """At least one lost side effect in at least 16 of the same 20 runs under baseline."""
    kills = [e for e in matrix.kill_runs(BASELINE) if e.is_scoreable]
    exhibited = [e.run_id for e in kills if e.exhibited_loss]

    holds = len(exhibited) >= C2_REQUIRED_LOSS_RUNS
    return Verdict(
        claim="C2",
        verdict=VERDICT_HOLDS if holds else VERDICT_FAILED,
        floor=f"at least {C2_REQUIRED_LOSS_RUNS} of {C2_DENOMINATOR} kill runs lose at least one",
        observed=f"{len(exhibited)} of {len(kills)} kill runs exhibited loss",
        detail={
            "denominator": len(kills),
            "runs_exhibiting_loss": exhibited,
            "required": C2_REQUIRED_LOSS_RUNS,
        },
    )


def loss_capable_subset(matrix: Matrix) -> dict[str, Any]:
    """The baseline's loss rate over only the runs where loss is structurally possible.

    **Report-only, and not a claim.** No floor applies to it, nothing passes or fails
    on it, and it is never presented as C2 passing or as C2 passing on a subset. It is
    pre-registered in ADR-0004 precisely so it cannot become a post-hoc subgroup
    analysis chosen after the numbers were visible.

    The ceiling and the attained figure are both reported. The ceiling is what the
    frozen schedule permits; the attained figure is what the matrix measured. A gap
    between them is itself evidence about the apparatus rather than about the
    configuration, and a reader needs both to see it.
    """
    kills = [e for e in matrix.kill_runs(BASELINE) if e.is_scoreable]
    capable = [e for e in kills if e.loss_possible]
    exhibited = [e.run_id for e in capable if e.exhibited_loss]

    return {
        "figure": "baseline loss rate over the loss-capable subset",
        "status": "report-only, not a claim",
        "threshold": None,
        "note": (
            "No floor applies to this figure. It is not C2, it is not C2 restricted to a "
            "subset, and nothing passes or fails on it. The ceiling is what the frozen "
            "schedule permits; the attained figure is what the matrix measured."
        ),
        "ceiling": len(capable),
        "attained": len(exhibited),
        "denominator": len(capable),
        "runs_exhibiting_loss": exhibited,
        "runs_capable_without_loss": [e.run_id for e in capable if not e.exhibited_loss],
        "by_fault_type": _subset_by_fault_type(capable),
    }


def _subset_by_fault_type(capable: Sequence[Execution]) -> dict[str, dict[str, int]]:
    """Split the subset by fault type, because that is where the open question lives.

    The ingest runs are excluded from the subset by proof. Whether the broker runs lose
    in practice was an open empirical question when ADR-0004 was written, so the matrix
    reports them separately rather than folding them into one rate a reader cannot
    decompose.
    """
    grouped: dict[str, dict[str, int]] = {}
    for execution in capable:
        bucket = grouped.setdefault(execution.fault_type, {"runs": 0, "exhibited_loss": 0})
        bucket["runs"] += 1
        bucket["exhibited_loss"] += 1 if execution.exhibited_loss else 0
    return grouped


def evaluate_c3(outcomes: Sequence[ReplayOutcome], excluded: Sequence[int]) -> Verdict:
    """A replay rebuilds the sink ledger to the same checksum.

    The denominator is named explicitly and every excluded run listed, so nobody has to
    reverse-engineer why 21 became 19.
    """
    if not outcomes:
        raise DenominatorError(
            "C3 has no replay outcomes to evaluate, and an empty comparison reports a "
            "match. No verdict is produced."
        )

    mismatches = [o for o in outcomes if not o.matched]
    runs = sorted({o.run_id for o in outcomes})
    holds = not mismatches
    return Verdict(
        claim="C3",
        verdict=VERDICT_HOLDS if holds else VERDICT_FAILED,
        floor="every replay rebuilds the sink ledger to the same checksum",
        observed=f"{len(outcomes) - len(mismatches)} of {len(outcomes)} sink replays matched",
        detail={
            "denominator": len(outcomes),
            "runs_replayed": runs,
            "runs_excluded": sorted(excluded),
            "mismatches": [o.to_jsonable() for o in mismatches],
        },
    )


def evaluate(
    matrix: Matrix,
    replays: Sequence[ReplayOutcome] = (),
    replay_excluded: Sequence[int] = (),
) -> dict[str, Any]:
    """Every verdict, with the denominators asserted before any of them is computed."""
    # FIRST. Before any verdict exists. See assert_denominators.
    assert_denominators(matrix)

    c1 = evaluate_c1(matrix)
    c2 = evaluate_c2(matrix)
    verdicts = [c1, c2]
    if replays:
        verdicts.append(evaluate_c3(replays, replay_excluded))

    ships_report_only = not c2.holds
    return {
        "verdicts": [v.to_jsonable() for v in verdicts],
        "loss_capable_subset": loss_capable_subset(matrix),
        "ship_rule": {
            "c1_failed_means_failed_headline": not c1.holds,
            "c2_failed_means_report_only": ships_report_only,
            "everything_ships_report_only": ships_report_only,
        },
    }
