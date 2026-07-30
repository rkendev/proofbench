"""The matrix-validity rule, and the control that stops it passing vacuously.

Apparatus failures are not scored, so letting an inconvenient run break removes it
from a denominator without anyone editing a number. The six rules in ADR-0004 each
close a different route to that, and they were pre-registered before the matrix ran,
which is the only time they could be written without knowing what they would exclude.

Every rule is a refusal, and a refusal that can never fire looks exactly like a matrix
with nothing wrong. That failure mode has appeared twice in this repository already, so
the control here is not an afterthought: the shape check runs before any rule is
consulted, and it is tested first.
"""

from __future__ import annotations

from typing import Any

import pytest

from proofbench.core.matrix import (
    MAX_APPARATUS_FAILURES,
    MAX_VOID_AND_RERUN_CYCLES,
    STATUS_APPARATUS_FAILURE,
    STATUS_CLEAN,
    Execution,
    Matrix,
    MatrixVoid,
    loss_capable_runs,
    loss_structurally_possible,
)


def _execution(
    run_id: int,
    configuration: str,
    status: str = STATUS_CLEAN,
    is_control: bool = False,
    lost: int = 0,
    diagnosis: str = "",
) -> Execution:
    return Execution(
        run_id=run_id,
        configuration=configuration,
        fault_type="none" if is_control else "consumer_sigkill_between_sinks",
        is_control=is_control,
        status=status,
        duplicated=0,
        lost=lost,
        loss_possible=not is_control,
        transactions_committed=400,
        transactions_aborted=0,
        max_open_transaction_ms=53.0,
        recovery={},
        diagnosis=diagnosis,
    )


def _whole_matrix(**overrides: Any) -> Matrix:
    """A complete, valid 42-execution matrix: 1 control plus 20 kill runs, twice."""
    executions = []
    for configuration in ("good", "baseline"):
        executions.append(_execution(0, configuration, is_control=True))
        for run_id in range(1, 21):
            executions.append(_execution(run_id, configuration))
    return Matrix(executions=executions, **overrides)


# --------------------------------------------------------------------------
# The positive control, first, because every rule below is a refusal
# --------------------------------------------------------------------------


def test_an_empty_matrix_is_refused_before_any_rule_is_consulted() -> None:
    """Every rule reads as satisfied over no executions.

    A partial-matrix check counting zero against 42 would fire here anyway, but that
    is luck rather than design: the count rule, the diagnosis rule and the denominator
    rule are all vacuously true on an empty collection. The shape check refuses first
    so no rule is ever asked a question about nothing.
    """
    with pytest.raises(MatrixVoid, match="no executions at all"):
        Matrix().assert_shippable()


def test_an_unknown_status_is_refused() -> None:
    """A typo would remove an execution from two rules at once.

    An unrecognised status counts as neither scoreable nor as an apparatus failure, so
    it vanishes from rule 3's threshold and rule 4's denominator simultaneously. That
    is the quietest possible way to drop an inconvenient run.
    """
    matrix = _whole_matrix()
    matrix.executions[5] = _execution(5, "good", status="mostly_fine")
    with pytest.raises(MatrixVoid, match="not among"):
        matrix.assert_shippable()


def test_a_matrix_with_one_configuration_is_refused() -> None:
    """It would satisfy the denominator rule for whichever one it holds.

    C2 is a comparison. A matrix holding only the good configuration has 20 scoreable
    kill runs and passes rule 4, while being incapable of saying anything about the
    claim it exists to evaluate.
    """
    matrix = Matrix(executions=[e for e in _whole_matrix().executions if e.configuration == "good"])
    with pytest.raises(MatrixVoid, match="configuration"):
        matrix.assert_shippable()


def test_a_whole_matrix_passes() -> None:
    """The control must not reject a valid matrix, or it voids what it protects."""
    _whole_matrix().assert_shippable()


# --------------------------------------------------------------------------
# Rule 1: a partial matrix never ships
# --------------------------------------------------------------------------


def test_a_forty_one_execution_matrix_is_refused() -> None:
    """The named red-proof. One missing execution is one a reader cannot see."""
    matrix = _whole_matrix()
    matrix.executions.pop()
    with pytest.raises(MatrixVoid, match="41 executions where 42"):
        matrix.assert_shippable()


# --------------------------------------------------------------------------
# Rule 2: every apparatus failure is diagnosed in writing
# --------------------------------------------------------------------------


def test_an_undiagnosed_apparatus_failure_is_refused() -> None:
    """Undiagnosed is indistinguishable from allowed-to-break."""
    matrix = _whole_matrix()
    matrix.executions[3] = _execution(3, "good", status=STATUS_APPARATUS_FAILURE)
    with pytest.raises(MatrixVoid, match="no written diagnosis"):
        matrix.assert_shippable()


# --------------------------------------------------------------------------
# Rules 3 and 4: the threshold, and the exact denominator
# --------------------------------------------------------------------------


def test_three_apparatus_failures_void_the_matrix() -> None:
    """The named red-proof for rule 3."""
    matrix = _whole_matrix()
    for index in range(MAX_APPARATUS_FAILURES + 1):
        matrix.executions[index] = _execution(
            index, "good", status=STATUS_APPARATUS_FAILURE, diagnosis="broker refused a connection"
        )
    with pytest.raises(MatrixVoid, match="past the pre-registered limit"):
        matrix.assert_shippable()


def test_one_apparatus_failure_among_the_kill_runs_still_voids_the_matrix() -> None:
    """Rule 4, which is independent of rule 3's count and stricter than it.

    A single broken kill run is inside the threshold and still fatal, because C1 and
    C2 are pre-registered over exactly 20. Scoring 19 against a floor written for 20
    is cherry-picking by another route, so the claim becomes not-evaluable rather than
    evaluated on a smaller denominator.
    """
    matrix = _whole_matrix()
    matrix.executions[7] = _execution(
        7, "good", status=STATUS_APPARATUS_FAILURE, diagnosis="the coordinator never returned"
    )
    assert len(matrix.apparatus_failures()) <= MAX_APPARATUS_FAILURES
    with pytest.raises(MatrixVoid, match="19 scoreable kill runs where"):
        matrix.assert_shippable()


def test_the_threshold_allowance_covers_only_the_control_executions() -> None:
    """Which is what rule 4 reduces rule 3's allowance to, in practice.

    Two apparatus failures are tolerable only if both are controls, because any
    failure among the 40 kill executions trips the denominator rule first. Stated as a
    test so the interaction between the two rules is visible rather than inferred.
    """
    matrix = _whole_matrix()
    for configuration in ("good", "baseline"):
        index = next(
            i
            for i, e in enumerate(matrix.executions)
            if e.configuration == configuration and e.is_control
        )
        matrix.executions[index] = _execution(
            0,
            configuration,
            status=STATUS_APPARATUS_FAILURE,
            is_control=True,
            diagnosis="the control run could not provision its topics",
        )
    matrix.assert_shippable()


# --------------------------------------------------------------------------
# Rule 6: the void-and-rerun cap
# --------------------------------------------------------------------------


def test_a_fourth_cycle_is_refused() -> None:
    """The named red-proof. Each cycle is legitimate; the aggregate is overfitting.

    Repairing until the apparatus yields a clean matrix is the failure the floors
    exist to prevent, one level up, so the number of repairs is itself bounded and the
    incompletable outcome is publishable.
    """
    _whole_matrix(cycle=MAX_VOID_AND_RERUN_CYCLES).assert_shippable()
    with pytest.raises(MatrixVoid, match="past the pre-registered cap"):
        _whole_matrix(cycle=MAX_VOID_AND_RERUN_CYCLES + 1).assert_shippable()


# --------------------------------------------------------------------------
# The loss-possibility predicate, which is where ADR-0004's 13 comes from
# --------------------------------------------------------------------------


def test_the_ingest_fault_cannot_lose() -> None:
    """By construction of the frozen resume rule, not by luck or by measurement."""
    entry = {
        "run_id": 1,
        "fault_type": "producer_sigkill_mid_send",
        "fault_point": {"saga_index": 96, "step_index": 2},
    }
    assert not loss_structurally_possible(entry)


def test_the_other_two_faults_can_lose() -> None:
    for fault_type in ("consumer_sigkill_between_sinks", "broker_stop_start"):
        entry = {
            "run_id": 2,
            "fault_type": fault_type,
            "fault_point": {"saga_index": 108, "step_index": 1},
        }
        assert loss_structurally_possible(entry)


def test_the_control_cannot_lose_because_nothing_is_killed() -> None:
    assert not loss_structurally_possible({"run_id": 0, "fault_type": "none", "fault_point": None})


def test_the_ceiling_of_thirteen_is_computed_from_the_frozen_schedule() -> None:
    """ADR-0004's C2 prediction, derived rather than typed.

    Seven producer kills cannot lose, so the maximum attainable C2 numerator is the
    seven consumer kills plus the six broker kills. Thirteen against a floor of
    sixteen. The number comes out of the committed artifact here so a reader can check
    the arithmetic without trusting the ADR's prose.

    Proven red by marking producer_sigkill_mid_send loss-capable, which takes the
    ceiling to 20 and makes the prediction disappear.
    """
    import json

    from proofbench.config import Settings, repo_root

    settings = Settings(_env_file=None)
    payload = json.loads((repo_root() / settings.schedule_path).read_text(encoding="utf-8"))
    kills = [run for run in payload["runs"] if run["fault_point"] is not None]

    assert len(kills) == settings.kill_runs == 20
    capable = loss_capable_runs(kills)
    assert len(capable) == 13, (
        f"the loss-capable ceiling is {len(capable)}, not 13. ADR-0004's prediction that "
        f"C2 cannot reach its floor of 16 depends on this number, so a change here "
        f"changes a pre-registered claim outcome."
    )
    # And the floor it is measured against is genuinely out of reach.
    floor = 16
    assert len(capable) < floor


def test_the_predicate_is_not_uniformly_true_or_false() -> None:
    """The positive control for the ceiling.

    A predicate stuck at False would give a ceiling of 0 and a predicate stuck at True
    a ceiling of 20. Both are numbers, and neither would look obviously wrong in a
    document. So both directions are exercised.
    """
    import json

    from proofbench.config import Settings, repo_root

    settings = Settings(_env_file=None)
    payload = json.loads((repo_root() / settings.schedule_path).read_text(encoding="utf-8"))
    kills = [run for run in payload["runs"] if run["fault_point"] is not None]

    capable = set(loss_capable_runs(kills))
    incapable = {int(run["run_id"]) for run in kills} - capable
    assert capable, "the predicate says no run can lose, so the ceiling is meaningless"
    assert incapable, "the predicate says every run can lose, so it is not discriminating"
    assert len(capable) + len(incapable) == 20
