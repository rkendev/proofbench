"""C1, C2 and C3 computed by committed code, and refused when the evidence cannot bear them.

The denominator assertion is the whole point of this file, and it is tested before the
verdicts. C1 is a universal quantifier over a collection: all(clean for e in kills). On
an empty or mis-filtered collection that is True, and a false C1 PASS is the worst
outcome this project could ship, a claim that exactly-once held under kill published on
no evidence at all.

Three shapes of that hazard are covered: an empty matrix, a 19-run matrix, and a
call-site gate asserting the evaluator consults the check before producing any verdict.
A control nobody calls is not a control, and that failure has already appeared twice in
this repository.
"""

from __future__ import annotations

import ast
import inspect

import pytest

from proofbench.core.claims import (
    C2_REQUIRED_LOSS_RUNS,
    VERDICT_FAILED,
    VERDICT_HOLDS,
    DenominatorError,
    assert_denominators,
    evaluate,
    evaluate_c1,
    evaluate_c2,
    evaluate_c3,
    loss_capable_subset,
)
from proofbench.core.matrix import STATUS_CLEAN, STATUS_NOT_CLEAN, Execution, Matrix
from proofbench.core.replay import ReplayOutcome


def _execution(
    run_id,
    configuration,
    *,
    lost=0,
    duplicated=0,
    is_control=False,
    fault_type="consumer_sigkill_between_sinks",
    loss_possible=True,
    redeliveries=0,
):
    return Execution(
        run_id=run_id,
        configuration=configuration,
        fault_type="none" if is_control else fault_type,
        is_control=is_control,
        status=STATUS_CLEAN if not (lost or duplicated) else STATUS_NOT_CLEAN,
        duplicated=duplicated,
        lost=lost,
        loss_possible=False if is_control else loss_possible,
        transactions_committed=400,
        transactions_aborted=0,
        max_open_transaction_ms=53.0,
        recovery={},
        redeliveries=redeliveries,
    )


def _matrix(good_lost=0, baseline_loss_runs=0, capable=20):
    executions = []
    for configuration in ("good", "baseline"):
        executions.append(_execution(0, configuration, is_control=True))
        for run_id in range(1, 21):
            is_capable = run_id <= capable
            lost = 0
            if configuration == "baseline" and is_capable and run_id <= baseline_loss_runs:
                lost = 3
            if configuration == "good" and run_id <= good_lost:
                lost = 1
            executions.append(
                _execution(
                    run_id,
                    configuration,
                    lost=lost,
                    fault_type="consumer_sigkill_between_sinks"
                    if is_capable
                    else "producer_sigkill_mid_send",
                    loss_possible=is_capable,
                    # One witness, so the matrix satisfies the rule that the rebalance
                    # branch was actually entered.
                    redeliveries=1 if run_id == 3 else 0,
                )
            )
    return Matrix(executions=executions)


# --------------------------------------------------------------------------
# The denominator check, first
# --------------------------------------------------------------------------


def test_an_empty_matrix_produces_no_verdict() -> None:
    """The named red-proof. all() over nothing is True, so C1 would PASS."""
    with pytest.raises(Exception) as caught:
        evaluate(Matrix())
    assert "no executions" in str(caught.value) or "pre-registered" in str(caught.value)


def test_a_nineteen_run_matrix_produces_no_verdict() -> None:
    """The other named red-proof. A floor written for 20 must not score 19."""
    matrix = _matrix()
    matrix.executions = [
        e for e in matrix.executions if not (e.run_id == 20 and e.configuration == "good")
    ]
    with pytest.raises(Exception) as caught:
        evaluate(matrix)
    assert "42" in str(caught.value) or "exactly" in str(caught.value)


def test_a_whole_matrix_is_accepted() -> None:
    assert_denominators(_matrix())


def test_the_evaluator_consults_the_denominator_check_before_any_verdict() -> None:
    """A control nobody calls is not a control.

    Proven red by deleting the call, and by moving it after the verdicts are computed:
    a check that runs afterwards has already let the verdict exist.
    """
    tree = ast.parse(inspect.getsource(evaluate))
    called = [
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    assert "assert_denominators" in called, "evaluate does not consult the denominator check"

    source = inspect.getsource(evaluate)
    assert source.index("assert_denominators(matrix)") < source.index("evaluate_c1(matrix)"), (
        "the denominator check runs after a verdict has already been computed"
    )


# --------------------------------------------------------------------------
# The verdicts
# --------------------------------------------------------------------------


def test_c1_holds_on_a_clean_good_configuration() -> None:
    verdict = evaluate_c1(_matrix())
    assert verdict.verdict == VERDICT_HOLDS
    assert verdict.detail["denominator"] == 20


def test_one_lost_side_effect_fails_c1() -> None:
    """The floor tolerates nothing, so a single record decides it."""
    verdict = evaluate_c1(_matrix(good_lost=1))
    assert verdict.verdict == VERDICT_FAILED
    assert verdict.detail["runs_not_clean"] == [1]


def test_c2_holds_at_the_floor_and_fails_below_it() -> None:
    assert evaluate_c2(_matrix(baseline_loss_runs=C2_REQUIRED_LOSS_RUNS)).verdict == VERDICT_HOLDS
    assert (
        evaluate_c2(_matrix(baseline_loss_runs=C2_REQUIRED_LOSS_RUNS - 1)).verdict == VERDICT_FAILED
    )


def test_c3_fails_on_a_single_differing_checksum() -> None:
    matched = ReplayOutcome(0, "good", "a" * 64, "a" * 64, 600, 600, "sink_a")
    differs = ReplayOutcome(1, "good", "a" * 64, "b" * 64, 600, 599, "sink_a")
    assert evaluate_c3([matched], []).verdict == VERDICT_HOLDS
    assert evaluate_c3([matched, differs], [7]).verdict == VERDICT_FAILED
    assert evaluate_c3([matched, differs], [7]).detail["runs_excluded"] == [7]


def test_c3_refuses_an_empty_denominator() -> None:
    """An empty comparison reports a match, which would be a C3 pass on nothing."""
    with pytest.raises(DenominatorError, match="no replay outcomes"):
        evaluate_c3([], [])


# --------------------------------------------------------------------------
# The subset figure, which is not a claim
# --------------------------------------------------------------------------


def test_the_subset_figure_carries_no_threshold() -> None:
    """Without this a reader who sees 11 of 13 infers it cleared a bar nobody set."""
    figure = loss_capable_subset(_matrix(baseline_loss_runs=11, capable=13))
    assert figure["threshold"] is None
    assert figure["status"] == "report-only, not a claim"
    assert "not C2" in figure["note"]


def test_the_subset_reports_the_ceiling_beside_the_attained_figure() -> None:
    """A gap between them is evidence about the apparatus, not the configuration."""
    figure = loss_capable_subset(_matrix(baseline_loss_runs=7, capable=13))
    assert figure["ceiling"] == 13
    assert figure["attained"] == 7
    assert len(figure["runs_capable_without_loss"]) == 6


def test_the_subset_is_decomposed_by_fault_type() -> None:
    """Because whether the broker runs lose in practice was the open question."""
    figure = loss_capable_subset(_matrix(baseline_loss_runs=7, capable=13))
    assert "consumer_sigkill_between_sinks" in figure["by_fault_type"]


def test_a_c2_failure_puts_everything_report_only() -> None:
    """The ship rule, computed rather than remembered."""
    result = evaluate(_matrix(baseline_loss_runs=13))
    assert result["ship_rule"]["everything_ships_report_only"] is True
    assert result["loss_capable_subset"]["threshold"] is None
