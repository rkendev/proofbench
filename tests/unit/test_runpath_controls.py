"""Positive controls for guards on the RUN PATH, which fail open rather than red.

A vacuous guard in a test fails in CI, loudly, before anything ships. A vacuous guard
here fails open during the matrix, and the number it should have stopped is written
into the evidence as a claim result.

Three guards on the run path share the shape that produced commit 8's false green: on a
healthy run they are either never consulted or correctly empty, so "nothing has gone
wrong" and "the check is broken" look identical from the outside. Each now answers a
question with a known answer before it is trusted with a real one.

The fourth guard the sweep asked about, the matrix-validity rule, does not exist yet.
It arrives with the matrix runner and carries its control from the first commit rather
than acquiring one afterwards.
"""

from __future__ import annotations

import pytest

from proofbench.core.recovery import ApparatusFailure, RecoveryBudget, TransactionOutcome
from proofbench.core.window import (
    WindowFacts,
    assert_boundary_discriminates,
    is_within_fault_window,
)

# --------------------------------------------------------------------------
# 1. The in-window predicate
# --------------------------------------------------------------------------


def test_the_boundary_control_passes_on_a_working_boundary() -> None:
    """It is consulted on every delivery failure, so it must not be expensive or flaky."""
    assert assert_boundary_discriminates() is None


def test_the_boundary_control_is_asking_a_real_question() -> None:
    """Both probe states have answers fixed by construction, in opposite directions.

    A control that only checked one direction would miss the dangerous one. Stuck at
    True absorbs every apparatus break as recovery and inflates C2 on evidence that
    looks ordinary; stuck at False merely voids the broker runs, which is visible as a
    hole in the matrix.
    """
    definitely_in = WindowFacts(
        entry_names_a_fault=True,
        fault_has_fired=True,
        window_closed=False,
        budget_exhausted=False,
    )
    definitely_out = WindowFacts(
        entry_names_a_fault=False,
        fault_has_fired=False,
        window_closed=True,
        budget_exhausted=True,
    )
    assert is_within_fault_window(definitely_in)
    assert not is_within_fault_window(definitely_out)


def test_a_boundary_stuck_open_is_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    """The seeded violation: the predicate answers True to everything.

    This is the direction that would ship a wrong number quietly. On a healthy run the
    boundary is never consulted, so without the control a predicate stuck at True is
    invisible until it absorbs a real apparatus break.
    """
    from proofbench.core import window

    monkeypatch.setattr(window, "is_within_fault_window", lambda state: True)
    with pytest.raises(ApparatusFailure, match="no longer distinguishes"):
        window.assert_boundary_discriminates()


def test_a_boundary_stuck_closed_is_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    """The PB-T2 direction. Less dangerous, still refused."""
    from proofbench.core import window

    monkeypatch.setattr(window, "is_within_fault_window", lambda state: False)
    with pytest.raises(ApparatusFailure, match="no longer distinguishes"):
        window.assert_boundary_discriminates()


def test_the_run_path_actually_consults_the_control() -> None:
    """A control nobody calls is not a control.

    Caught by its own red-proof: removing the call from ``resolve_delivery_error`` left
    every other test in this file green, because they all exercise the control directly
    rather than through the path that needs it. A positive control needs a call-site
    gate for the same reason a guard needs a positive control.
    """
    import ast
    import inspect

    from proofbench.core.run import resolve_delivery_error

    tree = ast.parse(inspect.getsource(resolve_delivery_error))
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "assert_boundary_discriminates" in called, (
        "resolve_delivery_error does not consult the boundary control, so a predicate "
        "stuck open would absorb every apparatus break as recovery unnoticed"
    )
    assert "is_within_fault_window" in called

    # And the control runs BEFORE the decision it protects.
    source = inspect.getsource(resolve_delivery_error)
    assert source.index("assert_boundary_discriminates()") < source.index(
        "if not is_within_fault_window(state):"
    )


def test_the_attributability_control_runs_before_the_early_return() -> None:
    """Otherwise the vacuous path is reached before the cross-check can fire.

    The early return on an empty key set is correct, and it is also exactly what a
    broken extraction produces. The cross-check has to sit in front of it.
    """
    import inspect

    from proofbench.core.run import assert_losses_are_attributable

    source = inspect.getsource(assert_losses_are_attributable)
    assert source.index("if reported and not lost_keys:") < source.index("if not lost_keys:")


# --------------------------------------------------------------------------
# 2. The attributability invariant
# --------------------------------------------------------------------------


class _Diff:
    def __init__(self, lost: tuple[object, ...]) -> None:
        self.lost = lost
        self.duplicated: tuple[object, ...] = ()


class _Sink:
    def __init__(self, lost: tuple[object, ...]) -> None:
        self.diff = _Diff(lost)


class _Record:
    def __init__(self, key: str) -> None:
        self.idempotency_key = key


def test_an_empty_diff_is_correctly_vacuous() -> None:
    """No loss means nothing to attribute, and that is the expected shape under good.

    The invariant being vacuous here is right rather than a defect. What the control
    below adds is the ability to tell this case apart from a broken walk.
    """
    from proofbench.core.run import assert_losses_are_attributable

    sinks = [_Sink(()), _Sink(())]
    assert_losses_are_attributable(configuration=None, sinks=sinks, gaps=[], steps_per_saga=3)


def test_a_diff_that_reports_loss_but_yields_no_keys_is_caught(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The seeded violation: the extraction cannot see the loss it exists to explain.

    A wrong attribute, a changed diff shape, or a comprehension over the wrong
    collection all produce no keys. Without the cross-check the early return fires and
    a run with real unattributable loss reports a claim result instead of stopping.

    Simulated by substituting the extraction, which is what a bug in it would do. The
    diff still reports two lost records, so the two routes disagree and the control
    fires before anything touches a broker.
    """
    from proofbench.core import run

    monkeypatch.setattr(run, "lost_keys_of", lambda sinks: [])
    sinks = [_Sink((_Record("s-0096:charge_card"), _Record("s-0096:send_confirmation")))]

    with pytest.raises(ApparatusFailure, match="extracted no keys"):
        run.assert_losses_are_attributable(
            configuration=None, sinks=sinks, gaps=[], steps_per_saga=3
        )


def test_the_two_routes_really_are_different_code() -> None:
    """Which is the entire value of a control computed a second way.

    One counts records per sink without looking at their contents; the other builds a
    set of identities across sinks. If both were the same comprehension written twice,
    a single bug would move them together and the comparison would prove nothing.
    """
    from proofbench.core.run import lost_keys_of, reported_loss_count

    sinks = [
        _Sink((_Record("a"), _Record("a"), _Record("b"))),
        _Sink((_Record("b"),)),
    ]
    # Four lost records, two distinct keys. The routes disagree in value on purpose.
    assert reported_loss_count(sinks) == 4
    assert lost_keys_of(sinks) == ["a", "b"]


def test_the_control_does_not_fire_on_a_genuine_clean_run() -> None:
    """Both routes agree at zero, which is the shape of every good-configuration run."""
    from proofbench.core.run import lost_keys_of, reported_loss_count

    sinks = [_Sink(()), _Sink(())]
    assert reported_loss_count(sinks) == 0
    assert lost_keys_of(sinks) == []


# --------------------------------------------------------------------------
# 3. The recovery budget bound
# --------------------------------------------------------------------------


def test_a_consistent_budget_passes() -> None:
    budget = RecoveryBudget()
    budget.record(TransactionOutcome.RETRY, "coordinator re-electing")
    budget.record(TransactionOutcome.ABORT_AND_REPLAY, "transaction unusable")
    budget.assert_consistent()
    assert budget.to_jsonable()["retries"] == 1


def test_a_clean_run_is_consistent_too() -> None:
    """Zero everywhere is a legitimate state, and the control must not reject it.

    Every run in the matrix serializes its budget, so a control that fired on clean
    runs would void the whole matrix rather than protect it.
    """
    RecoveryBudget().assert_consistent()


def test_a_counter_that_moved_without_a_reason_is_caught() -> None:
    """The seeded violation: something recovered without going through record.

    That is exactly the failure the bound cannot see on its own. reinits stays at
    whatever record last set it, the bound never fires, and a run that re-initialised
    its producer repeatedly is indistinguishable from a run that never needed to.
    """
    budget = RecoveryBudget()
    budget.reinits = 2  # a recovery that bypassed record
    with pytest.raises(ApparatusFailure, match="recovered without being counted"):
        budget.assert_consistent()


def test_a_reason_without_a_counter_is_caught() -> None:
    """The mirror image, and equally a sign that the accounting is not the truth."""
    budget = RecoveryBudget()
    budget.reasons.append("retry: something happened")
    with pytest.raises(ApparatusFailure, match="recovered without being counted"):
        budget.assert_consistent()


def test_a_limit_that_cannot_bind_is_caught() -> None:
    """A non-positive limit makes the ADR-0003 section 6 bound unenforceable."""
    with pytest.raises(ApparatusFailure, match="cannot be enforced"):
        RecoveryBudget(max_reinits=0).assert_consistent()


def test_the_control_runs_on_every_run_not_only_the_recovering_ones() -> None:
    """Serialization is the trigger, and every run writes its budget into evidence.

    A control that only fired when recovery happened would be absent from exactly the
    runs where a silent accounting failure looks most like success.
    """
    import inspect

    source = inspect.getsource(RecoveryBudget.to_jsonable)
    assert "self.assert_consistent()" in source


def test_the_bound_still_fires_when_the_accounting_is_honest() -> None:
    """The control is additional to the bound, not a replacement for it."""
    budget = RecoveryBudget()
    for _ in range(budget.max_reinits):
        budget.record(TransactionOutcome.REINIT_PRODUCER, "producer fenced")
    budget.assert_consistent()
    with pytest.raises(ApparatusFailure, match="the run is abandoned"):
        budget.record(TransactionOutcome.REINIT_PRODUCER, "producer fenced again")
