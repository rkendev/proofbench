"""The killable child: what it may do, and what it must not.

``scripts/run_phase.py`` exists as a separate process because two of the three fault
types deliver an uncatchable SIGKILL, and a harness that killed itself would take the
matrix with it. That separation is only worth anything if the child cannot undo the
parent's work, so the constraints are gated rather than described.

The one that matters most is provisioning. A restarted phase that deleted and recreated
the topics it was resuming into would destroy the durable state the ADR-0003 section 7
resume contract reads, and the run would report total loss for an apparatus reason
while looking exactly like a run that lost everything to the fault. Nothing downstream
could tell the difference.

Nothing here runs a phase. The offline checks are structural; the live SIGKILL is in
tests/integration/test_fault_injection.py, where a broker exists to be killed against.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

WORKER = Path(__file__).resolve().parents[2] / "scripts" / "run_phase.py"
TREE = ast.parse(WORKER.read_text(encoding="utf-8"))


def _imported_names() -> set[str]:
    """Every name the worker imports, at any level."""
    names: set[str] = set()
    for node in ast.walk(TREE):
        if isinstance(node, ast.ImportFrom | ast.Import):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def _called_names() -> set[str]:
    """Every bare function name the worker calls."""
    return {
        node.func.id
        for node in ast.walk(TREE)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


# --------------------------------------------------------------------------
# What the child must not do
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "forbidden",
    ["provision", "prepare_topics", "delete_consumer_groups", "delete"],
)
def test_the_worker_cannot_reach_provisioning(forbidden: str) -> None:
    """A resumed phase must never recreate the topics it is resuming into.

    Doing so would delete the input topic the ingest resume reads back and the
    committed offset the process resume depends on, so the phase would start from
    nothing and the run would report total loss. Indistinguishable, downstream, from a
    fault that lost everything.

    The parent provisions once per execution before the first attempt. Proven red by
    importing provision into the worker.
    """
    assert forbidden not in _imported_names(), (
        f"scripts/run_phase.py imports {forbidden}, so a restarted phase can destroy "
        f"the durable state the resume contract reads"
    )
    assert forbidden not in _called_names()


def test_the_worker_does_not_execute_a_whole_run() -> None:
    """One phase per invocation, or the kill lands in the wrong place.

    execute_run provisions, ingests, processes and verifies in one process. Calling it
    here would both re-provision and make the phase boundary meaningless, since the
    parent could no longer restart just the phase that died.
    """
    assert "execute_run" not in _imported_names()


def test_the_worker_writes_no_evidence_files() -> None:
    """Evidence is the parent's job, because the child may die mid-write.

    The child writes exactly one durable thing, the run state, through the atomic
    writer. Everything else waits until a process that is not going to be killed can
    write it.
    """
    assert "write_evidence" not in _imported_names()


# --------------------------------------------------------------------------
# What the child must do
# --------------------------------------------------------------------------


def test_the_worker_selects_an_injector_before_doing_any_work() -> None:
    """The arm-once guard lives in selection, so it has to run first.

    A run whose marker was lost has to be stopped before it produces half a stream,
    not after: the half-stream would be indistinguishable from a fault's own work.
    """
    main = next(
        node for node in ast.walk(TREE) if isinstance(node, ast.FunctionDef) and node.name == "main"
    )
    order = [
        node.func.id
        for node in ast.walk(main)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    assert "select_injector" in order
    for phase_call in ("ingest", "process"):
        if phase_call in order:
            assert order.index("select_injector") < order.index(phase_call), (
                f"{phase_call} runs before the injector is selected, so a run with a "
                f"lost marker would produce work before being stopped"
            )


def test_the_worker_persists_state_on_the_apparatus_failure_path() -> None:
    """A run abandoned in the child has to leave the parent something to read.

    Otherwise apparatus_failure is indistinguishable from a crash, and matrix-validity
    rule 2 requires every one of them to be diagnosed in writing.
    """
    saves = [
        node
        for node in ast.walk(TREE)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "save"
    ]
    assert len(saves) >= 3, "the state is not saved on every exit path"


def test_the_state_path_is_derived_rather_than_passed() -> None:
    """The parent and the child cannot be allowed to disagree about where it lives.

    A child writing its marker somewhere the parent does not read would start every
    attempt with a clean marker, re-fire the fault, and loop forever.
    """
    import inspect

    import run_phase

    parameters = set(inspect.signature(run_phase.state_directory).parameters)
    assert parameters == {"run_id", "configuration_name", "settings"}
    assert "--state-path" not in WORKER.read_text(encoding="utf-8")


def test_the_exit_codes_are_distinct_and_few() -> None:
    """They are the only channel back to the parent, so ambiguity is expensive."""
    import run_phase

    assert run_phase.EXIT_COMPLETED == 0
    assert run_phase.EXIT_REFUSED == 2
    assert run_phase.EXIT_COMPLETED != run_phase.EXIT_REFUSED


def test_the_worker_offers_exactly_the_two_killable_phases() -> None:
    """verify and diff are not here: nothing kills them, and they need no restart."""
    import run_phase

    assert set(run_phase.PHASES) == {"ingest", "process"}


def test_the_fault_window_closes_when_its_phase_finishes() -> None:
    """A delivery failure after the hosting phase completed is no longer the fault.

    The window opens when the injector fires and closes when the phase that hosted it
    has resolved the affected saga. Without the close, every later failure in the run
    would be absorbed as recovery, which is the too-loose direction of the boundary.
    """
    source = WORKER.read_text(encoding="utf-8")
    assert "fault_window_closed = True" in source
    assert "PHASE_OF_FAULT" in source, (
        "the window closes without checking which phase hosted the fault, so a fault "
        "in ingest would be closed by the process phase finishing"
    )
