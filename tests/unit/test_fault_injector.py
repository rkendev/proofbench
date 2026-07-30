"""The injector fires once, at the seeded point, identically in both configurations.

Three properties, each of which would corrupt the matrix in a different way if it
failed.

**Inert by default.** A live injector must not exist where firing would be wrong. The
control run at run_id 0 is the apparatus check the whole project rests on, and a fault
in it would not merely produce a wrong number, it would invalidate the evidence that
the harness reads zero when nothing was killed.

**Armed once.** The seeded fault fires exactly one time. A second firing kills the
restarted phase, which restarts, which fires again: the run never ends and the matrix
stalls rather than failing. The guard is a cross-check between two independent durable
facts rather than a flag, because a flag cannot detect its own loss.

**Configuration-blind.** INV-P3 at the layer the allow-list gate cannot see. That gate
compares settings dictionaries; an injector that fired at a different point, or not at
all, under one configuration would satisfy every INV-P3 rule while turning the matrix
into a comparison of two different experiments.

Nothing here kills anything. The one function that can is exercised only through an AST
walk, because a test that actually called it would take the test runner with it.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from typing import Any

import pytest

from proofbench.config import Settings
from proofbench.core.faults import (
    FAULT_BROKER_STOP_START,
    FAULT_CONSUMER_SIGKILL,
    FAULT_PRODUCER_SIGKILL,
    PHASE_OF_FAULT,
    FaultInjectingWriter,
    NoFault,
    SagaFaultInjector,
    SeededFault,
    select_injector,
)
from proofbench.core.recovery import ApparatusFailure
from proofbench.core.state import OUTCOME_COMPLETED, OUTCOME_KILLED, Attempt, RunState

PACKAGE_DIR = Path(__file__).resolve().parents[2] / "src" / "proofbench"
FAULTS_MODULE = PACKAGE_DIR / "core" / "faults.py"

FAULT_SAGA = 108
FAULT_STEP = 1


def _entry(fault_type: str = FAULT_CONSUMER_SIGKILL, control: bool = False) -> dict[str, Any]:
    if control:
        return {"run_id": 0, "fault_type": "none", "fault_point": None, "control": True}
    return {
        "run_id": 2,
        "fault_type": fault_type,
        "fault_point": {"saga_index": FAULT_SAGA, "step_index": FAULT_STEP},
        "control": False,
    }


def _state(fired: bool = False) -> RunState:
    return RunState(run_id=2, configuration="good", entry_names_a_fault=True, fault_fired=fired)


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(_env_file=None)


class RecordingInjector:
    """Records the seams it was told about, so the wiring can be checked offline."""

    def __init__(self) -> None:
        self.events: list[tuple[str, int]] = []

    def saga_started(self, saga_index: int) -> None:
        self.events.append(("saga_started", saga_index))

    def step_produced(self, saga_index: int, step_index: int) -> None:
        self.events.append(("step_produced", step_index))

    def sink_flushed(self, ordinal: int) -> None:
        self.events.append(("sink_flushed", ordinal))


class RecordingWriter:
    """The inner writer, same shape as the sink-ordering test's."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self.calls.append(("produce", topic))

    def flush(self) -> None:
        self.calls.append(("flush", ""))


# --------------------------------------------------------------------------
# Inert by default
# --------------------------------------------------------------------------


def test_a_control_run_gets_no_injector(settings: Settings, tmp_path: Path) -> None:
    """The apparatus check the whole project rests on cannot be injured.

    A fault in run_id 0 would not merely produce a wrong number: it would destroy the
    evidence that the harness reports zero when nothing was killed, which is the
    precondition for trusting every other row of the matrix.
    """
    injector = select_injector(
        _entry(control=True), _state(), tmp_path / "state.json", "process", settings
    )
    assert isinstance(injector, NoFault)


def test_a_control_entry_cannot_construct_a_live_injector_even_directly(
    settings: Settings, tmp_path: Path
) -> None:
    """Belt and braces, because the consequence is losing the control run."""
    with pytest.raises(ApparatusFailure, match="no fault point"):
        SeededFault(_entry(control=True), _state(), tmp_path / "state.json", settings)


def test_a_fault_belonging_to_another_phase_is_inert(settings: Settings, tmp_path: Path) -> None:
    """The producer fault lives in ingest, so the process phase must not arm it."""
    injector = select_injector(
        _entry(FAULT_PRODUCER_SIGKILL), _state(), tmp_path / "state.json", "process", settings
    )
    assert isinstance(injector, NoFault)

    live = select_injector(
        _entry(FAULT_PRODUCER_SIGKILL), _state(), tmp_path / "state.json", "ingest", settings
    )
    assert isinstance(live, SeededFault)


def test_every_fault_type_belongs_to_exactly_one_phase() -> None:
    """Otherwise "the fault fires once" would need a per-phase answer."""
    assert set(PHASE_OF_FAULT) == {
        FAULT_PRODUCER_SIGKILL,
        FAULT_CONSUMER_SIGKILL,
        FAULT_BROKER_STOP_START,
    }
    assert PHASE_OF_FAULT[FAULT_PRODUCER_SIGKILL] == "ingest"
    assert PHASE_OF_FAULT[FAULT_CONSUMER_SIGKILL] == "process"
    assert PHASE_OF_FAULT[FAULT_BROKER_STOP_START] == "process"


def test_the_fault_menu_matches_the_frozen_schedule() -> None:
    """The injector's menu is CLAIMS.md's, not a second list that could drift."""
    import json

    from proofbench.config import repo_root

    settings = Settings(_env_file=None)
    payload = json.loads((repo_root() / settings.schedule_path).read_text(encoding="utf-8"))
    assert set(payload["constants"]["fault_menu"]) == set(PHASE_OF_FAULT)


def test_the_inert_injector_does_nothing_at_every_seam() -> None:
    """It is what 34 of the 42 executions run with, so it has to be a true no-op."""
    inert = NoFault()
    assert inert.saga_started(5) is None
    assert inert.step_produced(5, 1) is None
    assert inert.sink_flushed(0) is None


# --------------------------------------------------------------------------
# Armed once
# --------------------------------------------------------------------------


def test_a_fault_already_fired_is_not_armed_again(settings: Settings, tmp_path: Path) -> None:
    """The restarted phase runs inert, which is what lets the run finish."""
    state = _state(fired=True)
    state.record_attempt(Attempt("process", 1, OUTCOME_KILLED))
    injector = select_injector(_entry(), state, tmp_path / "state.json", "process", settings)
    assert isinstance(injector, NoFault)


def test_a_disarmed_marker_stops_the_run_rather_than_letting_it_loop(
    settings: Settings, tmp_path: Path
) -> None:
    """The arm-once guard, driven by its named seeded violation.

    The T-prompt's red-proof: disarm the marker and assert the run is stopped rather
    than allowed to loop. A flag cannot detect its own loss, so the guard cross-checks
    two independent durable facts. The attempt history says this phase was already
    killed; the marker says the fault never fired. Both cannot be true. Arming again
    would fire a second time, the phase would be killed again, and the run would never
    finish, so it is stopped and recorded as an apparatus failure.

    A fault that fires twice is not the fault the frozen schedule describes, and a run
    containing it has no claim to report.
    """
    state = _state(fired=False)  # the marker, disarmed
    state.record_attempt(Attempt("process", 1, OUTCOME_KILLED))  # but a kill happened

    path = tmp_path / "state.json"
    with pytest.raises(ApparatusFailure, match="marker was lost or disarmed"):
        select_injector(_entry(), state, path, "process", settings)

    assert state.fault_fired_twice
    reloaded = RunState.load(path)
    assert reloaded is not None and reloaded.fault_fired_twice, (
        "the double-fire condition must be durable, or a later reader of the evidence "
        "cannot tell why the run was abandoned"
    )


def test_a_completed_attempt_does_not_trip_the_guard(settings: Settings, tmp_path: Path) -> None:
    """Only a kill without a marker is contradictory.

    A phase that completed and is being re-run for some other reason has not been
    killed, so there is nothing to cross-check and the fault is armed normally.
    """
    state = _state(fired=False)
    state.record_attempt(Attempt("ingest", 1, OUTCOME_COMPLETED))
    injector = select_injector(_entry(), state, tmp_path / "state.json", "process", settings)
    assert isinstance(injector, SeededFault)


def test_firing_records_the_marker_before_it_inflicts_anything(settings: Settings) -> None:
    """The ordering that stops the run looping, read out of the source.

    os.kill with SIGKILL cannot be caught, deferred, or wrapped in a finally, so
    anything not on disk before the call never happens. A marker written afterwards is
    a marker never written, and a restarted phase that reads "not yet fired" fires
    again. Checked structurally because the alternative is a test that dies.
    """
    source = inspect.getsource(SeededFault._fire)
    save_at = source.index("self._state.save(")
    kill_at = source.index("_sigkill_self()")
    outage_at = source.index("self._await_outage()")
    assert save_at < kill_at, "the marker is saved after the kill, so it is never saved"
    assert save_at < outage_at, "the marker is saved after the outage begins"


def test_the_marker_records_where_the_fault_fired(settings: Settings, tmp_path: Path) -> None:
    """ "aborted: 1" is uninterpretable without knowing which phase it happened in.

    The same is true of the fault itself, so the marker carries the phase and the saga
    rather than only a boolean.
    """
    state = _state()
    path = tmp_path / "state.json"
    fault = SeededFault(_entry(FAULT_BROKER_STOP_START), state, path, settings)
    # Write the resume token first, so the rendezvous returns immediately and nothing
    # blocks: this exercises the recording, not the outage.
    from proofbench.core.faults import RESUME_TOKEN_FILE

    (tmp_path / RESUME_TOKEN_FILE).write_text("{}", encoding="utf-8")

    fault.saga_started(FAULT_SAGA)
    fault.sink_flushed(0)

    reloaded = RunState.load(path)
    assert reloaded is not None
    assert reloaded.fault_fired
    assert reloaded.fault_fired_phase == "process"
    assert reloaded.fault_fired_saga == FAULT_SAGA


# --------------------------------------------------------------------------
# It fires at the seeded point and nowhere else
# --------------------------------------------------------------------------


def test_the_producer_fault_ignores_every_step_but_the_seeded_one(
    settings: Settings, tmp_path: Path
) -> None:
    """A fault point is a position, so every other position must be a no-op."""
    state = _state()
    fault = SeededFault(_entry(FAULT_PRODUCER_SIGKILL), state, tmp_path / "state.json", settings)

    fault.step_produced(FAULT_SAGA - 1, FAULT_STEP)
    fault.step_produced(FAULT_SAGA, FAULT_STEP + 1)
    fault.step_produced(FAULT_SAGA + 1, 0)
    assert not state.fault_fired, "the fault fired at a position the schedule did not name"


def test_the_consumer_fault_fires_after_sink_a_and_never_after_sink_b(
    settings: Settings, tmp_path: Path
) -> None:
    """Ordinal 0 is sink A's flush. Firing after B would be the mirror-image fault.

    CLAIMS.md names the partial-write case: A present, B absent. With the order or the
    ordinal reversed, the fault would leave both present and demonstrate nothing.
    """
    state = _state()
    fault = SeededFault(_entry(FAULT_CONSUMER_SIGKILL), state, tmp_path / "state.json", settings)

    fault.saga_started(FAULT_SAGA)
    fault.sink_flushed(1)  # sink B, wrong ordinal
    assert not state.fault_fired

    fault.saga_started(FAULT_SAGA - 1)
    fault.sink_flushed(0)  # right ordinal, wrong saga
    assert not state.fault_fired


def test_the_writer_decorator_leaves_the_frozen_ordering_untouched() -> None:
    """write_saga_to_sinks and its test are not modified, so the guard still guards.

    ADR-0003 section 4: the ordering is pinned by a unit test because the control run
    cannot see it. Decorating the writer rather than editing the function keeps that
    pin exactly as it was, and this asserts the decorator is transparent.
    """
    from proofbench.core.run import write_saga_to_sinks

    inner = RecordingWriter()
    recorder = RecordingInjector()
    writer = FaultInjectingWriter(inner, recorder)
    records = [("k0", b"0"), ("k1", b"1")]

    writer.saga_started(FAULT_SAGA)
    write_saga_to_sinks(writer, ("topic.a", "topic.b"), records)

    assert inner.calls == [
        ("produce", "topic.a"),
        ("produce", "topic.a"),
        ("flush", ""),
        ("produce", "topic.b"),
        ("produce", "topic.b"),
        ("flush", ""),
    ]
    assert recorder.events == [
        ("saga_started", FAULT_SAGA),
        ("sink_flushed", 0),
        ("sink_flushed", 1),
    ]


def test_the_flush_counter_resets_per_saga() -> None:
    """Otherwise the second saga's sink A would be counted as somebody's sink B."""
    writer = FaultInjectingWriter(RecordingWriter(), (recorder := RecordingInjector()))
    for saga in (10, 11):
        writer.saga_started(saga)
        writer.flush()
        writer.flush()
    assert [event for event in recorder.events if event[0] == "sink_flushed"] == [
        ("sink_flushed", 0),
        ("sink_flushed", 1),
        ("sink_flushed", 0),
        ("sink_flushed", 1),
    ]


def test_the_injector_fires_after_the_inner_flush_not_before() -> None:
    """Sink A has to be genuinely durable when the kill lands.

    A flush before the kill is what makes the partial write real rather than nominal:
    without it both sets of records would sit in one producer queue and reach the
    broker together, and the kill could not leave A present and B absent at all.
    """
    order: list[str] = []

    class Ordered:
        def produce(self, topic: str, key: str, value: bytes) -> None:
            order.append("produce")

        def flush(self) -> None:
            order.append("inner flush")

    class Watcher:
        def saga_started(self, saga_index: int) -> None:
            return None

        def step_produced(self, saga_index: int, step_index: int) -> None:
            return None

        def sink_flushed(self, ordinal: int) -> None:
            order.append("injector")

    FaultInjectingWriter(Ordered(), Watcher()).flush()
    assert order == ["inner flush", "injector"]


# --------------------------------------------------------------------------
# INV-P3: the injector never sees the configuration
# --------------------------------------------------------------------------


def test_the_injector_module_cannot_reach_the_configuration() -> None:
    """INV-P3 at the layer the allow-list gate cannot see.

    That gate compares settings dictionaries. An injector that fired at a different
    point, or not at all, under one configuration would satisfy every one of its rules
    while making the matrix a comparison of two different experiments, and C2's floor
    would measure the rigging.

    Proven red by making the injector behave differently for `good`.
    """
    tree = ast.parse(FAULTS_MODULE.read_text(encoding="utf-8"))

    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    assert "proofbench.core.configs" not in imported, (
        "the injector imports the configurations module, so it can see which "
        "configuration is under test"
    )

    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Module)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    named = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and node.value in ("good", "baseline")
    }
    assert not named, f"the injector carries the configuration name(s) {sorted(named)} in code"

    reads = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id in ("configuration", "config")
    }
    assert not reads, f"the injector reads configuration attribute(s) {sorted(reads)}"


def test_the_injector_signature_takes_no_configuration() -> None:
    """It cannot branch on what it is never given."""
    for function in (select_injector, SeededFault.__init__):
        parameters = set(inspect.signature(function).parameters)
        assert "configuration" not in parameters, (
            f"{function.__qualname__} accepts a configuration, so the injector can "
            f"differ between the two"
        )


# --------------------------------------------------------------------------
# There is exactly one place that can kill this process
# --------------------------------------------------------------------------


def _kill_calls(path: Path) -> list[ast.Call]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "kill"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "os"
    ]


def test_os_kill_appears_in_exactly_one_module() -> None:
    """A second kill site would be a fault nobody scheduled.

    The seeded fault is the experiment. A kill anywhere else would be an unscheduled
    fault landing in a run the matrix reports as something else, which is worse than a
    wrong number because the evidence would not know it happened.
    """
    offenders = [
        str(path.relative_to(PACKAGE_DIR.parents[1]))
        for path in sorted(PACKAGE_DIR.rglob("*.py"))
        if path.resolve() != FAULTS_MODULE.resolve() and _kill_calls(path)
    ]
    assert not offenders, f"os.kill outside core/faults.py: {offenders}"


def test_the_only_kill_is_this_process_with_sigkill() -> None:
    """Signal and target are both pinned, and both matter.

    SIGKILL rather than SIGTERM because it cannot be caught or handled: no finally
    runs, no atexit hook fires, no client flushes on the way out, which is what makes
    the kill a crash rather than a shutdown. os.getpid() rather than any other pid
    because a harness that could signal another process is a harness that could take
    down the broker container, or the supervisor, by a typo.
    """
    calls = _kill_calls(FAULTS_MODULE)
    assert len(calls) == 1, f"expected exactly one os.kill, found {len(calls)}"
    target, sig = calls[0].args

    assert isinstance(target, ast.Call)
    assert isinstance(target.func, ast.Attribute)
    assert target.func.attr == "getpid"
    assert isinstance(target.func.value, ast.Name) and target.func.value.id == "os"

    assert isinstance(sig, ast.Attribute)
    assert sig.attr == "SIGKILL"
    assert isinstance(sig.value, ast.Name) and sig.value.id == "signal"


def test_the_walk_detects_a_smuggled_kill() -> None:
    """The rule above passes by absence, so the mechanism is pinned."""
    tree = ast.parse("import os, signal\nos.kill(1234, signal.SIGTERM)\n")
    found = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "kill"
    ]
    assert len(found) == 1


def test_the_protocol_is_what_the_run_path_depends_on() -> None:
    """Both implementations satisfy it, so the run path cannot tell them apart.

    That is the property that keeps the injected and non-injected paths the same
    code: no method returns a value, so no caller can branch on what the injector did.
    """
    for implementation in (NoFault(), RecordingInjector()):
        assert isinstance(implementation, SagaFaultInjector)
    for name in ("saga_started", "step_produced", "sink_flushed"):
        assert inspect.signature(getattr(NoFault, name)).return_annotation == "None"
