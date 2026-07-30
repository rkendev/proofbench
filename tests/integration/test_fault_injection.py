"""A real SIGKILL, a real restart, and a real resume, against a live broker.

The first place the injector is not theoretical. Everything before this is structural:
the AST gates prove there is exactly one kill site and that the injector cannot see the
configuration, and the unit tests prove the arm-once guard fires. None of that shows
that a process actually dies at the seeded point, that the marker survives it, or that
the restarted phase declines to fire again.

Run through ``scripts/run_phase.py`` as a subprocess rather than in-process, because
that is what the matrix does and because a self-kill executed inside pytest would take
the test runner with it. The parent watches the exit status: ``-9`` means the child was
SIGKILLed, which on a kill run is the expected outcome of a successful injection rather
than an error.

Skipped with a named reason when no broker is reachable, so CI runs the whole offline
chain and boots nothing.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from proofbench.config import Settings, repo_root
from proofbench.core.configs import CONFIGURATION_NAMES, build_configuration
from proofbench.core.faults import FAULT_CONSUMER_SIGKILL, FAULT_PRODUCER_SIGKILL
from proofbench.core.run import load_schedule_entry, prepare_topics
from proofbench.core.state import OUTCOME_COMPLETED, OUTCOME_STARTED, RunState
from proofbench.core.topics import read_to_end_with_offsets

# Run 1 is producer_sigkill_mid_send at saga 96 step 2; run 2 is
# consumer_sigkill_between_sinks at saga 108 step 1. Both from the frozen schedule,
# named here rather than searched for so the test is about those entries.
PRODUCER_RUN = 1
CONSUMER_RUN = 2

SIGKILL_STATUS = -9


def _run_phase(
    run_id: int, configuration_name: str, phase: str
) -> subprocess.CompletedProcess[str]:
    """Execute one phase in a child process, exactly as the matrix runner will."""
    return subprocess.run(
        [
            sys.executable,
            str(repo_root() / "scripts" / "run_phase.py"),
            "--run-id",
            str(run_id),
            "--config",
            configuration_name,
            "--phase",
            phase,
        ],
        capture_output=True,
        text=True,
        cwd=repo_root(),
        timeout=300,
        check=False,
    )


def _state_path(run_id: int, configuration_name: str, settings: Settings) -> Path:
    return (
        repo_root()
        / settings.run_output_dir
        / f"run_{run_id:02d}"
        / configuration_name
        / "run_state.json"
    )


@pytest.fixture
def clean_execution(broker: str, settings: Settings):
    """Provision topics and clear any state, as the parent does once per execution."""

    def _prepare(run_id: int, configuration_name: str):
        configuration = build_configuration(configuration_name, run_id, settings)
        prepare_topics(configuration, settings)
        path = _state_path(run_id, configuration_name, settings)
        path.unlink(missing_ok=True)
        for stray in path.parent.glob("broker_fault_*.json"):
            stray.unlink()
        return configuration

    return _prepare


# --------------------------------------------------------------------------
# The kill actually happens
# --------------------------------------------------------------------------


@pytest.mark.parametrize("configuration_name", list(CONFIGURATION_NAMES))
def test_the_producer_fault_kills_the_ingest_phase(
    settings: Settings, clean_execution, configuration_name: str
) -> None:
    """A real SIGKILL at the seeded point, in both configurations.

    The exit status is the evidence: -9 is a process that was killed rather than one
    that returned. A phase that merely raised would exit 2, and a phase that finished
    would exit 0, so this distinguishes an injection from both.
    """
    clean_execution(PRODUCER_RUN, configuration_name)
    entry = load_schedule_entry(PRODUCER_RUN, settings)
    assert entry["fault_type"] == FAULT_PRODUCER_SIGKILL

    result = _run_phase(PRODUCER_RUN, configuration_name, "ingest")
    assert result.returncode == SIGKILL_STATUS, (
        f"the ingest phase exited {result.returncode} rather than being SIGKILLed. "
        f"stderr: {result.stderr[-400:]}"
    )


@pytest.mark.parametrize("configuration_name", list(CONFIGURATION_NAMES))
def test_the_consumer_fault_kills_the_process_phase(
    settings: Settings, clean_execution, configuration_name: str
) -> None:
    """The partial-write case: killed between sink A's flush and sink B."""
    clean_execution(CONSUMER_RUN, configuration_name)
    entry = load_schedule_entry(CONSUMER_RUN, settings)
    assert entry["fault_type"] == FAULT_CONSUMER_SIGKILL

    assert _run_phase(CONSUMER_RUN, configuration_name, "ingest").returncode == 0
    result = _run_phase(CONSUMER_RUN, configuration_name, "process")
    assert result.returncode == SIGKILL_STATUS, (
        f"the process phase exited {result.returncode} rather than being SIGKILLed. "
        f"stderr: {result.stderr[-400:]}"
    )


# --------------------------------------------------------------------------
# The marker survives the kill, which is what stops the loop
# --------------------------------------------------------------------------


@pytest.mark.parametrize("configuration_name", list(CONFIGURATION_NAMES))
def test_the_marker_is_durable_across_the_kill(
    settings: Settings, clean_execution, configuration_name: str
) -> None:
    """Written and fsynced before os.kill, so it exists after an uncatchable signal.

    This is the property the whole restart mechanism rests on. A marker written after
    the kill is a marker never written, and a restarted phase reading "not yet fired"
    fires again, and the run never ends.
    """
    clean_execution(PRODUCER_RUN, configuration_name)
    assert _run_phase(PRODUCER_RUN, configuration_name, "ingest").returncode == SIGKILL_STATUS

    state = RunState.load(_state_path(PRODUCER_RUN, configuration_name, settings))
    assert state is not None, "the marker did not survive the SIGKILL"
    assert state.fault_fired
    assert state.fault_fired_phase == "ingest"
    assert state.fault_fired_saga == 96


@pytest.mark.parametrize("configuration_name", list(CONFIGURATION_NAMES))
def test_the_restarted_phase_does_not_fire_again(
    settings: Settings, clean_execution, configuration_name: str
) -> None:
    """Arm-once, end to end. The second attempt completes rather than dying.

    Without this the run loops: killed, restarted, killed, restarted. The matrix would
    stall rather than fail, which is worse, because a stall produces no evidence at
    all.
    """
    clean_execution(PRODUCER_RUN, configuration_name)
    first = _run_phase(PRODUCER_RUN, configuration_name, "ingest")
    assert first.returncode == SIGKILL_STATUS

    second = _run_phase(PRODUCER_RUN, configuration_name, "ingest")
    assert second.returncode == 0, (
        f"the restarted phase exited {second.returncode}; the fault fired a second "
        f"time. stderr: {second.stderr[-400:]}"
    )

    state = RunState.load(_state_path(PRODUCER_RUN, configuration_name, settings))
    assert state is not None
    assert len(state.killed_attempts("ingest")) == 1, (
        "exactly one attempt must be recorded as killed: zero would mean nothing "
        "records a kill and the arm-once cross-check is unreachable, more than one "
        "would mean the fault fired again"
    )
    assert [a.outcome for a in state.attempts_for("ingest")] == [
        OUTCOME_STARTED,
        OUTCOME_COMPLETED,
    ], "the killed attempt and the successful restart are not both recorded"


# --------------------------------------------------------------------------
# The resume is correct, which is what the seven producer runs rest on
# --------------------------------------------------------------------------


@pytest.mark.parametrize("configuration_name", list(CONFIGURATION_NAMES))
def test_the_resumed_ingest_produces_every_saga(
    settings: Settings, clean_execution, configuration_name: str
) -> None:
    """After a real kill and a real restart, nothing is missing from the input topic.

    This is the property that makes producer_sigkill_mid_send structurally incapable of
    losing a side effect, which is in turn the arithmetic behind ADR-0004's prediction
    that C2 cannot reach its floor. Measured here against a live broker after an
    uncatchable signal, rather than argued.
    """
    configuration = clean_execution(PRODUCER_RUN, configuration_name)
    assert _run_phase(PRODUCER_RUN, configuration_name, "ingest").returncode == SIGKILL_STATUS
    assert _run_phase(PRODUCER_RUN, configuration_name, "ingest").returncode == 0

    records = read_to_end_with_offsets(dict(configuration.verifier), configuration.topics.input)
    seen: dict[int, set[int]] = {}
    for _, value in records:
        payload = json.loads(value.decode("utf-8"))
        seen.setdefault(int(payload["saga_index"]), set()).add(int(payload["step_index"]))

    assert set(seen) == set(range(settings.sagas_per_run)), "the resume skipped a saga"
    assert all(len(steps) == settings.steps_per_saga for steps in seen.values())


def test_the_good_configuration_loses_and_duplicates_nothing_across_a_real_kill(
    settings: Settings, clean_execution
) -> None:
    """C1's mechanism, observed once before the matrix asks it 20 times.

    The killed producer's partial saga is inside an aborted transaction, so
    read_committed never shows it, and init_transactions on the restart fences the dead
    epoch. The visible input topic therefore holds exactly 600 records, once each.
    """
    configuration = clean_execution(PRODUCER_RUN, "good")
    assert _run_phase(PRODUCER_RUN, "good", "ingest").returncode == SIGKILL_STATUS
    assert _run_phase(PRODUCER_RUN, "good", "ingest").returncode == 0

    records = read_to_end_with_offsets(dict(configuration.verifier), configuration.topics.input)
    keys = [str(json.loads(value.decode("utf-8"))["idempotency_key"]) for _, value in records]

    assert len(keys) == settings.sagas_per_run * settings.steps_per_saga
    assert len(keys) == len(set(keys)), "the good configuration duplicated across the resume"


def test_the_baseline_duplicates_the_partial_saga_across_a_real_kill(
    settings: Settings, clean_execution
) -> None:
    """The mirror image, and the reason the two configurations diverge here.

    The baseline's partial send is non-transactional and durable, so whatever landed
    before the kill is still visible and the resume re-sends the whole saga. Bounded at
    M-1 records, because all M visible would mean the saga was complete and the resume
    would have skipped it.

    The exact count depends on how much of the saga reached the broker before the kill,
    which is genuinely racy, so the assertion is the bound rather than a number.
    """
    configuration = clean_execution(PRODUCER_RUN, "baseline")
    assert _run_phase(PRODUCER_RUN, "baseline", "ingest").returncode == SIGKILL_STATUS
    assert _run_phase(PRODUCER_RUN, "baseline", "ingest").returncode == 0

    records = read_to_end_with_offsets(dict(configuration.verifier), configuration.topics.input)
    counts: dict[str, int] = {}
    for _, value in records:
        key = str(json.loads(value.decode("utf-8"))["idempotency_key"])
        counts[key] = counts.get(key, 0) + 1

    duplicated = sum(count - 1 for count in counts.values() if count > 1)
    assert duplicated <= settings.steps_per_saga - 1, (
        f"the baseline duplicated {duplicated} records, past the one-saga bound "
        f"ADR-0003 section 7 buys"
    )
    # And nothing is missing either way, which is the half that matters for C2:
    # the ingest phase duplicates under the baseline but cannot lose under either.
    assert len(counts) == settings.sagas_per_run * settings.steps_per_saga, (
        "a key is missing from the input topic, so the ingest resume lost a side "
        "effect, which the resume rule makes structurally impossible"
    )


def test_a_control_run_is_never_killed(settings: Settings, clean_execution) -> None:
    """The apparatus check cannot be injured, verified by running it through the child.

    The same entry point that kills a kill run has to complete a control run untouched,
    or every later row of the matrix rests on evidence the injector could have
    corrupted.
    """
    clean_execution(settings.control_run_id, "good")
    for phase in ("ingest", "process"):
        result = _run_phase(settings.control_run_id, "good", phase)
        assert result.returncode == 0, (
            f"the control run's {phase} phase exited {result.returncode}. "
            f"stderr: {result.stderr[-400:]}"
        )

    state = RunState.load(_state_path(settings.control_run_id, "good", settings))
    assert state is not None
    assert not state.fault_fired, "a fault fired on the no-fault control run"


def test_the_arm_once_guard_is_reachable_end_to_end(settings: Settings, clean_execution) -> None:
    """Disarm the marker after a real kill and assert the run stops rather than loops.

    The T-prompt's named red-proof, run against a live broker rather than a stub. The
    kill is real, the marker is real, and the marker is then tampered with the way a
    lost or truncated file would leave it: fault_fired back to false, while the attempt
    history still records that this phase died.

    Both facts cannot be true. Arming again would fire a second time, the phase would
    die again, and the run would never finish, so the guard stops it. Before the
    started-record fix this test could not have passed at all: nothing marked a killed
    attempt, so the cross-check had nothing to compare against and the guard, though
    correct, was unreachable.
    """
    clean_execution(PRODUCER_RUN, "good")
    assert _run_phase(PRODUCER_RUN, "good", "ingest").returncode == SIGKILL_STATUS

    path = _state_path(PRODUCER_RUN, "good", settings)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["fault"]["fired"] is True
    payload["fault"]["fired"] = False  # the seeded violation: the marker is disarmed
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = _run_phase(PRODUCER_RUN, "good", "ingest")
    assert result.returncode == 2, (
        f"the run continued after the marker was disarmed (exit {result.returncode}); "
        f"it would have injected a second time and looped"
    )
    assert "marker was lost or disarmed" in result.stderr

    stopped = RunState.load(path)
    assert stopped is not None and stopped.fault_fired_twice, (
        "the double-fire condition was not recorded, so a later reader of the evidence "
        "cannot tell why the run was abandoned"
    )
