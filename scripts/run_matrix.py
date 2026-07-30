#!/usr/bin/env python3
"""Execute all 42 matrix executions, strictly sequentially, and write the evidence.

The supervising parent. It provisions, spawns each phase as a killable child, restarts
what the injector killed, takes the broker away when a child asks, assembles the
matrix, and refuses to emit one that breaks a pre-registered validity rule.

    make run-matrix

**Never in parallel, and this is not a performance decision.** ``broker_stop_start``
takes down the shared broker. Two concurrent executions would mean one run's injected
outage landing in the middle of another run's ordinary work, which would record a fault
nobody scheduled against a run the matrix reports as something else. The evidence would
not know it had happened. There is one broker, so there is one execution at a time.

Progress is printed unbuffered as ``[N/42]`` because the whole thing takes half an hour
and a silent half hour is indistinguishable from a hang.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from proofbench.config import Settings, get_settings, repo_root
from proofbench.core.configs import CONFIGURATION_NAMES, build_configuration
from proofbench.core.evidence import write_json
from proofbench.core.faults import (
    FAULT_BROKER_STOP_START,
    PHASE_OF_FAULT,
    RENDEZVOUS_FILE,
    RESUME_TOKEN_FILE,
)
from proofbench.core.ledger_diff import KeyedLedgerDiffer
from proofbench.core.matrix import (
    STATUS_APPARATUS_FAILURE,
    STATUS_CLEAN,
    STATUS_NOT_CLEAN,
    Execution,
    Matrix,
    MatrixVoid,
    loss_structurally_possible,
)
from proofbench.core.run import (
    assert_losses_are_attributable,
    load_schedule_entry,
    prepare_topics,
    verify,
    write_evidence,
)
from proofbench.core.saga import expand_sagas, expected_ledger
from proofbench.core.state import MAX_ATTEMPTS_PER_PHASE, PHASE_INGEST, PHASE_PROCESS, RunState
from proofbench.core.trace import load_trace

EXIT_OK = 0
EXIT_VOID = 3

SIGKILL_STATUS = -9
STATE_FILE = "run_state.json"

# Every compose call is pinned to this project name, as the Makefile is. An unscoped
# compose command reaches whatever compose decides the project is, and this box runs
# twenty-odd unrelated containers.
COMPOSE = ("docker", "compose", "-p", "proofbench")


def _say(message: str) -> None:
    """Unbuffered progress. A silent half hour is indistinguishable from a hang."""
    print(message, flush=True)


def _run_phase(run_id: int, configuration: str, phase: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [
            sys.executable,
            str(repo_root() / "scripts" / "run_phase.py"),
            "--run-id",
            str(run_id),
            "--config",
            configuration,
            "--phase",
            phase,
        ],
        cwd=repo_root(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _compose(*args: str) -> None:
    subprocess.run([*COMPOSE, *args], cwd=repo_root(), check=True, capture_output=True)


def _await_broker(settings: Settings, deadline_s: float = 120.0) -> None:
    """Block until the broker answers, so the next phase does not race the restart."""
    from proofbench.core.topics import TopicProvisioningError, _admin

    bootstrap = settings.broker_bootstrap_servers
    assert bootstrap
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        try:
            _admin(bootstrap).list_topics(timeout=5.0)
            return
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
    raise TopicProvisioningError(
        f"the broker did not answer within {deadline_s:.0f}s of being restarted"
    )


def _supervise_outage(directory: Path, settings: Settings) -> None:
    """Take the broker away while the child waits at the fault point, then bring it back.

    The child blocks on the resume token, so the outage is guaranteed to contain the
    failing call rather than racing it. Stopping the broker before writing the token is
    what makes that true: released first, the child could produce, flush and commit
    before compose had finished, and the run would be labelled broker_stop_start with no
    outage in it.
    """
    _say("        broker: stopping")
    _compose("stop", "kafka")
    write_json(directory / RESUME_TOKEN_FILE, {"released": True})

    outage_s = settings.broker_outage_ms / 1000.0
    _say(f"        broker: down for {outage_s:.0f}s")
    time.sleep(outage_s)

    _say("        broker: starting")
    _compose("start", "kafka")
    _await_broker(settings)
    _say("        broker: answering again")


def _drive_phase(
    run_id: int, configuration: str, phase: str, directory: Path, settings: Settings
) -> str:
    """Run one phase to completion, restarting it as many times as the fault requires.

    Returns the phase's final outcome. A SIGKILLed child is not an error here: on a kill
    run it is the expected result of a successful injection, and the restart is the
    resume contract being exercised rather than a retry.
    """
    entry = load_schedule_entry(run_id, settings)
    is_broker_fault = (
        str(entry["fault_type"]) == FAULT_BROKER_STOP_START
        and PHASE_OF_FAULT.get(str(entry["fault_type"])) == phase
    )
    rendezvous = directory / RENDEZVOUS_FILE

    for attempt in range(1, MAX_ATTEMPTS_PER_PHASE + 1):
        child = _run_phase(run_id, configuration, phase)

        if is_broker_fault:
            # Watch for the child reaching the fault point. It blocks there, so this is
            # a rendezvous rather than a race.
            while child.poll() is None:
                if rendezvous.exists():
                    _supervise_outage(directory, settings)
                    break
                time.sleep(0.1)

        child.wait()
        if child.returncode == 0:
            return "completed"
        if child.returncode == SIGKILL_STATUS:
            _say(f"        {phase}: killed on attempt {attempt}, restarting")
            continue
        stderr = (child.stderr.read().decode("utf-8", "replace") if child.stderr else "").strip()
        return f"apparatus_failure: {stderr.splitlines()[-1] if stderr else 'no diagnosis'}"

    return (
        f"apparatus_failure: the {phase} phase was killed on every one of "
        f"{MAX_ATTEMPTS_PER_PHASE} attempts, which the seeded fault cannot explain"
    )


def _execute(run_id: int, configuration: str, settings: Settings, ordinal: int) -> Execution:
    """One execution: provision, drive both phases, verify, diff, write evidence."""
    entry = load_schedule_entry(run_id, settings)
    _say(f"[{ordinal}/42] run {run_id:02d} {configuration:<8} {entry['fault_type']}")

    configuration_obj = build_configuration(configuration, run_id, settings)
    directory = repo_root() / settings.run_output_dir / f"run_{run_id:02d}" / configuration
    directory.mkdir(parents=True, exist_ok=True)

    # Once per execution, in the parent, before the first attempt. Never between
    # attempts: that would destroy the durable state the resume contract reads.
    for stale in (STATE_FILE, RENDEZVOUS_FILE, RESUME_TOKEN_FILE):
        (directory / stale).unlink(missing_ok=True)
    prepare_topics(configuration_obj, settings)

    started = time.monotonic()
    diagnosis = ""
    for phase in (PHASE_INGEST, PHASE_PROCESS):
        outcome = _drive_phase(run_id, configuration, phase, directory, settings)
        if outcome != "completed":
            diagnosis = f"{phase} phase: {outcome}"
            break

    state = RunState.load(directory / STATE_FILE)
    elapsed = time.monotonic() - started

    if diagnosis:
        _say(f"        apparatus failure in {elapsed:.0f}s: {diagnosis}")
        return _failed_execution(run_id, configuration, entry, state, diagnosis)

    # Verify and diff in the parent, which is not going to be killed.
    trace = load_trace(repo_root() / settings.trace_path)
    sagas = expand_sagas(str(entry["seed"]), settings, trace)
    expected = expected_ledger(sagas)
    differ = KeyedLedgerDiffer()

    from proofbench.core.run import RunResult, SinkOutcome

    sinks = []
    try:
        for name, topic in (
            ("sink_a", configuration_obj.topics.sink_a),
            ("sink_b", configuration_obj.topics.sink_b),
        ):
            observed = verify(configuration_obj, topic, int(entry["steps_per_saga"]))
            sinks.append(
                SinkOutcome(
                    name=name,
                    topic=topic,
                    records_sent=len(observed),
                    records_visible=len(observed),
                    diff=differ.diff(expected, observed),
                    observed=tuple(observed),
                )
            )
        assert state is not None
        assert_losses_are_attributable(
            configuration_obj, sinks, state.offset_gaps(), settings.steps_per_saga
        )
    except Exception as exc:  # noqa: BLE001
        diagnosis = f"verification: {type(exc).__name__}: {exc}"
        _say(f"        apparatus failure in {elapsed:.0f}s: {diagnosis}")
        return _failed_execution(run_id, configuration, entry, state, diagnosis)

    status = STATUS_CLEAN if all(s.is_clean for s in sinks) else STATUS_NOT_CLEAN
    assert state is not None
    result = RunResult(
        run_id=run_id,
        configuration=configuration_obj,
        schedule_entry=entry,
        expected=expected,
        sinks=tuple(sinks),
        budget=state.budget,
        transactions=state.transactions,
        process_stats={},
        status=status,
    )
    write_evidence(result, settings)

    duplicated = sum(len(s.diff.duplicated) for s in sinks)
    lost = sum(len(s.diff.lost) for s in sinks)
    _say(
        f"        {status} in {elapsed:.0f}s: "
        f"duplicated={duplicated} lost={lost} "
        f"txn committed={state.transactions.committed} aborted={state.transactions.aborted}"
    )

    return Execution(
        run_id=run_id,
        configuration=configuration,
        fault_type=str(entry["fault_type"]),
        is_control=bool(entry["control"]),
        status=status,
        duplicated=duplicated,
        lost=lost,
        loss_possible=loss_structurally_possible(entry),
        transactions_committed=state.transactions.committed,
        transactions_aborted=state.transactions.aborted,
        max_open_transaction_ms=round(state.transactions.max_open_ms, 3),
        recovery=state.budget.to_jsonable(),
    )


def _failed_execution(
    run_id: int, configuration: str, entry: dict[str, Any], state: RunState | None, diagnosis: str
) -> Execution:
    return Execution(
        run_id=run_id,
        configuration=configuration,
        fault_type=str(entry["fault_type"]),
        is_control=bool(entry["control"]),
        status=STATUS_APPARATUS_FAILURE,
        duplicated=0,
        lost=0,
        loss_possible=loss_structurally_possible(entry),
        transactions_committed=state.transactions.committed if state else 0,
        transactions_aborted=state.transactions.aborted if state else 0,
        max_open_transaction_ms=round(state.transactions.max_open_ms, 3) if state else 0.0,
        recovery=state.budget.to_jsonable() if state else {},
        diagnosis=diagnosis,
    )


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cycle",
        type=int,
        default=1,
        help="Which void-and-rerun cycle this is. Recorded into the evidence.",
    )
    args = parser.parse_args(argv)

    payload = json.loads((repo_root() / settings.schedule_path).read_text(encoding="utf-8"))
    entries = payload["runs"]
    total = len(entries) * len(CONFIGURATION_NAMES)

    _say(f"proofbench matrix: {total} executions, strictly sequential, cycle {args.cycle}")
    _say("never in parallel: broker_stop_start takes down the shared broker\n")

    started = time.monotonic()
    matrix = Matrix(expected_executions=total, cycle=args.cycle)
    ordinal = 0
    for entry in entries:
        for configuration in CONFIGURATION_NAMES:
            ordinal += 1
            matrix.executions.append(
                _execute(int(entry["run_id"]), configuration, settings, ordinal)
            )

    elapsed = time.monotonic() - started
    _say(f"\nall {ordinal} executions attempted in {elapsed / 60:.1f} minutes")

    out = repo_root() / settings.run_output_dir / "matrix.json"
    write_json(out, {**matrix.to_jsonable(), "wall_clock_seconds": round(elapsed, 1)})
    _say(f"matrix written to {out}")

    try:
        matrix.assert_shippable()
    except MatrixVoid as void:
        _say(f"\nMATRIX VOID: {void}")
        return EXIT_VOID

    _say("\nthe matrix satisfies every pre-registered validity rule")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
