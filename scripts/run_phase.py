#!/usr/bin/env python3
"""Execute ONE phase of one run under one configuration, in a process that may die.

This is the killable child. The matrix runner is the parent, and it survives, which
is the whole reason this file exists as a separate entry point: two of the three fault
types deliver an uncatchable SIGKILL, and a harness that killed itself would take the
matrix with it.

    python scripts/run_phase.py --run-id 3 --config good --phase ingest

Everything durable belongs to the parent's directory and is written atomically here,
because this process can vanish between any two instructions. The marker that records
whether the seeded fault has fired is saved before the kill, never after.

**What this script deliberately cannot do.** It does not provision topics and it does
not delete consumer groups. A restarted phase that recreated the topics it was resuming
into would destroy the durable state the ADR-0003 section 7 resume contract reads, and
the run would report total loss for an apparatus reason. The parent does that once, per
execution, before the first attempt. tests/unit/test_phase_worker.py walks this module
and asserts provisioning is unreachable from it.

Exit codes are the channel back to the parent, so they are meaningful and few:

    0   the phase completed
    2   the run was refused, or ended as an apparatus failure
    -9  (as seen by the parent) the phase was SIGKILLed, which for a kill run is
        the expected outcome of a successful injection rather than an error

A phase that was killed writes nothing on the way out. That is not an oversight: it is
what makes the marker's before-the-kill discipline load-bearing rather than decorative.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from proofbench.config import Settings, get_settings, repo_root
from proofbench.core.configs import CONFIGURATION_NAMES, build_configuration
from proofbench.core.faults import PHASE_OF_FAULT, select_injector
from proofbench.core.recovery import ApparatusFailure
from proofbench.core.run import ingest, load_schedule_entry, process
from proofbench.core.saga import expand_sagas
from proofbench.core.state import (
    OUTCOME_COMPLETED,
    OUTCOME_FAILED,
    OUTCOME_STARTED,
    PHASE_INGEST,
    PHASE_PROCESS,
    Attempt,
    RunState,
)
from proofbench.core.trace import load_trace

EXIT_COMPLETED = 0
EXIT_REFUSED = 2

PHASES = (PHASE_INGEST, PHASE_PROCESS)

STATE_FILE = "run_state.json"


def state_directory(run_id: int, configuration_name: str, settings: Settings) -> Path:
    """Where this execution's durable state and evidence live.

    Derived rather than passed, so the parent and the child cannot disagree about it.
    A child that wrote its marker somewhere the parent did not read would restart with
    a clean marker every time and re-fire the fault forever.
    """
    return repo_root() / settings.run_output_dir / f"run_{run_id:02d}" / configuration_name


def _load_or_create_state(
    path: Path, run_id: int, configuration_name: str, entry: dict[str, Any]
) -> RunState:
    existing = RunState.load(path)
    if existing is not None:
        return existing
    return RunState(
        run_id=run_id,
        configuration=configuration_name,
        entry_names_a_fault=entry["fault_point"] is not None,
    )


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, required=True, help="Schedule entry to run.")
    parser.add_argument(
        "--config", required=True, choices=list(CONFIGURATION_NAMES), help="Configuration."
    )
    parser.add_argument("--phase", required=True, choices=list(PHASES), help="Phase to execute.")
    args = parser.parse_args(argv)

    try:
        entry = load_schedule_entry(args.run_id, settings)
    except ApparatusFailure as exc:
        print(f"phase refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    configuration = build_configuration(args.config, args.run_id, settings)
    directory = state_directory(args.run_id, args.config, settings)
    directory.mkdir(parents=True, exist_ok=True)
    state_path = directory / STATE_FILE
    state = _load_or_create_state(state_path, args.run_id, args.config, entry)

    try:
        attempt_number = state.next_attempt_number(args.phase)
        # Selecting the injector is where the arm-once guard lives, so it runs before
        # any work: a run whose marker was lost is stopped here rather than after it
        # has produced half a stream.
        injector = select_injector(entry, state, state_path, args.phase, settings)
    except ApparatusFailure as exc:
        state.record_attempt(Attempt(args.phase, 0, OUTCOME_FAILED, detail=str(exc)))
        state.save(state_path)
        print(f"apparatus failure, no result: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    # Recorded BEFORE the work, and replaced when the work ends. A SIGKILLed phase
    # cannot write its own epitaph, so it writes it in advance: a record still saying
    # "started" after the process is gone is the record of a kill. Without this nothing
    # would mark a killed attempt at all, and both the arm-once cross-check and the
    # crash-loop backstop would count zero forever.
    state.record_attempt(Attempt(args.phase, attempt_number, OUTCOME_STARTED))
    state.save(state_path)

    def record_progress(last_applied: int) -> None:
        """Persist how far this attempt has durably got, so a kill leaves a record.

        The attempt is rewritten in place, so a SIGKILL at any instant leaves a
        "started" record carrying the last offset the phase actually applied. The
        attributability invariant needs that number: the gap between it and where the
        restart resumes is what explains a lost side effect, and without it every
        baseline loss is unattributable and a real measurement is reported as an
        apparatus failure.
        """
        state.record_attempt(
            Attempt(
                args.phase,
                attempt_number,
                OUTCOME_STARTED,
                resumed_at=resumed_so_far[0],
                last_applied=last_applied,
            )
        )
        state.save(state_path)

    resumed_so_far: list[int | None] = [None]

    trace = load_trace(repo_root() / settings.trace_path)
    sagas = expand_sagas(str(entry["seed"]), settings, trace)

    try:
        stats: dict[str, Any]
        if args.phase == PHASE_INGEST:
            stats = ingest(configuration, sagas, state.transactions, injector)
            resumed_at, last_applied = None, None
        else:
            stats = process(
                configuration,
                settings,
                len(sagas),
                state.budget,
                state.transactions,
                is_control=bool(entry["control"]),
                injector=injector,
                # RunState IS the WindowState the boundary consumes, so the fault
                # window the injector opened is the one the recovery path reads.
                window=state,
                progress=record_progress,
            )
            failed: list[str] = list(stats.get("permanently_failed_keys", []))
            state.permanently_failed_keys = sorted(set(state.permanently_failed_keys) | set(failed))
            resumed_at = (
                int(stats["resumed_at_offset"]) if int(stats["resumed_at_offset"]) >= 0 else None
            )
            last_applied = (
                int(stats["last_applied_offset"])
                if int(stats["last_applied_offset"]) >= 0
                else None
            )
    except ApparatusFailure as exc:
        # Reached only when the phase survives long enough to raise. A SIGKILLed phase
        # never gets here, and the parent infers that outcome from the exit status
        # instead, which is why the marker cannot rely on this path.
        state.record_attempt(Attempt(args.phase, attempt_number, OUTCOME_FAILED, detail=str(exc)))
        state.save(state_path)
        print(f"apparatus failure, no result: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    # The fault window closes when the phase that hosted it finishes: the affected saga
    # has been resolved one way or the other, so a delivery failure after this point is
    # no longer part of the fault.
    if state.fault_fired and PHASE_OF_FAULT.get(str(entry["fault_type"])) == args.phase:
        state.fault_window_closed = True

    state.record_attempt(
        Attempt(
            args.phase,
            attempt_number,
            OUTCOME_COMPLETED,
            resumed_at=resumed_at,
            last_applied=last_applied,
        )
    )
    state.save(state_path)
    print(json.dumps({"phase": args.phase, "attempt": attempt_number, **stats}, sort_keys=True))
    return EXIT_COMPLETED


if __name__ == "__main__":
    raise SystemExit(main())
