"""The frozen run schedule: a pure deterministic expansion of the master seed.

CLAIMS.md requires that the run schedule be committed before the first broker boot.
This module is what makes that checkable rather than asserted: it expands the master
seed and the fault menu into the schedule, and tests/unit/test_schedule_frozen.py
proves that regenerating reproduces the committed docs/run_schedule.json byte for
byte. Cherry-picking runs after the fact would change those bytes and fail the gate.

Pure by construction: this module reads no file, writes no file, and imports no
client. ``build_schedule`` takes settings and returns data; ``serialize_schedule``
turns that data into the exact bytes of the committed artifact. The only thing that
touches disk is scripts/write_run_schedule.py.

Determinism is deliberately not delegated to ``random``. Every value is derived by
integer arithmetic on a SHA-256 digest, so the schedule depends on nothing but the
master seed: no generator implementation, no Python version, no platform. Each run's
seed is derived independently as ``sha256(f"{master_seed}:{run_id}")``, so run 7 is
reproducible without replaying runs 0 through 6, and inserting or reordering a run
cannot silently shift every seed after it.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from proofbench.config import Settings

# Bumped only if the artifact's shape changes, which is a deliberate act: the
# committed schedule is evidence, and a reader needs to know what shape they hold.
SCHEMA_VERSION = 1

# Bytes of the digest used for each derived value. The digest is 32 bytes, so these
# three windows do not overlap and never run off the end.
_SEED_BYTES = slice(0, 8)  # the run seed: 8 bytes, 64 bits, 16 hex characters
_SAGA_BYTES = slice(8, 16)  # the fault point's saga index
_STEP_BYTE = 16  # the fault point's step index


@dataclass(frozen=True, slots=True)
class FaultPoint:
    """Where a seeded fault fires, as a position in the saga stream.

    The fault fires immediately BEFORE step ``step_index`` of saga ``saga_index``
    is executed. Because ``step_index`` is drawn from 1 to M-1 inclusive, at least
    one step of that saga has completed and at least one has not, which is what
    CLAIMS.md means by killing "mid-saga". A step index of 0 would put the fault at
    a saga boundary, where nothing is half-done and the interesting failure modes
    do not exist.
    """

    saga_index: int
    step_index: int


@dataclass(frozen=True, slots=True)
class ScheduledRun:
    """One run of the schedule: everything a harness needs to reproduce it.

    ``seed`` is 16 lowercase hex characters rather than an integer on purpose. The
    value is 64 bits, which exceeds what a JSON number holds exactly, so a reader
    using IEEE doubles would silently round an integer seed. Byte-equality catches a
    mangled regeneration but not a mangled reading, so the wire format removes the
    hazard instead of relying on a gate to notice it.
    """

    run_id: int
    control: bool
    seed: str
    sagas: int
    steps_per_saga: int
    fault_type: str
    fault_point: FaultPoint | None

    def to_jsonable(self) -> dict[str, Any]:
        """Return the plain-data form written into the committed artifact."""
        point: dict[str, int] | None = None
        if self.fault_point is not None:
            point = {
                "saga_index": self.fault_point.saga_index,
                "step_index": self.fault_point.step_index,
            }
        return {
            "run_id": self.run_id,
            "control": self.control,
            "seed": self.seed,
            "sagas": self.sagas,
            "steps_per_saga": self.steps_per_saga,
            "fault_type": self.fault_type,
            "fault_point": point,
        }


@dataclass(frozen=True, slots=True)
class Schedule:
    """The whole frozen schedule: its inputs and its runs.

    The constants are carried in the artifact alongside the runs so that it is
    self-describing: a reader should not have to read this module to know what was
    frozen. They also sit inside the byte-equality gate, which is the point. The
    client tuning is there for the same reason N is: what is in flight at the
    instant of the kill is what determines whether the known-bad baseline loses a
    side effect, so leaving those knobs out of the frozen artifact would leave the
    numbers that drive claim C2's outcome adjustable after seeing the outcome.
    """

    schema_version: int
    master_seed: int
    sagas_per_run: int
    step_names: tuple[str, ...]
    fault_menu: tuple[str, ...]
    fault_saga_band: tuple[float, float]
    transaction_boundary: str
    client_tuning: dict[str, int]
    baseline_tuning: dict[str, int]
    runs: tuple[ScheduledRun, ...]

    def to_jsonable(self) -> dict[str, Any]:
        """Return the plain-data form written into the committed artifact."""
        return {
            "schema_version": self.schema_version,
            "master_seed": self.master_seed,
            "constants": {
                "sagas_per_run": self.sagas_per_run,
                "step_names": list(self.step_names),
                "fault_menu": list(self.fault_menu),
                "fault_saga_band": list(self.fault_saga_band),
                "transaction_boundary": self.transaction_boundary,
                "client_tuning": dict(self.client_tuning),
                "baseline_tuning": dict(self.baseline_tuning),
            },
            "runs": [run.to_jsonable() for run in self.runs],
        }

    @property
    def kill_runs(self) -> tuple[ScheduledRun, ...]:
        """The seeded kill runs, which are every run that is not the control."""
        return tuple(run for run in self.runs if not run.control)


def run_digest(master_seed: int, run_id: int) -> bytes:
    """Return the SHA-256 digest that every value for ``run_id`` is derived from."""
    return hashlib.sha256(f"{master_seed}:{run_id}".encode()).digest()


def derive_seed(master_seed: int, run_id: int) -> str:
    """Return the 64-bit run seed for ``run_id`` as 16 lowercase hex characters."""
    return run_digest(master_seed, run_id)[_SEED_BYTES].hex()


def derive_fault_point(digest: bytes, settings: Settings) -> FaultPoint:
    """Return the seeded fault point for the run whose digest this is.

    The saga index lands inside the configured band (the middle 60 percent of the
    stream by default), which keeps the fault away from the cold-start and drain
    edges where a miss would be an artifact of timing rather than of configuration.
    The step index is strictly mid-saga; see ``FaultPoint``.
    """
    low, high = settings.fault_saga_band
    first_saga = int(low * settings.sagas_per_run)
    last_saga = int(high * settings.sagas_per_run)
    saga_span = last_saga - first_saga
    saga_index = first_saga + int.from_bytes(digest[_SAGA_BYTES], "big") % saga_span

    # Steps 1 to M-1 inclusive, so the fault always leaves a saga part-done.
    step_span = settings.steps_per_saga - 1
    step_index = 1 + digest[_STEP_BYTE] % step_span
    return FaultPoint(saga_index=saga_index, step_index=step_index)


def build_schedule(settings: Settings) -> Schedule:
    """Expand the master seed and the fault menu into the frozen schedule.

    The control run occupies ``settings.control_run_id`` and carries no fault. Every
    other run is a kill run, and the fault menu is assigned round-robin over the
    kill-run ordinal rather than over ``run_id``: the control must not consume a
    slot in the rotation, or the distribution would silently shift if the control
    ever moved. With 20 kill runs over a menu of 3 this gives 7, 7, and 6.
    """
    runs: list[ScheduledRun] = []
    kill_ordinal = 0
    for run_id in range(settings.total_runs):
        digest = run_digest(settings.master_seed, run_id)
        seed = digest[_SEED_BYTES].hex()
        is_control = run_id == settings.control_run_id
        if is_control:
            fault_type = settings.no_fault_label
            fault_point: FaultPoint | None = None
        else:
            fault_type = settings.fault_menu[kill_ordinal % len(settings.fault_menu)]
            fault_point = derive_fault_point(digest, settings)
            kill_ordinal += 1
        runs.append(
            ScheduledRun(
                run_id=run_id,
                control=is_control,
                seed=seed,
                sagas=settings.sagas_per_run,
                steps_per_saga=settings.steps_per_saga,
                fault_type=fault_type,
                fault_point=fault_point,
            )
        )

    return Schedule(
        schema_version=SCHEMA_VERSION,
        master_seed=settings.master_seed,
        sagas_per_run=settings.sagas_per_run,
        step_names=settings.saga_step_names,
        fault_menu=settings.fault_menu,
        fault_saga_band=settings.fault_saga_band,
        transaction_boundary=settings.transaction_boundary,
        client_tuning={
            "producer_linger_ms": settings.producer_linger_ms,
            "producer_batch_size_bytes": settings.producer_batch_size_bytes,
            "consumer_max_batch_records": settings.consumer_max_batch_records,
            "consumer_queued_min_messages": settings.consumer_queued_min_messages,
        },
        baseline_tuning={
            "auto_commit_interval_ms": settings.baseline_auto_commit_interval_ms,
        },
        runs=tuple(runs),
    )


def serialize_schedule(schedule: Schedule) -> str:
    """Return the exact text of the committed artifact.

    One canonical form, used by both the writer script and the byte-equality gate,
    so the two cannot drift. ``sort_keys`` makes the output independent of field
    declaration order, and the trailing newline makes the file well formed for the
    text tools that read it.
    """
    return json.dumps(schedule.to_jsonable(), indent=2, sort_keys=True) + "\n"
