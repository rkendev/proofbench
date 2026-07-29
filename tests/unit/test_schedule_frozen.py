"""The committed run schedule is exactly what the master seed regenerates.

This is the gate that makes "the schedule was committed before the first boot"
checkable by someone who does not trust the author, rather than a claim in a README.
Regenerating from the master seed must reproduce docs/run_schedule.json byte for
byte. Any of the following makes it go red, which is the whole point:

- editing the committed artifact (cherry-picking a run, moving a fault point)
- changing the master seed
- changing N, the fault menu, the fault band, or any frozen client-tuning value

The last of those matters as much as the first. What is in flight at the instant of
the kill is what determines whether the known-bad baseline loses a side effect, so
if the batch and poll parameters were not frozen, the numbers driving claim C2's
outcome would still be adjustable after seeing that outcome.

Note on where each side of the comparison comes from: the expected values are read
from config, not restated here, which would otherwise look like asserting a value
against itself. The committed artifact is the immovable side. If someone edits
config to 19 kill runs, regeneration no longer matches the committed bytes and this
file fails. Config is the authority for generating; the committed artifact is the
authority for what was frozen.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from proofbench.config import Settings, repo_root
from proofbench.core.schedule import (
    SCHEMA_VERSION,
    build_schedule,
    derive_seed,
    serialize_schedule,
)


@pytest.fixture(scope="module")
def settings() -> Settings:
    """Frozen defaults, deliberately not read from the ambient environment.

    ``get_settings`` would honour a stray PB_ variable or a local .env, which would
    make this gate pass or fail based on the machine it ran on. The committed
    artifact was generated from the defaults, so the defaults are what it is pinned
    against.
    """
    return Settings(_env_file=None)


@pytest.fixture(scope="module")
def schedule_path(settings: Settings) -> Path:
    return repo_root() / settings.schedule_path


@pytest.fixture(scope="module")
def committed(schedule_path: Path) -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(schedule_path.read_text(encoding="utf-8"))
    return parsed


def test_regeneration_matches_the_committed_file_byte_for_byte(
    settings: Settings, schedule_path: Path
) -> None:
    assert schedule_path.is_file(), f"committed schedule missing at {schedule_path}"
    regenerated = serialize_schedule(build_schedule(settings))
    committed_text = schedule_path.read_text(encoding="utf-8")
    assert regenerated == committed_text, (
        "regenerating the schedule from the master seed does not reproduce the "
        "committed artifact; either the artifact or a frozen constant was changed"
    )
    # Compare bytes too, so an encoding or line-ending change cannot slip past a
    # comparison that decoded both sides into equal strings.
    assert regenerated.encode("utf-8") == schedule_path.read_bytes()


def test_the_schedule_holds_the_pre_registered_run_counts(
    settings: Settings, committed: dict[str, Any]
) -> None:
    """Exactly 20 kill runs, plus one control.

    CLAIMS.md C1 says "at least 20" seeded kill runs and C2 says "the same 20", so
    the kill-run count is exactly 20. The control is not a kill run: it carries no
    fault and exists to show the apparatus reads zero when nothing was killed, so it
    changes no claim, no floor, and no ship rule (ADR-0002).
    """
    runs = committed["runs"]
    kills = [run for run in runs if run["fault_type"] != settings.no_fault_label]
    controls = [run for run in runs if run["control"]]

    assert len(kills) == settings.kill_runs
    assert len(controls) == 1
    assert len(runs) == settings.total_runs
    # control and fault_type must agree: exactly the control carries no fault.
    assert [run["run_id"] for run in controls] == [settings.control_run_id]
    assert all(run["fault_point"] is None for run in controls)
    assert all(run["fault_point"] is not None for run in kills)


def test_the_fault_menu_is_distributed_seven_seven_six(
    settings: Settings, committed: dict[str, Any]
) -> None:
    """Round-robin over the kill-run ordinal, counted from the committed file.

    20 kill runs do not divide by a menu of 3, so the split is 7, 7, 6 in menu
    order. Deliberately not rebalanced toward broker faults: single-node KRaft makes
    those stop and start outages rather than failover, which CLAIMS.md already
    states as the v1 scope limit, so they are the least informative of the three.
    """
    counts = Counter(
        run["fault_type"]
        for run in committed["runs"]
        if run["fault_type"] != settings.no_fault_label
    )
    assert set(counts) == set(settings.fault_menu), "a kill run carries an unknown fault type"

    menu_size = len(settings.fault_menu)
    expected = {
        fault: settings.kill_runs // menu_size
        + (1 if index < settings.kill_runs % menu_size else 0)
        for index, fault in enumerate(settings.fault_menu)
    }
    assert dict(counts) == expected
    assert sorted(counts.values(), reverse=True) == [7, 7, 6]


def test_every_fault_point_is_strictly_mid_saga(
    settings: Settings, committed: dict[str, Any]
) -> None:
    """A fault that lands at a saga boundary is not the failure CLAIMS.md describes.

    The step index runs from 1 to M-1, so at least one step of the saga has
    completed and at least one has not. Index 0 would mean nothing was half-done.
    The saga index stays inside the configured band, keeping the fault away from the
    cold-start and drain edges where a miss would be an artifact of timing.
    """
    low, high = settings.fault_saga_band
    first_saga = int(low * settings.sagas_per_run)
    last_saga = int(high * settings.sagas_per_run)

    for run in committed["runs"]:
        point = run["fault_point"]
        if point is None:
            continue
        where = f"run {run['run_id']}"
        assert 1 <= point["step_index"] <= settings.steps_per_saga - 1, f"{where}: boundary step"
        assert first_saga <= point["saga_index"] < last_saga, f"{where}: outside the fault band"


def test_every_run_seed_is_independently_derivable(
    settings: Settings, committed: dict[str, Any]
) -> None:
    """Each seed comes from the master seed and its own run id, nothing else.

    Derived independently rather than by chained draws, so run 7 is reproducible
    without replaying runs 0 through 6 and inserting or reordering a run cannot
    silently shift every seed after it. Seeds are 16 hex characters, not integers,
    because 64 bits exceeds what a JSON number holds exactly and a reader using
    doubles would round one silently.
    """
    seeds = [run["seed"] for run in committed["runs"]]
    assert len(set(seeds)) == len(seeds), "two runs share a seed"
    for run in committed["runs"]:
        assert run["seed"] == derive_seed(settings.master_seed, run["run_id"])
        assert len(run["seed"]) == 16
        int(run["seed"], 16)  # raises if it is not hex


def test_the_frozen_constants_are_carried_in_the_artifact(
    settings: Settings, committed: dict[str, Any]
) -> None:
    """The artifact is self-describing, and the tuning is inside the gate.

    A reader should not have to read the generator to know what was frozen. More
    importantly, the client tuning sits inside the byte-equality comparison above:
    N is not what determines whether the baseline loses a side effect, what is in
    flight at the kill instant is, so these knobs are frozen for the same reason and
    at the same time as N (ADR-0002).
    """
    assert committed["schema_version"] == SCHEMA_VERSION
    assert committed["master_seed"] == settings.master_seed

    constants = committed["constants"]
    assert constants["sagas_per_run"] == settings.sagas_per_run
    assert constants["step_names"] == list(settings.saga_step_names)
    assert constants["fault_menu"] == list(settings.fault_menu)
    assert constants["fault_saga_band"] == list(settings.fault_saga_band)
    assert constants["transaction_boundary"] == settings.transaction_boundary
    assert constants["client_tuning"] == {
        "producer_linger_ms": settings.producer_linger_ms,
        "producer_batch_size_bytes": settings.producer_batch_size_bytes,
        "consumer_max_batch_records": settings.consumer_max_batch_records,
        "consumer_queued_min_messages": settings.consumer_queued_min_messages,
    }
    assert constants["baseline_tuning"] == {
        "auto_commit_interval_ms": settings.baseline_auto_commit_interval_ms
    }

    # Every run restates the stream shape, so a run cannot disagree with the header.
    for run in committed["runs"]:
        assert run["sagas"] == settings.sagas_per_run
        assert run["steps_per_saga"] == settings.steps_per_saga


def test_the_generator_is_sensitive_to_the_master_seed(settings: Settings) -> None:
    """A different master seed must produce a different schedule.

    Every assertion above passes by agreement, so a generator that ignored its input
    and returned a constant would satisfy all of them. This pins the dependency
    itself: change the seed and the seeds and fault points move, while the frozen
    structure (run count, distribution) does not.
    """
    other = settings.model_copy(update={"master_seed": settings.master_seed + 1})
    baseline = build_schedule(settings)
    shifted = build_schedule(other)

    assert [run.seed for run in shifted.runs] != [run.seed for run in baseline.runs]
    assert serialize_schedule(shifted) != serialize_schedule(baseline)
    # The structure is a property of the contract, not of the seed.
    assert len(shifted.runs) == len(baseline.runs)
    assert [run.fault_type for run in shifted.runs] == [run.fault_type for run in baseline.runs]


def test_the_generator_is_sensitive_to_the_client_tuning(settings: Settings) -> None:
    """Changing a frozen tuning value changes the artifact, so the gate would catch it.

    This is the specific protection the tuning freeze exists for. Without it, the
    batch and poll parameters could be retuned after seeing C2's result and no gate
    would notice.
    """
    retuned = settings.model_copy(update={"producer_batch_size_bytes": 32768})
    assert serialize_schedule(build_schedule(retuned)) != serialize_schedule(
        build_schedule(settings)
    )
