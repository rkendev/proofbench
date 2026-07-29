"""The committed agent trace is exactly what the master seed regenerates.

The trace is an input to every measurement: it decides the payload behind every
side effect, and therefore every ``payload_checksum`` that claim C3 will later
compare. So it gets the same treatment as docs/run_schedule.json rather than a
weaker one. Regenerating from the master seed must reproduce docs/agent_trace.json
byte for byte, and any of the following makes it go red:

- editing the committed artifact (retouching a payload after seeing a result)
- changing the master seed
- changing the vocabularies or the pool size in core/trace.py

ADR-0003 records why the trace is authored from the seed rather than sampled from
a live model. The property this file pins is the one that choice buys: a reader
who does not trust the author can regenerate the artifact and check it, which a
recorded trace could not offer.

Same division of authority as the schedule gate: expected values come from config
rather than being restated here, and the committed artifact is the immovable side.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from proofbench.config import Settings, repo_root
from proofbench.core.trace import (
    TRACE_SCHEMA_VERSION,
    VARIANTS_PER_STEP,
    build_trace,
    call_digest,
    parse_trace,
    serialize_trace,
)


@pytest.fixture(scope="module")
def settings() -> Settings:
    """Frozen defaults, deliberately not read from the ambient environment.

    ``get_settings`` would honour a stray PB_ variable or a local .env, which
    would make this gate pass or fail based on the machine it ran on.
    """
    return Settings(_env_file=None)


@pytest.fixture(scope="module")
def trace_path(settings: Settings) -> Path:
    return repo_root() / settings.trace_path


@pytest.fixture(scope="module")
def committed(trace_path: Path) -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(trace_path.read_text(encoding="utf-8"))
    return parsed


def test_regeneration_matches_the_committed_file_byte_for_byte(
    settings: Settings, trace_path: Path
) -> None:
    assert trace_path.is_file(), f"committed trace missing at {trace_path}"
    regenerated = serialize_trace(build_trace(settings))
    assert regenerated == trace_path.read_text(encoding="utf-8"), (
        "regenerating the trace from the master seed does not reproduce the "
        "committed artifact; either the artifact or the generator was changed"
    )
    # Bytes too, so an encoding or line-ending change cannot slip past a
    # comparison that decoded both sides into equal strings.
    assert regenerated.encode("utf-8") == trace_path.read_bytes()


def test_the_trace_covers_exactly_the_frozen_saga_steps(
    settings: Settings, committed: dict[str, Any]
) -> None:
    """A step with no templates would fail deep inside a run, not here.

    CLAIMS.md names exactly three steps and the schedule freezes them, so a trace
    that covered two or four would mean the workload had drifted from the
    contract.
    """
    assert committed["step_names"] == list(settings.saga_step_names)
    assert set(committed["tool_calls"]) == set(settings.saga_step_names)
    for step in settings.saga_step_names:
        assert len(committed["tool_calls"][step]) == VARIANTS_PER_STEP


def test_the_artifact_is_self_describing(settings: Settings, committed: dict[str, Any]) -> None:
    """A reader should not have to read the generator to know what they hold."""
    assert committed["schema_version"] == TRACE_SCHEMA_VERSION
    assert committed["master_seed"] == settings.master_seed
    assert committed["variants_per_step"] == VARIANTS_PER_STEP


def test_the_artifact_states_its_own_provenance(committed: dict[str, Any]) -> None:
    """The provenance travels with the artifact, not only in a README.

    The README must not imply a live model was involved. Neither must the file,
    which is the thing someone is most likely to read in isolation.
    """
    provenance = committed["provenance"].lower()
    assert "not sampled from a live model" in provenance
    assert "authored deterministically" in provenance


def test_every_template_is_well_formed(settings: Settings, committed: dict[str, Any]) -> None:
    for step in settings.saga_step_names:
        for index, call in enumerate(committed["tool_calls"][step]):
            where = f"{step}[{index}]"
            assert call["step_name"] == step, where
            assert call["tool"] == step, where
            assert call["call_id"] == f"{step}-{index:03d}", where
            assert call["arguments"], f"{where}: a tool call with no arguments"


def test_the_templates_actually_vary(settings: Settings, committed: dict[str, Any]) -> None:
    """Every assertion above would pass if the generator returned one template N times.

    This pins the variety itself. The floor is deliberately below the pool size:
    the argument fields are drawn independently, so a handful of collisions across
    64 draws is expected arithmetic rather than a defect, and demanding perfect
    distinctness would be asserting that a birthday collision never happens.
    """
    for step in settings.saga_step_names:
        combinations = {
            json.dumps(call["arguments"], sort_keys=True) for call in committed["tool_calls"][step]
        }
        assert len(combinations) >= VARIANTS_PER_STEP * 3 // 4, (
            f"{step}: only {len(combinations)} distinct argument combinations "
            f"across {VARIANTS_PER_STEP} templates"
        )
    # And the three steps must not have produced the same content as each other.
    per_step = {
        step: {call["call_id"] for call in committed["tool_calls"][step]}
        for step in settings.saga_step_names
    }
    assert len(set.union(*per_step.values())) == VARIANTS_PER_STEP * len(per_step)


def test_the_generator_is_sensitive_to_the_master_seed(settings: Settings) -> None:
    """A different master seed must produce a different trace.

    Every assertion above passes by agreement, so a generator that ignored its
    input and returned a constant would satisfy all of them. This pins the
    dependency itself: change the seed and the payloads move, while the frozen
    structure (steps, pool size) does not.
    """
    other = settings.model_copy(update={"master_seed": settings.master_seed + 1})
    baseline = build_trace(settings)
    shifted = build_trace(other)

    assert serialize_trace(shifted) != serialize_trace(baseline)
    assert shifted.step_names == baseline.step_names
    assert shifted.variants_per_step == baseline.variants_per_step
    # Same call ids, different content: the identity is structural, the payload
    # is what the seed drives.
    for step in settings.saga_step_names:
        assert [call.call_id for call in shifted.variants_for(step)] == [
            call.call_id for call in baseline.variants_for(step)
        ]
    assert any(
        shifted.variants_for(step)[index].arguments != baseline.variants_for(step)[index].arguments
        for step in settings.saga_step_names
        for index in range(VARIANTS_PER_STEP)
    )


def test_each_template_is_independently_derivable(settings: Settings) -> None:
    """A template depends on the seed, its step, and its index, and nothing else.

    Derived independently rather than by a running counter, so widening the pool
    or adding a step cannot silently shift every template after it. This is the
    same property core/schedule.py gives each run's seed.
    """
    assert call_digest(settings.master_seed, "charge_card", 0) != call_digest(
        settings.master_seed, "charge_card", 1
    )
    assert call_digest(settings.master_seed, "charge_card", 0) != call_digest(
        settings.master_seed, "create_ticket", 0
    )
    assert call_digest(settings.master_seed, "charge_card", 0) == call_digest(
        settings.master_seed, "charge_card", 0
    )


def test_a_parsed_artifact_round_trips(settings: Settings, committed: dict[str, Any]) -> None:
    """What a run loads must be what the gate compared.

    A run reads the trace from disk rather than rebuilding it. If parsing lost or
    coerced a field, the run would consume something the byte-equality gate never
    looked at, and the gate would be checking an artifact nobody used.
    """
    assert serialize_trace(parse_trace(committed)) == serialize_trace(build_trace(settings))


def test_an_unknown_step_names_itself(settings: Settings) -> None:
    """A missing step must fail where the cause is, not deep inside a run."""
    trace = build_trace(settings)
    with pytest.raises(KeyError, match="holds no tool calls for step"):
        trace.variants_for("refund_card")
