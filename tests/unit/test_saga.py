"""The saga expansion is deterministic, correctly shaped, and honestly checksummed.

Determinism is the load-bearing property. A seeded run has to be reproducible from
its seed, or claim C3 (replay determinism) is untestable rather than merely failed,
and a diff computed against a non-reproducible expectation would be a number with
nothing behind it.

The shape assertions come from config rather than being restated here. 200 sagas of
3 steps is 600 side effects, and every one of those numbers is frozen inside the
byte-equality gate on docs/run_schedule.json, so restating them here would be
asserting a value against itself.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from proofbench.config import Settings, repo_root
from proofbench.core.saga import (
    canonical_bytes,
    expand_sagas,
    expected_ledger,
    idempotency_key_for,
    observed_record,
    payload_checksum,
    saga_id_for,
)
from proofbench.core.trace import AgentTrace, load_trace

SEED = "0f1e2d3c4b5a6978"
OTHER_SEED = "abcdef0123456789"


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(_env_file=None)


@pytest.fixture(scope="module")
def trace(settings: Settings) -> AgentTrace:
    """The committed trace, because that is what a run actually consumes."""
    return load_trace(repo_root() / settings.trace_path)


def test_the_same_seed_produces_the_same_expected_ledger(
    settings: Settings, trace: AgentTrace
) -> None:
    """The property every later claim rests on.

    Compared field by field rather than by object identity, so a dataclass that
    happened to be cached would not pass this by accident.
    """
    first = expected_ledger(expand_sagas(SEED, settings, trace))
    second = expected_ledger(expand_sagas(SEED, settings, trace))
    assert first == second
    assert [record.payload_checksum for record in first] == [
        record.payload_checksum for record in second
    ]


def test_a_different_seed_produces_a_different_expected_ledger(
    settings: Settings, trace: AgentTrace
) -> None:
    """Every assertion above would pass if the expansion ignored its seed.

    The structure is a property of the contract and must not move; the keys and
    the payloads are what the seed drives.
    """
    baseline = expected_ledger(expand_sagas(SEED, settings, trace))
    shifted = expected_ledger(expand_sagas(OTHER_SEED, settings, trace))

    assert len(shifted) == len(baseline)
    assert [r.step_name for r in shifted] == [r.step_name for r in baseline]
    assert [r.sequence for r in shifted] == [r.sequence for r in baseline]
    assert {r.idempotency_key for r in shifted}.isdisjoint({r.idempotency_key for r in baseline})
    assert [r.payload_checksum for r in shifted] != [r.payload_checksum for r in baseline]


def test_the_ledger_has_the_frozen_shape(settings: Settings, trace: AgentTrace) -> None:
    sagas = expand_sagas(SEED, settings, trace)
    ledger = expected_ledger(sagas)

    assert len(sagas) == settings.sagas_per_run
    assert all(len(saga.steps) == settings.steps_per_saga for saga in sagas)
    assert len(ledger) == settings.sagas_per_run * settings.steps_per_saga

    # Every saga runs the frozen steps in the frozen order. A saga that reordered
    # them would change what "mid-saga" means for every seeded fault point.
    for saga in sagas:
        assert [step.record.step_name for step in saga.steps] == list(settings.saga_step_names)


def test_every_idempotency_key_is_unique_within_a_run(
    settings: Settings, trace: AgentTrace
) -> None:
    """Duplication detection rests entirely on this.

    If the expansion could emit one key twice, a clean run would look like a run
    with a duplicate and the differ would be reporting on the workload rather
    than on the delivery configuration.
    """
    ledger = expected_ledger(expand_sagas(SEED, settings, trace))
    keys = [record.idempotency_key for record in ledger]
    assert len(set(keys)) == len(keys)


def test_keys_from_different_runs_cannot_collide(settings: Settings, trace: AgentTrace) -> None:
    """The run seed is part of the key, so run 3 and run 7 share no key.

    Both runs expand the same 200 saga indices, so without the seed in the key
    every run in the matrix would use the same 600 keys.
    """
    first = {r.idempotency_key for r in expected_ledger(expand_sagas(SEED, settings, trace))}
    second = {r.idempotency_key for r in expected_ledger(expand_sagas(OTHER_SEED, settings, trace))}
    assert first.isdisjoint(second)


def test_the_sequence_is_the_position_in_the_stream(settings: Settings, trace: AgentTrace) -> None:
    ledger = expected_ledger(expand_sagas(SEED, settings, trace))
    assert [record.sequence for record in ledger] == list(range(len(ledger)))


def test_the_checksum_covers_the_bytes_that_go_on_the_wire(
    settings: Settings, trace: AgentTrace
) -> None:
    """One encoding, used by both the checksum and the producer.

    A separate encode-for-sending path would let a replay's checksum match while
    its bytes differed, which is exactly the failure claim C3 exists to detect
    and which would therefore be invisible to it.
    """
    sagas = expand_sagas(SEED, settings, trace)
    for step in sagas[0].steps + sagas[-1].steps:
        assert step.record.payload_checksum == payload_checksum(step.payload)
        assert canonical_bytes(step.payload) == step.payload_bytes()
        # And the wire bytes really do parse back to the same payload.
        assert json.loads(step.payload_bytes().decode("utf-8")) == step.payload


def test_the_canonical_encoding_ignores_key_order() -> None:
    """Otherwise a rebuild that constructed the dict differently would fail C3.

    This pins the mechanism rather than the result: the two dicts below are equal
    to Python, so a naive encoder that preserved insertion order would still
    produce different bytes for them.
    """
    one: dict[str, Any] = {"b": 2, "a": 1}
    two: dict[str, Any] = {"a": 1, "b": 2}
    assert canonical_bytes(one) == canonical_bytes(two)
    assert payload_checksum(one) == payload_checksum(two)


def test_a_record_read_back_off_a_sink_matches_the_expected_record(
    settings: Settings, trace: AgentTrace
) -> None:
    """The verify path rebuilds records from payloads, so the two must agree.

    If they did not, every run would report 600 losses and 600 unexpected keys
    for a reason that had nothing to do with Kafka.
    """
    sagas = expand_sagas(SEED, settings, trace)
    for saga in (sagas[0], sagas[len(sagas) // 2], sagas[-1]):
        for step in saga.steps:
            round_tripped = json.loads(step.payload_bytes().decode("utf-8"))
            assert observed_record(round_tripped, settings.steps_per_saga) == step.record


def test_a_tampered_payload_changes_the_observed_checksum(
    settings: Settings, trace: AgentTrace
) -> None:
    """The checksum is recomputed from the bytes, not read from a field.

    A record that carried its own checksum would agree with itself no matter what
    happened to the payload in between, which would make the integrity half of
    the diff decorative.
    """
    step = expand_sagas(SEED, settings, trace)[0].steps[1]
    tampered = json.loads(step.payload_bytes().decode("utf-8"))
    tampered["arguments"] = {**tampered["arguments"], "amount_cents": 1}

    rebuilt = observed_record(tampered, settings.steps_per_saga)
    assert rebuilt.idempotency_key == step.record.idempotency_key
    assert rebuilt.payload_checksum != step.record.payload_checksum


def test_the_key_and_saga_id_helpers_are_what_the_expansion_used(
    settings: Settings, trace: AgentTrace
) -> None:
    """The helpers are public because evidence and tests need to rebuild a key.

    Pinning them against the expansion keeps a second, silently diverging
    derivation from appearing later.
    """
    saga = expand_sagas(SEED, settings, trace)[7]
    assert saga.saga_id == saga_id_for(SEED, 7)
    for step in saga.steps:
        assert step.record.idempotency_key == idempotency_key_for(
            saga.saga_id, step.record.step_name
        )


def test_the_expansion_reads_no_file(settings: Settings, trace: AgentTrace) -> None:
    """Purity is a stated property of this module, so it is pinned.

    The trace arrives as data. An expansion that reached for the committed
    artifact itself would be reproducible only on a checkout, which is a weaker
    guarantee than the one claim C3 needs.
    """
    trimmed = AgentTrace(
        schema_version=trace.schema_version,
        master_seed=trace.master_seed,
        provenance=trace.provenance,
        variants_per_step=1,
        step_names=trace.step_names,
        tool_calls={step: (calls[0],) for step, calls in trace.tool_calls.items()},
    )
    sagas = expand_sagas(SEED, settings, trimmed)
    assert len(sagas) == settings.sagas_per_run
    # One template per step means every saga draws the same call id for a step.
    assert {step.payload["call_id"] for saga in sagas for step in saga.steps} == {
        calls[0].call_id for calls in trace.tool_calls.values()
    }
