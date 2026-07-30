"""C3's comparison: what it compares, and what it refuses to compare.

CLAIMS.md's literal wording, "rebuilds the sink byte-identical to the original run",
cannot be met by raw topic bytes: a replay gets different offsets, timestamps and
producer epochs, all broker-assigned rather than produced by the consumer under test.
ADR-0004 settles the reading as the effect log, which is the faithful sense of "the
sink" for a harness whose whole subject is side effects.

The refusal matters as much as the comparison. An empty ledger and an empty replay
checksum identically, so a replay that read nothing would report a match. That is the
C3 shape of the vacuous-guard problem this repository has already met twice.
"""

from __future__ import annotations

import pytest

from proofbench.config import Settings
from proofbench.core.configs import GOOD, build_configuration
from proofbench.core.recovery import ApparatusFailure
from proofbench.core.replay import REPLAY_SCOPE, compare, replay_configuration, replay_topics
from proofbench.interfaces.ledger import SideEffectRecord


def _record(key: str, sequence: int, checksum: str = "a" * 64) -> SideEffectRecord:
    return SideEffectRecord(
        idempotency_key=key,
        saga_id=key.split(":")[0],
        step_name=key.split(":")[1],
        sequence=sequence,
        payload_checksum=checksum,
    )


LEDGER = (_record("s0:create_ticket", 0), _record("s0:charge_card", 1))


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(_env_file=None, broker_bootstrap_servers="broker-placeholder:1")


def test_an_identical_rebuild_matches() -> None:
    outcome = compare(LEDGER, LEDGER, 0, GOOD, "sink_a")
    assert outcome.matched
    assert outcome.original_records == outcome.replayed_records == 2


def test_arrival_order_does_not_break_a_match() -> None:
    """A replay is not obliged to reproduce arrival order, only content."""
    assert compare(LEDGER, tuple(reversed(LEDGER)), 0, GOOD, "sink_a").matched


def test_a_missing_record_fails_the_match() -> None:
    assert not compare(LEDGER, LEDGER[:1], 0, GOOD, "sink_a").matched


def test_a_duplicated_record_fails_the_match() -> None:
    """Collapsing duplicates would make C3 blind to what C1 counts."""
    assert not compare(LEDGER, (*LEDGER, LEDGER[0]), 0, GOOD, "sink_a").matched


def test_a_changed_payload_fails_the_match() -> None:
    """Keys alone would call a replay identical that rebuilt different contents."""
    tampered = (LEDGER[0], _record("s0:charge_card", 1, checksum="b" * 64))
    assert not compare(LEDGER, tampered, 0, GOOD, "sink_a").matched


def test_an_empty_original_is_refused_rather_than_matched() -> None:
    """The C3 shape of the vacuous-guard problem.

    An empty ledger and an empty replay checksum identically, so a replay that read
    nothing would report a match and C3 would pass on no evidence at all.
    """
    with pytest.raises(ApparatusFailure, match="empty original ledger"):
        compare((), (), 0, GOOD, "sink_a")


def test_the_replay_writes_to_fresh_sinks(settings: Settings) -> None:
    """Writing into the originals would append and double the ledger."""
    configuration = build_configuration(GOOD, 3, settings)
    topics = replay_topics(configuration)
    assert topics.sink_a != configuration.topics.sink_a
    assert topics.sink_b != configuration.topics.sink_b
    assert topics.sink_a.endswith(REPLAY_SCOPE)
    # The input topic is shared: replaying the same event log is the point.
    assert topics.input == configuration.topics.input


def test_the_replay_uses_its_own_consumer_group(settings: Settings) -> None:
    """Otherwise it resumes at the end of the topic and reads nothing.

    A replay that read nothing would rebuild an empty ledger, and the empty-original
    refusal above is the only thing that would catch it. Better not to reach that.
    """
    configuration = build_configuration(GOOD, 3, settings)
    replay = replay_configuration(configuration, settings)
    assert replay.consumer["group.id"] != configuration.consumer["group.id"]
    assert replay.verifier["group.id"] != configuration.verifier["group.id"]
    assert REPLAY_SCOPE in str(replay.consumer["group.id"])


def test_everything_else_about_the_replay_is_identical(settings: Settings) -> None:
    """C3 asks whether the committed consumer is deterministic.

    Handing the replay a different consumer would answer a different question, so only
    the sinks and the group change and every allow-listed setting is carried over.
    """
    configuration = build_configuration(GOOD, 3, settings)
    replay = replay_configuration(configuration, settings)

    for key in ("enable.auto.commit", "isolation.level", "enable.auto.offset.store"):
        assert replay.consumer[key] == configuration.consumer[key]
    assert replay.offset_commit == configuration.offset_commit
    assert replay.transaction_boundary == configuration.transaction_boundary
    assert replay.sink_producer == configuration.sink_producer
