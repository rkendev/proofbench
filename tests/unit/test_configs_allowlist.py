"""INV-P3: the two configurations differ only where CLAIMS.md says they differ.

This is the gate C2's floor depends on. C2 requires the known-bad baseline to lose
at least one side effect in at least 80 percent of the 20 kill runs, and that
number is only about delivery configuration if the delivery configuration is the
only thing that differs. Hand the good configuration a larger batch, a different
sink path, or a longer timeout and the baseline might fail for that instead, at
which point the 80 percent measures the rigging.

Restated as a rule, because it is the one this file enforces: keys present in
exactly one configuration must be allow-listed; keys present in both with
different values must be allow-listed or identity-derived; the verifier must be
identical once identities are stripped; and structural differences must be
allow-listed too.

The allow-list is enumerated independently below rather than imported and trusted.
Widening what C2 is permitted to be measuring then takes two visible edits in two
files instead of one quiet one.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from proofbench.config import Settings, repo_root
from proofbench.core.configs import (
    ALLOW_LIST,
    BASELINE,
    GOOD,
    IDENTITY_KEYS,
    STRUCTURAL_ALLOW_LIST,
    ConfigurationError,
    RunConfiguration,
    build_both,
    build_configuration,
    transactional_id_for,
)

RUN_ID = 0
BOOTSTRAP = "broker-placeholder:1"

# Enumerated here on purpose. See the module docstring.
EXPECTED_ALLOW_LIST = {
    "enable.idempotence",
    "transactional.id",
    "isolation.level",
    "enable.auto.commit",
    "auto.commit.interval.ms",
}

# Every allow-listed setting, mapped to the token that traces it back to a
# recorded document. CLAIMS.md names most of them, but not always by the setting's
# own name: it says "a read_committed consumer" rather than "isolation.level", so
# the value is what a reader can find. auto.commit.interval.ms is not in CLAIMS.md
# at all, because CLAIMS.md fixes the baseline's semantics and not its numeric
# tuning; ADR-0002 is where that number was frozen, and dated.
TRACEABLE_TO_CLAIMS = {
    "enable.idempotence": "enable.idempotence",
    "enable.auto.commit": "enable.auto.commit",
    "isolation.level": "read_committed",
    "transactional.id": "transactional",
}
TRACEABLE_TO_ADR = {"auto.commit.interval.ms": "auto.commit.interval.ms"}


@pytest.fixture(scope="module")
def settings() -> Settings:
    """Frozen defaults plus a placeholder broker, never dialled."""
    return Settings(_env_file=None, broker_bootstrap_servers=BOOTSTRAP)


@pytest.fixture(scope="module")
def configurations(settings: Settings) -> dict[str, RunConfiguration]:
    return build_both(RUN_ID, settings)


def _sole_presence(one: Mapping[str, Any], other: Mapping[str, Any]) -> set[str]:
    """Keys present in exactly one of the two maps."""
    return set(one) ^ set(other)


def _differing_values(one: Mapping[str, Any], other: Mapping[str, Any]) -> set[str]:
    """Keys present in both maps but carrying different values."""
    return {key for key in set(one) & set(other) if one[key] != other[key]}


# --------------------------------------------------------------------------
# The four INV-P3 rules
# --------------------------------------------------------------------------


def test_a_key_present_in_only_one_configuration_is_allow_listed(
    configurations: dict[str, RunConfiguration],
) -> None:
    good, baseline = configurations[GOOD], configurations[BASELINE]
    for role, good_conf in good.client_sections().items():
        offenders = _sole_presence(good_conf, baseline.client_sections()[role]) - ALLOW_LIST
        assert not offenders, (
            f"{role}: setting(s) {sorted(offenders)} exist in one configuration "
            f"and not the other, and are not on the INV-P3 allow-list"
        )


def test_a_key_that_differs_in_value_is_allow_listed_or_an_identity(
    configurations: dict[str, RunConfiguration],
) -> None:
    good, baseline = configurations[GOOD], configurations[BASELINE]
    permitted = ALLOW_LIST | IDENTITY_KEYS
    for role, good_conf in good.client_sections().items():
        offenders = _differing_values(good_conf, baseline.client_sections()[role]) - permitted
        assert not offenders, (
            f"{role}: setting(s) {sorted(offenders)} carry different values in the "
            f"two configurations, and are neither allow-listed nor identity-derived"
        )


def test_the_verifier_is_identical_once_identities_are_stripped(
    configurations: dict[str, RunConfiguration],
) -> None:
    """Both configurations are read back by the same code with the same settings.

    Including the isolation level: read_committed filters only aborted
    transactional messages, so it returns the baseline's non-transactional writes
    in full. If verification differed, the two ledgers would have been produced
    by two different measurements.
    """
    good = dict(configurations[GOOD].verifier)
    baseline = dict(configurations[BASELINE].verifier)
    for key in IDENTITY_KEYS:
        good.pop(key, None)
        baseline.pop(key, None)
    assert good == baseline
    assert configurations[GOOD].verifier["isolation.level"] == "read_committed"
    assert configurations[BASELINE].verifier["isolation.level"] == "read_committed"


def test_structural_differences_are_allow_listed(
    configurations: dict[str, RunConfiguration],
) -> None:
    good = configurations[GOOD].structural_fields()
    baseline = configurations[BASELINE].structural_fields()
    offenders = _differing_values(good, baseline) - STRUCTURAL_ALLOW_LIST
    assert not offenders, f"structural field(s) {sorted(offenders)} differ and are not allow-listed"
    assert set(good) == set(baseline)


# --------------------------------------------------------------------------
# The allow-list itself
# --------------------------------------------------------------------------


def test_the_allow_list_is_exactly_what_this_file_enumerates() -> None:
    """Widening it takes two edits in two files, not one quiet one."""
    assert set(ALLOW_LIST) == EXPECTED_ALLOW_LIST


def test_every_allow_listed_setting_traces_to_a_recorded_document() -> None:
    """No entry may be there merely because the harness found it convenient.

    Reads the documents rather than trusting this file's own list. An allow-list
    entry that neither CLAIMS.md nor ADR-0002 mentions would be a difference
    between the two configurations that nobody pre-registered, which is precisely
    the freedom INV-P3 exists to remove.
    """
    assert set(TRACEABLE_TO_CLAIMS) | set(TRACEABLE_TO_ADR) == set(ALLOW_LIST), (
        "every allow-listed setting must be accounted for here, so widening the "
        "allow-list forces someone to say where the new entry comes from"
    )

    contract = (repo_root() / Path("CLAIMS.md")).read_text(encoding="utf-8")
    for setting, token in TRACEABLE_TO_CLAIMS.items():
        assert token in contract, (
            f"{setting} is allow-listed on the grounds that CLAIMS.md names "
            f"{token!r}, and it does not"
        )

    adr = (repo_root() / Path("adr/0002-measurement-invariants.md")).read_text(encoding="utf-8")
    for setting, token in TRACEABLE_TO_ADR.items():
        assert token in adr, (
            f"{setting} is allow-listed on the grounds that ADR-0002 froze it, and it does not"
        )


def test_the_rules_above_can_actually_fail(
    settings: Settings, configurations: dict[str, RunConfiguration]
) -> None:
    """Three of the four rules pass by absence, so the mechanism is pinned.

    A comparison that always returned an empty set would satisfy every assertion
    above while permitting anything at all.
    """
    good = dict(configurations[GOOD].ingest_producer)
    baseline = dict(configurations[BASELINE].ingest_producer)

    smuggled = {**good, "compression.type": "gzip"}
    assert _sole_presence(smuggled, baseline) - ALLOW_LIST == {"compression.type"}

    retuned = {**good, "linger.ms": settings.producer_linger_ms + 1}
    assert "linger.ms" in _differing_values(retuned, {**baseline, **good})


# --------------------------------------------------------------------------
# What the two configurations actually are
# --------------------------------------------------------------------------


def test_the_good_configuration_is_what_c1_names(
    settings: Settings, configurations: dict[str, RunConfiguration]
) -> None:
    """An idempotent transactional producer, read_committed, offsets in the transaction."""
    good = configurations[GOOD]
    assert good.ingest_producer["enable.idempotence"] is True
    assert good.sink_producer["enable.idempotence"] is True
    assert good.sink_producer["transactional.id"] == transactional_id_for(RUN_ID, GOOD, "sink")
    assert good.consumer["isolation.level"] == "read_committed"
    assert good.consumer["enable.auto.commit"] is False
    assert good.transactional
    assert good.transaction_boundary == settings.transaction_boundary


def test_the_baseline_is_what_c2_names(
    settings: Settings, configurations: dict[str, RunConfiguration]
) -> None:
    """enable.auto.commit, commit before processing, no idempotence, read_uncommitted."""
    baseline = configurations[BASELINE]
    assert baseline.ingest_producer["enable.idempotence"] is False
    assert baseline.sink_producer["enable.idempotence"] is False
    assert "transactional.id" not in baseline.ingest_producer
    assert "transactional.id" not in baseline.sink_producer
    assert baseline.consumer["enable.auto.commit"] is True
    assert baseline.consumer["auto.commit.interval.ms"] == settings.baseline_auto_commit_interval_ms
    assert baseline.consumer["isolation.level"] == "read_uncommitted"
    assert not baseline.transactional
    assert baseline.transaction_boundary is None


def test_commit_placement_is_a_client_setting_not_a_code_path(
    configurations: dict[str, RunConfiguration],
) -> None:
    """enable.auto.offset.store is True in both, which is what makes that true.

    librdkafka stores the offset the moment consume() hands the message over, so
    under the baseline enable.auto.commit then commits it on the interval whether
    or not the application got as far as writing the sinks. That is
    commit-before-processing, produced by settings rather than by a branch. In
    the good configuration it is inert, because auto-commit is off and offsets
    travel through send_offsets_to_transaction.
    """
    for name in (GOOD, BASELINE):
        assert configurations[name].consumer["enable.auto.offset.store"] is True


def test_the_shared_tuning_really_is_shared(
    settings: Settings, configurations: dict[str, RunConfiguration]
) -> None:
    """The frozen knobs that decide what is in flight at the kill instant.

    ADR-0002 froze these because what is in flight is what determines whether the
    baseline loses a side effect. If they drifted apart, C2 would be comparing two
    workloads rather than two configurations.
    """
    good, baseline = configurations[GOOD], configurations[BASELINE]
    for producer in (
        good.ingest_producer,
        good.sink_producer,
        baseline.ingest_producer,
        baseline.sink_producer,
    ):
        assert producer["linger.ms"] == settings.producer_linger_ms
        assert producer["batch.size"] == settings.producer_batch_size_bytes
    for consumer in (good.consumer, baseline.consumer, good.verifier, baseline.verifier):
        assert consumer["queued.min.messages"] == settings.consumer_queued_min_messages


def test_no_setting_restates_a_frozen_number(
    settings: Settings, configurations: dict[str, RunConfiguration]
) -> None:
    """Every tuning value traces back to Settings, which the schedule gate pins.

    A literal here would be a second authority for a number that decides a
    claim's outcome, and it would sit outside the byte-equality gate.
    """
    frozen = {
        settings.producer_linger_ms,
        settings.producer_batch_size_bytes,
        settings.consumer_queued_min_messages,
        settings.baseline_auto_commit_interval_ms,
    }
    seen: set[int] = set()
    for configuration in configurations.values():
        for section in configuration.client_sections().values():
            # bool is a subclass of int in Python, so enable.idempotence=True
            # would otherwise be collected here as the integer 1 and reported as
            # a stray literal. The flags are checked by the two configuration
            # tests above; this rule is about numbers.
            seen |= {
                value
                for value in section.values()
                if isinstance(value, int) and not isinstance(value, bool)
            }
    # Every integer the configurations carry is one of the frozen values. A
    # number that was not would be a literal introduced here.
    assert seen <= frozen, f"integer setting(s) {sorted(seen - frozen)} do not come from Settings"


# --------------------------------------------------------------------------
# Identities and topics
# --------------------------------------------------------------------------


def test_identities_differ_between_configurations(
    configurations: dict[str, RunConfiguration],
) -> None:
    """They must, or the second run would find the group already at the end.

    Allowed to differ in value, never in presence, which the value rule above
    enforces by keeping them out of the presence rule.
    """
    good, baseline = configurations[GOOD], configurations[BASELINE]
    assert good.consumer["group.id"] != baseline.consumer["group.id"]
    assert good.verifier["group.id"] != baseline.verifier["group.id"]
    assert "group.id" in good.consumer and "group.id" in baseline.consumer


def test_the_transactional_id_is_stable_and_role_scoped(settings: Settings) -> None:
    """Rebuilding the configuration must produce the same id, not a new one.

    ADR-0003: transactional.id is the zombie-fencing identity, so a restarted
    producer has to present the id the killed one used. A per-instance or random
    id would leave the dead epoch's transaction open until it timed out, with the
    Last Stable Offset parked behind it.
    """
    first = build_configuration(GOOD, RUN_ID, settings)
    second = build_configuration(GOOD, RUN_ID, settings)
    assert first.ingest_producer["transactional.id"] == second.ingest_producer["transactional.id"]
    # Roles are separate identities: the ingest producer and the sink producer are
    # two producers and must not share one.
    assert first.ingest_producer["transactional.id"] != first.sink_producer["transactional.id"]
    # And a different run is a different identity.
    other_run = build_configuration(GOOD, RUN_ID + 1, settings)
    assert (
        first.ingest_producer["transactional.id"] != other_run.ingest_producer["transactional.id"]
    )


def test_topics_are_scoped_per_run_and_configuration(
    configurations: dict[str, RunConfiguration],
) -> None:
    """Neither configuration may read the other's output."""
    good, baseline = configurations[GOOD].topics, configurations[BASELINE].topics
    assert set(good.all_topics()).isdisjoint(baseline.all_topics())
    assert len(set(good.all_topics())) == 3
    assert good.sinks == (good.sink_a, good.sink_b)


# --------------------------------------------------------------------------
# Plumbing
# --------------------------------------------------------------------------


def test_a_missing_broker_address_raises_rather_than_defaulting(settings: Settings) -> None:
    """A harness that quietly dialled a default would name an endpoint nobody chose."""
    without = settings.model_copy(update={"broker_bootstrap_servers": None})
    with pytest.raises(ConfigurationError, match="PB_BROKER_BOOTSTRAP_SERVERS"):
        build_configuration(GOOD, RUN_ID, without)


def test_an_unknown_configuration_name_raises(settings: Settings) -> None:
    with pytest.raises(ConfigurationError, match="unknown configuration"):
        build_configuration("optimistic", RUN_ID, settings)


def test_a_resolved_configuration_cannot_be_mutated(
    configurations: dict[str, RunConfiguration],
) -> None:
    """It is written into evidence, so it is a record of what the run used."""
    with pytest.raises(TypeError):
        configurations[GOOD].ingest_producer["linger.ms"] = 999  # type: ignore[index]


def test_the_evidence_form_carries_everything_that_distinguishes_the_two(
    configurations: dict[str, RunConfiguration],
) -> None:
    """A reader of resolved_config.json must be able to see the difference.

    That is the whole reason both configurations are written into every run's
    evidence rather than only the one that ran.
    """
    payload = configurations[GOOD].to_jsonable()
    assert payload["name"] == GOOD
    assert set(payload["clients"]) == {
        "ingest_producer",
        "sink_producer",
        "consumer",
        "verifier",
    }
    assert payload["clients"]["consumer"]["isolation.level"] == "read_committed"
    assert payload["topics"]["sink_a"].endswith(".sink_a")
