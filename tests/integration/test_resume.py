"""The ADR-0003 section 7 resume contract, against a live broker.

The ingest half is exercised here without an injector, by ingesting a truncated
stream and then asking the real ``ingest`` to finish it. That is the same durable
state a killed producer would have left, reached deterministically instead of by
racing a SIGKILL, so the resume rule is checked before anything depends on it.

Why it is worth its own file: these seven runs of the matrix
(``producer_sigkill_mid_send``) rest entirely on this rule, and the rule is what
makes them structurally incapable of losing a side effect, which is in turn the
arithmetic behind ADR-0004's prediction that C2 cannot pass. A defect here would
change a pre-registered claim outcome.

Skipped with a named reason when no broker is reachable, so CI runs the whole
offline chain and boots nothing.
"""

from __future__ import annotations

import json

import pytest

from proofbench.config import Settings, repo_root
from proofbench.core.configs import BASELINE, CONFIGURATION_NAMES, GOOD, build_configuration
from proofbench.core.recovery import resume_saga_index
from proofbench.core.run import durable_saga_indices, ingest, prepare_topics
from proofbench.core.saga import expand_sagas
from proofbench.core.topics import read_to_end_with_offsets
from proofbench.core.trace import load_trace
from proofbench.core.txn import TransactionLedger

SEED = "941fd94fa5c62dd8"
TRUNCATE_AFTER_SAGAS = 50


@pytest.fixture
def sagas(settings: Settings):
    trace = load_trace(repo_root() / settings.trace_path)
    return expand_sagas(SEED, settings, trace)


def _ingest_prefix(configuration, sagas, whole: int, plus_steps: int = 0) -> None:
    """Put a truncated stream on the input topic, as a killed producer would leave it.

    Non-transactional and direct, so the fixture is not the code under test. Under
    the good configuration a real kill would leave the partial saga inside an aborted
    transaction and therefore invisible; ``plus_steps`` here writes it visibly, which
    is the harder case for the resume rule to get right and the one the baseline
    actually produces.
    """
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": configuration.ingest_producer["bootstrap.servers"],
            "message.timeout.ms": configuration.ingest_producer["message.timeout.ms"],
        }
    )
    for saga in sagas[:whole]:
        for step in saga.steps:
            producer.produce(
                configuration.topics.input,
                key=step.record.idempotency_key.encode("utf-8"),
                value=step.payload_bytes(),
            )
    for step in sagas[whole].steps[:plus_steps]:
        producer.produce(
            configuration.topics.input,
            key=step.record.idempotency_key.encode("utf-8"),
            value=step.payload_bytes(),
        )
    assert producer.flush(30) == 0


@pytest.mark.parametrize("configuration_name", list(CONFIGURATION_NAMES))
def test_a_complete_prefix_resumes_at_the_first_missing_saga(
    broker: str, settings: Settings, sagas, configuration_name: str
) -> None:
    """The ordinary restart, in both configurations, by identical code."""
    configuration = build_configuration(configuration_name, settings.control_run_id, settings)
    prepare_topics(configuration, settings)
    _ingest_prefix(configuration, sagas, whole=TRUNCATE_AFTER_SAGAS)

    complete, _ = durable_saga_indices(configuration, settings.steps_per_saga)
    assert complete == set(range(TRUNCATE_AFTER_SAGAS))
    assert resume_saga_index(complete) == TRUNCATE_AFTER_SAGAS

    result = ingest(configuration, sagas, TransactionLedger())
    assert result["resumed_at_saga"] == TRUNCATE_AFTER_SAGAS
    assert result["durable_before"] == TRUNCATE_AFTER_SAGAS
    assert (
        result["records_sent"]
        == (settings.sagas_per_run - TRUNCATE_AFTER_SAGAS) * settings.steps_per_saga
    )


@pytest.mark.parametrize("configuration_name", list(CONFIGURATION_NAMES))
def test_a_half_sent_saga_is_resumed_rather_than_skipped(
    broker: str, settings: Settings, sagas, configuration_name: str
) -> None:
    """The rule that makes the ingest phase incapable of losing a side effect.

    A saga counts as complete only when all M steps are visible. Two of three visible
    means the saga is resumed, so every saga index ends up present. Skipping it would
    step past unrecorded work and report a loss no configuration caused, which is the
    precise failure that would make a kill run's number untrue.
    """
    configuration = build_configuration(configuration_name, settings.control_run_id, settings)
    prepare_topics(configuration, settings)
    _ingest_prefix(configuration, sagas, whole=TRUNCATE_AFTER_SAGAS, plus_steps=2)

    complete, _ = durable_saga_indices(configuration, settings.steps_per_saga)
    assert TRUNCATE_AFTER_SAGAS not in complete, "a two-of-three saga is not complete"
    assert resume_saga_index(complete) == TRUNCATE_AFTER_SAGAS

    ingest(configuration, sagas, TransactionLedger())

    # Every saga is present at least once. That is the property, and it is why these
    # seven runs of the matrix can duplicate but can never lose.
    records = read_to_end_with_offsets(dict(configuration.verifier), configuration.topics.input)
    seen: dict[int, set[int]] = {}
    for _, value in records:
        payload = json.loads(value.decode("utf-8"))
        seen.setdefault(int(payload["saga_index"]), set()).add(int(payload["step_index"]))
    assert set(seen) == set(range(settings.sagas_per_run))
    assert all(len(steps) == settings.steps_per_saga for steps in seen.values())


def test_the_baseline_duplicates_at_most_two_records_per_kill(
    broker: str, settings: Settings, sagas
) -> None:
    """The bound ADR-0003 section 7 buys, measured rather than asserted.

    Under the baseline the half-sent saga is non-transactional and durable, so the
    re-send duplicates whatever landed. At most M-1 = 2 records, because all three
    visible would mean the saga was complete and the resume would have skipped it.
    Exactly 3 is unreachable by this path.
    """
    configuration = build_configuration(BASELINE, settings.control_run_id, settings)
    prepare_topics(configuration, settings)
    _ingest_prefix(configuration, sagas, whole=TRUNCATE_AFTER_SAGAS, plus_steps=2)

    ingest(configuration, sagas, TransactionLedger())

    records = read_to_end_with_offsets(dict(configuration.verifier), configuration.topics.input)
    counts: dict[str, int] = {}
    for _, value in records:
        key = str(json.loads(value.decode("utf-8"))["idempotency_key"])
        counts[key] = counts.get(key, 0) + 1

    duplicated = sum(count - 1 for count in counts.values() if count > 1)
    assert duplicated == 2, f"expected the two half-sent steps to duplicate, got {duplicated}"
    assert duplicated <= settings.steps_per_saga - 1


def test_the_good_configuration_does_not_duplicate_across_the_resume(
    broker: str, settings: Settings, sagas
) -> None:
    """Because a real kill leaves the partial saga inside an aborted transaction.

    Simulated by writing only whole sagas, which is what read_committed shows after
    init_transactions has fenced the dead epoch and aborted what it left open. The
    resume then re-sends nothing that is already durable, which is the hazard ADR-0003
    section 7's wording refinement exists to avoid: idempotence does not span an epoch
    bump, so re-sending a committed saga would be a genuine duplicate under `good` and
    C1 would fail for an apparatus reason.
    """
    configuration = build_configuration(GOOD, settings.control_run_id, settings)
    prepare_topics(configuration, settings)
    _ingest_prefix(configuration, sagas, whole=TRUNCATE_AFTER_SAGAS)

    ingest(configuration, sagas, TransactionLedger())

    records = read_to_end_with_offsets(dict(configuration.verifier), configuration.topics.input)
    keys = [str(json.loads(value.decode("utf-8"))["idempotency_key"]) for _, value in records]
    assert len(keys) == len(set(keys)), "the resume re-sent a saga that was already durable"
    assert len(keys) == settings.sagas_per_run * settings.steps_per_saga


def test_a_fresh_run_resumes_at_zero(broker: str, settings: Settings, sagas) -> None:
    """The read-back is unconditional, so this is the path every clean run takes.

    On a fresh run the topic was just provisioned and is empty, so the read costs one
    cheap round trip and returns 0. The alternative would be a branch on "is this a
    restart" that both configurations would have to agree about, which is a branch
    INV-P3 would then have to police.
    """
    configuration = build_configuration(GOOD, settings.control_run_id, settings)
    prepare_topics(configuration, settings)

    complete, offsets = durable_saga_indices(configuration, settings.steps_per_saga)
    assert complete == set()
    assert offsets == {}

    result = ingest(configuration, sagas, TransactionLedger())
    assert result["resumed_at_saga"] == 0
    assert result["records_sent"] == settings.sagas_per_run * settings.steps_per_saga


def test_the_durability_read_is_read_committed_in_both_configurations(
    broker: str, settings: Settings
) -> None:
    """One rule, one semantics, two outcomes produced by allow-listed settings.

    The read-back uses the verifier map, which is identical in both configurations
    once group.id is stripped. That is not an INV-P3 widening: no client map changed,
    an existing identical map is used at a new call site. What it buys is that the
    completeness decision cannot differ between the two for any reason other than what
    each configuration actually made durable.
    """
    good = build_configuration(GOOD, settings.control_run_id, settings)
    baseline = build_configuration(BASELINE, settings.control_run_id, settings)
    assert good.verifier["isolation.level"] == baseline.verifier["isolation.level"]
    assert good.verifier["isolation.level"] == "read_committed"
