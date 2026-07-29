"""The control run, end to end, against a live broker.

The control exists to show the harness does not report duplications or losses when
nothing was killed. It injects no fault, it is not one of the 20 kill runs, and it
is never evidence for C1 (ADR-0002).

A control run that always read zero would also read zero, so this file does not
stop at asserting a clean result. The last two tests are the negative control: a
record is planted directly on a scratch sink topic and the verify-and-diff path
has to report it. Without them, "the apparatus reads zero" would be consistent
with an apparatus that cannot see anything, which is the specific weakness the
control is supposed to remove.

Skipped with a named reason when no broker is reachable, so CI runs the whole
offline chain and boots nothing.
"""

from __future__ import annotations

import json

import pytest

from proofbench.config import Settings, repo_root
from proofbench.core.configs import BASELINE, CONFIGURATION_NAMES, GOOD, build_configuration
from proofbench.core.ledger_diff import KeyedLedgerDiffer, LedgerIntegrityError
from proofbench.core.run import execute_run, verify
from proofbench.core.saga import expand_sagas
from proofbench.core.topics import delete, provision
from proofbench.core.trace import load_trace

SCRATCH_TOPIC = "proofbench.selftest.sink"


def test_a_transactional_producer_initialises_against_the_broker(broker: str) -> None:
    """The single-node first-boot trap, checked directly.

    Without the three replication settings docker-compose.yml pins, the internal
    transaction-state topic cannot reach its default replication factor on one
    node and this call fails with an error naming neither the topic nor the
    cause. It is worth its own test because everything the good configuration
    does depends on it.
    """
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": broker,
            "enable.idempotence": True,
            "transactional.id": "proofbench.selftest.init",
        }
    )
    producer.init_transactions(30)


@pytest.mark.parametrize("configuration_name", list(CONFIGURATION_NAMES))
def test_the_control_run_is_clean(broker: str, settings: Settings, configuration_name: str) -> None:
    """The PB-T2 acceptance gate, for both configurations.

    The baseline has to be clean here too. A baseline that fails a no-fault run
    is an apparatus defect, not a finding: it would mean the harness cannot tell
    a bad configuration from a broken one, and every later kill result would be
    uninterpretable.
    """
    result = execute_run(settings.control_run_id, configuration_name, settings)

    assert result.schedule_entry["fault_point"] is None, "the control run carries no fault"
    assert len(result.expected) == settings.sagas_per_run * settings.steps_per_saga

    for sink in result.sinks:
        assert sink.diff.duplicated == (), f"{sink.name} duplicated on a run with no fault"
        assert sink.diff.lost == (), f"{sink.name} lost records on a run with no fault"
        assert sink.diff.is_clean
        assert sink.records_visible == len(result.expected)
    assert result.is_clean


def test_the_good_configuration_commits_one_transaction_per_saga(
    broker: str, settings: Settings
) -> None:
    """The frozen transaction boundary, observed rather than assumed."""
    result = execute_run(settings.control_run_id, GOOD, settings)
    assert result.summary()["transactions_committed"] == settings.sagas_per_run
    assert result.summary()["transactions_aborted"] == 0


def test_the_baseline_commits_no_transaction_at_all(broker: str, settings: Settings) -> None:
    """It is non-transactional, which is half of what makes it the known-bad one."""
    result = execute_run(settings.control_run_id, BASELINE, settings)
    assert result.summary()["transactions_committed"] == 0


def test_the_control_run_needed_no_recovery(broker: str, settings: Settings) -> None:
    """An empty recovery history is part of what the control shows.

    A clean result that had required three producer re-initialisations to get
    there would say something quite different about the apparatus than a clean
    result that required none.
    """
    result = execute_run(settings.control_run_id, GOOD, settings)
    recovery = result.summary()["recovery"]
    assert recovery["producer_reinits"] == 0
    assert recovery["retries"] == 0
    assert recovery["history"] == []


# --------------------------------------------------------------------------
# The negative control: the apparatus can see something when there is something
# --------------------------------------------------------------------------


@pytest.fixture
def scratch(broker: str) -> str:
    """An empty topic that belongs to no run, for planting records on."""
    provision(broker, [SCRATCH_TOPIC])
    try:
        yield SCRATCH_TOPIC
    finally:
        delete(broker, [SCRATCH_TOPIC])


def _planted(settings: Settings, count: int, repeat_first: bool = False):
    """Return the first ``count`` steps of a scratch saga stream."""
    trace = load_trace(repo_root() / settings.trace_path)
    sagas = expand_sagas("0f1e2d3c4b5a6978", settings, trace)
    steps = [step for saga in sagas for step in saga.steps][:count]
    return steps + ([steps[0]] if repeat_first else [])


def test_the_verify_and_diff_path_sees_a_duplicate(
    broker: str, settings: Settings, scratch: str
) -> None:
    """End to end, not a unit test on the differ.

    A record is produced twice onto a real topic, read back through the real
    verification consumer, and rebuilt into a real ledger. If any link in that
    chain silently deduplicated, the control's zero would mean nothing.
    """
    from confluent_kafka import Producer

    configuration = build_configuration(GOOD, settings.control_run_id, settings)
    steps = _planted(settings, 4, repeat_first=True)

    producer = Producer({"bootstrap.servers": broker})
    for step in steps:
        producer.produce(
            scratch, key=step.record.idempotency_key.encode("utf-8"), value=step.payload_bytes()
        )
    assert producer.flush(30) == 0

    observed = verify(configuration, scratch, settings.steps_per_saga)
    assert len(observed) == 5

    expected = tuple(step.record for step in steps[:4])
    result = KeyedLedgerDiffer().diff(expected, observed)
    assert len(result.duplicated) == 1
    assert result.duplicated[0].idempotency_key == steps[0].record.idempotency_key
    assert result.lost == ()
    assert not result.is_clean


def test_the_verify_and_diff_path_sees_a_loss(
    broker: str, settings: Settings, scratch: str
) -> None:
    """The other direction, for the same reason."""
    from confluent_kafka import Producer

    configuration = build_configuration(GOOD, settings.control_run_id, settings)
    steps = _planted(settings, 4)

    producer = Producer({"bootstrap.servers": broker})
    for step in steps[:3]:  # the fourth is never written
        producer.produce(
            scratch, key=step.record.idempotency_key.encode("utf-8"), value=step.payload_bytes()
        )
    assert producer.flush(30) == 0

    observed = verify(configuration, scratch, settings.steps_per_saga)
    result = KeyedLedgerDiffer().diff(tuple(step.record for step in steps), observed)

    assert result.duplicated == ()
    assert [r.idempotency_key for r in result.lost] == [steps[3].record.idempotency_key]


def test_a_tampered_payload_on_the_wire_is_caught(
    broker: str, settings: Settings, scratch: str
) -> None:
    """The checksum covers the bytes, and the bytes came off a real topic.

    This is what makes the integrity check more than a unit test: the payload is
    altered in transit, and the rebuilt record's checksum has to disagree with
    the expected one rather than being read back out of the payload.
    """
    from confluent_kafka import Producer

    configuration = build_configuration(GOOD, settings.control_run_id, settings)
    steps = _planted(settings, 2)

    tampered = json.loads(steps[1].payload_bytes().decode("utf-8"))
    tampered["arguments"] = {**tampered["arguments"], "smuggled": True}

    producer = Producer({"bootstrap.servers": broker})
    producer.produce(scratch, key=b"k0", value=steps[0].payload_bytes())
    producer.produce(
        scratch,
        key=b"k1",
        value=json.dumps(tampered, sort_keys=True, separators=(",", ":")).encode(),
    )
    assert producer.flush(30) == 0

    observed = verify(configuration, scratch, settings.steps_per_saga)
    with pytest.raises(LedgerIntegrityError, match="carries checksum"):
        KeyedLedgerDiffer().diff(tuple(step.record for step in steps), observed)


# Note on what is deliberately NOT here: that the two configurations never share
# a topic needs no broker, so a copy of it in this file would run in CI, where
# build_configuration correctly refuses a missing broker address, and fail. It
# lives in tests/unit/test_configs_allowlist.py, which is where a check that
# needs nothing running belongs.
