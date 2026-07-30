"""Every Kafka property ProofBench sets exists in the pinned client.

ADR-0002 records a cross-client gotcha as its reason for caring: librdkafka does
not share the Java client's property names, and CLAIMS.md already carries one
instance of it (idempotence is spelled ``enable.idempotence`` here, not the
kafka-python spelling). A property that silently does not exist is the failure
this pins, and it is a real hazard rather than a theoretical one, because the
consequence is not an error but a run that measures a configuration nobody
selected.

librdkafka validates property names when a client is constructed and rejects
unknown ones, so constructing a client IS the check. That makes this an offline
gate: no broker is dialled, nothing is produced, and the bootstrap address below
is a placeholder that is never connected to. The client connects lazily in a
background thread, and these handles are discarded before it matters.

This gate is also what makes the pin in requirements.txt load-bearing rather than
decorative. A client upgrade that renamed or dropped one of these properties would
otherwise surface as a changed measurement rather than as a failed build.
"""

from __future__ import annotations

import logging

import pytest

from proofbench.config import Settings

# Never connected to. librdkafka requires a syntactically valid bootstrap string
# to construct a client, and this is the cheapest one that is obviously not an
# endpoint anyone could reach.
PLACEHOLDER_BOOTSTRAP = "broker-placeholder:1"

# librdkafka logs connection failures on its background thread. Routing its log
# to a discarding Python logger keeps that noise out of the test report; it does
# not affect what is being checked.
_SILENT = logging.getLogger("proofbench.tests.silent_rdkafka")
_SILENT.addHandler(logging.NullHandler())
_SILENT.propagate = False


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(_env_file=None)


def _producer(extra: dict[str, object]) -> object:
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": PLACEHOLDER_BOOTSTRAP, **extra}, logger=_SILENT)


def _consumer(extra: dict[str, object]) -> object:
    from confluent_kafka import Consumer

    return Consumer(
        {"bootstrap.servers": PLACEHOLDER_BOOTSTRAP, "group.id": "contract", **extra},
        logger=_SILENT,
    )


def test_every_producer_property_the_harness_sets_exists(settings: Settings) -> None:
    """Constructed one at a time, so a failure names the offending property."""
    for key, value in {
        "linger.ms": settings.producer_linger_ms,
        "batch.size": settings.producer_batch_size_bytes,
        "enable.idempotence": True,
        "transactional.id": "proofbench.contract.probe",
        # Added at PB-T3 to close an INV-P3 leak. The value has to be acceptable to
        # both a transactional and a non-transactional producer, which is a stronger
        # requirement than mere existence: librdkafka caps it at
        # transaction.timeout.ms, so a value above the pinned 60000 would construct
        # under the baseline and be rejected under the good configuration.
        # tests/unit/test_derived_defaults.py covers the cap itself.
        "message.timeout.ms": settings.producer_message_timeout_ms,
    }.items():
        _producer({key: value})


def test_every_consumer_property_the_harness_sets_exists(settings: Settings) -> None:
    for key, value in {
        "auto.offset.reset": "earliest",
        "queued.min.messages": settings.consumer_queued_min_messages,
        "enable.auto.offset.store": True,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": settings.baseline_auto_commit_interval_ms,
        "enable.partition.eof": True,
        "isolation.level": "read_committed",
        # Added at PB-T3 so a restarted process phase does not wait out a dead
        # member's session on every kill. The broker enforces its own floor at join
        # time rather than at construction, so this checks the property exists and
        # tests/unit/test_timeout_relationships.py checks the value clears the floor.
        "session.timeout.ms": settings.consumer_session_timeout_ms,
    }.items():
        _consumer({key: value})


def test_both_isolation_levels_are_spelled_the_way_the_configurations_spell_them() -> None:
    """CLAIMS.md names read_committed; the baseline is its opposite.

    A rejected spelling here would mean one of the two configurations under test
    could not be constructed at all, which is worth its own line.
    """
    _consumer({"isolation.level": "read_committed"})
    _consumer({"isolation.level": "read_uncommitted"})


def test_the_transactional_api_surface_is_present() -> None:
    """The good configuration is built entirely out of these five calls.

    Checked by attribute rather than by invocation: invoking them needs a broker,
    and this gate is deliberately offline. The integration test exercises them
    against a real broker.
    """
    from confluent_kafka import Consumer, Producer

    for name in (
        "init_transactions",
        "begin_transaction",
        "send_offsets_to_transaction",
        "commit_transaction",
        "abort_transaction",
    ):
        assert hasattr(Producer, name), f"pinned client has no Producer.{name}"
    assert hasattr(Consumer, "consumer_group_metadata"), (
        "send_offsets_to_transaction needs the consumer group metadata, and the "
        "pinned client does not expose it"
    )


def test_the_error_predicates_the_recovery_contract_branches_on_are_present() -> None:
    """ADR-0003 fixes three error classes and a different response to each.

    Without these predicates the harness could only retry blindly, which is the
    behaviour the recovery contract exists to replace.
    """
    from confluent_kafka import KafkaError

    for name in ("retriable", "txn_requires_abort", "fatal"):
        assert hasattr(KafkaError, name), f"pinned client has no KafkaError.{name}"


def test_an_unknown_property_is_actually_rejected() -> None:
    """The four rules above pass by construction succeeding, so this pins the mechanism.

    If the client had stopped validating property names, every assertion above
    would pass while proving nothing at all.
    """
    from confluent_kafka import KafkaException

    with pytest.raises((KafkaException, ValueError, TypeError)):
        _producer({"this.property.does.not.exist": 1})
