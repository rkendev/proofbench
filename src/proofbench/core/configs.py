"""The two configurations under test, as data, built from Settings.

CLAIMS.md fixes what distinguishes them. C1 names an idempotent transactional
producer, a read_committed consumer, and offset commits placed inside the
transaction. C2 names the known-bad baseline: enable.auto.commit with
commit-before-processing placement, and no idempotence. Everything else on the run
path is the same code.

**INV-P3, and why C2's floor depends on it.** The two configurations differ only
on the allow-listed Kafka client settings below. Same sink code, one produce-A,
flush, produce-B, flush path with no branch on configuration. Same ledger writer,
one verification consumer and one evidence serializer. If the good configuration
got a sink path or a writer the baseline did not, C2's 80 percent floor would
measure the rigging rather than the configuration, and a passing C1 would mean
nothing. tests/unit/test_configs_allowlist.py is the machine-checkable authority;
this docstring is not.

Two design notes that make INV-P3 hold more tightly than it otherwise would:

``enable.auto.offset.store`` is True in both. It is what makes the baseline's
commit placement "before processing" a pure client-setting consequence rather
than a divergent code path: librdkafka stores the offset the moment consume()
hands the message to the application, and enable.auto.commit then commits it on
the interval, whether or not the application got as far as writing the sinks. In
the good configuration it is inert, because enable.auto.commit is False and
offsets travel through send_offsets_to_transaction. That is what lets both
configurations share one consume loop.

The verifier is identical in both, including its isolation level. read_committed
filters only aborted transactional messages, so it returns the baseline's
non-transactional writes in full. Verification is therefore the same code and the
same settings for both configurations rather than something the allow-list has to
police.

No literal here restates a frozen number. Every tuning value is read from
Settings, whose defaults are emitted into docs/run_schedule.json and pinned by a
byte-equality gate.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from proofbench.config import Settings

GOOD = "good"
BASELINE = "baseline"
CONFIGURATION_NAMES: tuple[str, ...] = (GOOD, BASELINE)

# How offsets reach the broker. Structural rather than a client setting, because
# CLAIMS.md fixes the placement and the placement is what C2 is about.
COMMIT_INSIDE_TRANSACTION = "inside_transaction"
COMMIT_AUTO_BEFORE_PROCESSING = "auto_before_processing"

# The Kafka client settings on which the two configurations are permitted to
# differ. Every one of them is named by CLAIMS.md or is the frozen tuning that
# governs one that is. Widening this set widens what C2 is allowed to be
# measuring, so it is a deliberate, visible act: the gate enumerates the same set
# independently, and a change has to be made in two places.
ALLOW_LIST: frozenset[str] = frozenset(
    {
        "enable.idempotence",
        "transactional.id",
        "isolation.level",
        "enable.auto.commit",
        "auto.commit.interval.ms",
    }
)

# Keys whose values are derived identities rather than tuning. Both
# configurations carry them, and their values must differ, or the two runs would
# share a consumer group and the second would find the offsets already at the end
# of the topic. Allowed to differ in value, never in presence.
IDENTITY_KEYS: frozenset[str] = frozenset({"group.id", "transactional.id"})

# The non-client fields of a RunConfiguration that may differ.
STRUCTURAL_ALLOW_LIST: frozenset[str] = frozenset({"name", "offset_commit", "transaction_boundary"})


class ConfigurationError(Exception):
    """A configuration cannot be built from the settings it was given."""


@dataclass(frozen=True, slots=True)
class RunTopics:
    """The three topics one run of one configuration uses.

    Sinks A and B are Kafka topics rather than local stores, which is what puts
    the measured effect inside the transaction's reach. ADR-0003 records why:
    with the sinks outside Kafka, a kill between them either duplicates under
    both configurations or is absorbed under both, and either way the number
    measures the sink design rather than the delivery configuration.

    Scoped per run and per configuration so the two configurations never read
    each other's output and a rerun of one cannot contaminate the other.
    """

    input: str
    sink_a: str
    sink_b: str

    @property
    def sinks(self) -> tuple[str, str]:
        """The sinks in the frozen order: A is written before B is attempted."""
        return (self.sink_a, self.sink_b)

    def all_topics(self) -> tuple[str, ...]:
        return (self.input, self.sink_a, self.sink_b)


@dataclass(frozen=True, slots=True)
class RunConfiguration:
    """One named configuration, resolved for one run.

    Carries plain data, not clients. Nothing here dials a broker, so a
    configuration can be built, compared, and written into evidence offline,
    which is what lets the INV-P3 gate run in the offline suite.
    """

    name: str
    topics: RunTopics
    ingest_producer: Mapping[str, Any]
    sink_producer: Mapping[str, Any]
    consumer: Mapping[str, Any]
    verifier: Mapping[str, Any]
    offset_commit: str
    transaction_boundary: str | None

    @property
    def transactional(self) -> bool:
        """True when offsets and sink writes travel in one transaction."""
        return self.offset_commit == COMMIT_INSIDE_TRANSACTION

    def client_sections(self) -> dict[str, Mapping[str, Any]]:
        """The four client setting maps, keyed by the role they configure."""
        return {
            "ingest_producer": self.ingest_producer,
            "sink_producer": self.sink_producer,
            "consumer": self.consumer,
            "verifier": self.verifier,
        }

    def structural_fields(self) -> dict[str, Any]:
        """Everything about the configuration that is not a client setting."""
        return {
            "name": self.name,
            "offset_commit": self.offset_commit,
            "transaction_boundary": self.transaction_boundary,
        }

    def to_jsonable(self) -> dict[str, Any]:
        """Return the form written into a run's resolved_config.json evidence."""
        return {
            **self.structural_fields(),
            "topics": {
                "input": self.topics.input,
                "sink_a": self.topics.sink_a,
                "sink_b": self.topics.sink_b,
            },
            "clients": {role: dict(conf) for role, conf in self.client_sections().items()},
        }


def _scope(run_id: int, name: str) -> str:
    """The identity prefix every name in one run of one configuration shares."""
    return f"proofbench.r{run_id:02d}.{name}"


def topics_for(run_id: int, name: str) -> RunTopics:
    """Return the three topic names for one run of one configuration."""
    scope = _scope(run_id, name)
    return RunTopics(input=f"{scope}.input", sink_a=f"{scope}.sink_a", sink_b=f"{scope}.sink_b")


def transactional_id_for(run_id: int, name: str, role: str) -> str:
    """Return the transactional id for one producer role.

    Stable per run, configuration, and role. Never per-instance and never random,
    and that is the whole point (ADR-0003). transactional.id is the zombie-fencing
    identity: init_transactions bumps the producer epoch for that id and aborts
    whatever transaction the previous epoch left open, which is the only mechanism
    that cleans up after a producer SIGKILL. A per-instance id would leave the
    killed producer's transaction unfenced until transaction.timeout.ms expires,
    with the Last Stable Offset parked behind it and every read_committed consumer
    blocked on that partition.
    """
    return f"{_scope(run_id, name)}.{role}"


def _producer_base(settings: Settings, bootstrap: str) -> dict[str, Any]:
    """Producer settings shared by both configurations, byte for byte."""
    return {
        "bootstrap.servers": bootstrap,
        "linger.ms": settings.producer_linger_ms,
        "batch.size": settings.producer_batch_size_bytes,
    }


def _consumer_base(settings: Settings, bootstrap: str, group_id: str) -> dict[str, Any]:
    """Consumer settings shared by both configurations, byte for byte.

    ``enable.partition.eof`` gives the drain a deterministic end signal. Without
    it the only way to know a topic has been read to its end is a poll timeout,
    and a timeout that fired early would under-read the sink and report loss that
    never happened. A measurement harness cannot afford a stopping rule that can
    be wrong.
    """
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "queued.min.messages": settings.consumer_queued_min_messages,
        "enable.auto.offset.store": True,
        "enable.partition.eof": True,
    }


def _verifier(settings: Settings, bootstrap: str, run_id: int, name: str) -> dict[str, Any]:
    """The verification consumer, identical in both configurations.

    Always read_committed, for both. read_committed filters only aborted
    transactional messages, so it returns the baseline's non-transactional writes
    in full, and verification stays one code path with one set of settings.
    """
    return {
        **_consumer_base(settings, bootstrap, f"{_scope(run_id, name)}.verify"),
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    }


def build_configuration(name: str, run_id: int, settings: Settings) -> RunConfiguration:
    """Resolve one named configuration for one run.

    Raises rather than defaulting when the broker address is absent: a harness
    that quietly dialled a default would produce a run whose evidence named an
    endpoint nobody selected.
    """
    if name not in CONFIGURATION_NAMES:
        raise ConfigurationError(
            f"unknown configuration {name!r}; CLAIMS.md names exactly {list(CONFIGURATION_NAMES)}"
        )
    bootstrap = settings.broker_bootstrap_servers
    if not bootstrap:
        raise ConfigurationError(
            "PB_BROKER_BOOTSTRAP_SERVERS is not set, so no configuration can be "
            "resolved. Run `make broker-up`, which prints the value to use."
        )

    topics = topics_for(run_id, name)
    verifier = _verifier(settings, bootstrap, run_id, name)

    if name == GOOD:
        configuration = RunConfiguration(
            name=name,
            topics=topics,
            ingest_producer={
                **_producer_base(settings, bootstrap),
                "enable.idempotence": True,
                "transactional.id": transactional_id_for(run_id, name, "ingest"),
            },
            sink_producer={
                **_producer_base(settings, bootstrap),
                "enable.idempotence": True,
                "transactional.id": transactional_id_for(run_id, name, "sink"),
            },
            consumer={
                **_consumer_base(settings, bootstrap, _scope(run_id, name)),
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            },
            verifier=verifier,
            offset_commit=COMMIT_INSIDE_TRANSACTION,
            transaction_boundary=settings.transaction_boundary,
        )
    else:
        configuration = RunConfiguration(
            name=name,
            topics=topics,
            ingest_producer={
                **_producer_base(settings, bootstrap),
                "enable.idempotence": False,
            },
            sink_producer={
                **_producer_base(settings, bootstrap),
                "enable.idempotence": False,
            },
            consumer={
                **_consumer_base(settings, bootstrap, _scope(run_id, name)),
                "enable.auto.commit": True,
                "auto.commit.interval.ms": settings.baseline_auto_commit_interval_ms,
                "isolation.level": "read_uncommitted",
            },
            verifier=verifier,
            offset_commit=COMMIT_AUTO_BEFORE_PROCESSING,
            transaction_boundary=None,
        )

    return _freeze(configuration)


def _freeze(configuration: RunConfiguration) -> RunConfiguration:
    """Return the configuration with its client maps made read-only.

    A configuration is written into a run's evidence, so it is a record of what
    the run actually used. A caller that could mutate a setting after the
    evidence was serialized would make that record a claim rather than a fact.
    """
    return RunConfiguration(
        name=configuration.name,
        topics=configuration.topics,
        ingest_producer=MappingProxyType(dict(configuration.ingest_producer)),
        sink_producer=MappingProxyType(dict(configuration.sink_producer)),
        consumer=MappingProxyType(dict(configuration.consumer)),
        verifier=MappingProxyType(dict(configuration.verifier)),
        offset_commit=configuration.offset_commit,
        transaction_boundary=configuration.transaction_boundary,
    )


def build_both(run_id: int, settings: Settings) -> dict[str, RunConfiguration]:
    """Resolve both configurations for one run, for evidence and for the gate."""
    return {name: build_configuration(name, run_id, settings) for name in CONFIGURATION_NAMES}
