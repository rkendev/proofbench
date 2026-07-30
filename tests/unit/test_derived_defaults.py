"""INV-P3 below the level the allow-list gate can see.

tests/unit/test_configs_allowlist.py compares the dictionaries core/configs.py
writes. That is the right comparison for anything the harness sets, and it is blind
to a difference the **client** creates from a setting that is on the allow-list.

One such difference exists, and it was live in PB-T2. librdkafka caps
``message.timeout.ms`` at ``transaction.timeout.ms``. With the property unset, the
good configuration's producers therefore ran a 60s delivery deadline, because they
carry a ``transactional.id`` and the pinned transaction timeout is 60000, while the
baseline's ran librdkafka's 300s default. A five-fold difference in how long a send
may hang before being reported as failed, on a property that is not allow-listed,
produced indirectly by one that is.

It was invisible twice over: ``resolved_config.json`` records only explicitly-set
values, and the allow-list gate compares maps the harness authored. C2's floor is
only about delivery configuration if the delivery configuration is the only thing
that differs, so a leak at this level is exactly as damaging as one at the level
above and considerably harder to notice.

**What this gate is, and what it honestly is not.** It would be better to read each
client's effective configuration back after librdkafka has applied its defaults and
caps, and compare those. The pinned client cannot do that: ``debug=conf`` emits only
the properties that were explicitly set, plus internal callback pointers, and
``message.timeout.ms`` never appears in it even when it has been capped. There is no
effective-config dump in the Python binding. So this gate does the reachable thing
instead: it enumerates each known derivation, asserts the dependent property is set
identically in both configurations, and **proves the derivation is real against the
pinned client** rather than trusting the enumeration. That last part is what makes it
more than a list somebody wrote.

The bound is stated rather than implied: this covers the derivations that are known
and demonstrable. It cannot be exhaustive while the client exposes no effective
configuration, and ADR-0004 records that.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest

from proofbench.config import Settings
from proofbench.core.configs import ALLOW_LIST, BASELINE, GOOD, RunConfiguration, build_both
from proofbench.core.txn import PINNED_TRANSACTION_TIMEOUT_MS

RUN_ID = 0
BOOTSTRAP = "broker-placeholder:1"

# librdkafka logs on a background thread. Discarding its output keeps the rejected
# constructions below from filling the test report; it does not affect what is checked.
_SILENT = logging.getLogger("proofbench.tests.silent_derived")
_SILENT.addHandler(logging.NullHandler())
_SILENT.propagate = False


@dataclass(frozen=True)
class Derivation:
    """One property the client derives or caps from an allow-listed property."""

    # The property whose effective value the client decides.
    dependent: str
    # The allow-listed property whose presence or value triggers the derivation.
    trigger: str
    # Which client role maps carry it. Producers and consumers derive differently.
    section: str
    # Human-readable statement of the relation, for the failure message.
    relation: str


# Every derivation known to affect these two configurations. Adding one is a
# deliberate, visible act, exactly as widening the allow-list is.
DERIVATIONS: tuple[Derivation, ...] = (
    Derivation(
        dependent="message.timeout.ms",
        trigger="transactional.id",
        section="producer",
        relation="librdkafka caps message.timeout.ms at transaction.timeout.ms, which "
        "the client pin fixes at 60000, so a transactional producer silently gets a "
        "60s delivery deadline where a non-transactional one gets librdkafka's 300s",
    ),
)

PRODUCER_SECTIONS = ("ingest_producer", "sink_producer")


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(_env_file=None, broker_bootstrap_servers=BOOTSTRAP)


@pytest.fixture(scope="module")
def configurations(settings: Settings) -> dict[str, RunConfiguration]:
    return build_both(RUN_ID, settings)


def _producer(extra: dict[str, object]) -> object:
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": BOOTSTRAP, **extra}, logger=_SILENT)


# --------------------------------------------------------------------------
# The derivation is real, proven against the pin rather than asserted
# --------------------------------------------------------------------------


def test_the_client_really_does_cap_message_timeout_at_the_transaction_timeout() -> None:
    """The probe that makes this gate evidence rather than a list.

    Without it, DERIVATIONS would be a claim about librdkafka that nobody checked,
    and a client upgrade that dropped the cap would leave the gate asserting a
    relationship that no longer existed. The check is the client's own validation:
    it refuses at construction, offline, with no broker involved.
    """
    from confluent_kafka import KafkaException

    # One millisecond over the pinned transaction timeout is refused outright, and
    # the refusal names the relation.
    with pytest.raises((KafkaException, ValueError)) as caught:
        _producer(
            {
                "transactional.id": "proofbench.derived.probe",
                "message.timeout.ms": PINNED_TRANSACTION_TIMEOUT_MS + 1,
            }
        )
    assert "message.timeout.ms" in str(caught.value)
    assert "transaction.timeout.ms" in str(caught.value)

    # Exactly the pinned value is accepted, which is what fixes the number at 60000
    # rather than at whatever this file happens to say.
    _producer(
        {
            "transactional.id": "proofbench.derived.probe",
            "message.timeout.ms": PINNED_TRANSACTION_TIMEOUT_MS,
        }
    )

    # And without a transactional.id the cap does not apply, which is the asymmetry
    # that made the leak possible in the first place.
    _producer({"message.timeout.ms": PINNED_TRANSACTION_TIMEOUT_MS * 5})


def test_the_pinned_transaction_timeout_is_what_the_constant_records() -> None:
    """The constant is an observation of the pin, so the pin decides its value.

    ADR-0003 section 8 leaves transaction.timeout.ms to the client pin, and the
    harness never sets it. That makes the number an input this repository does not
    own, and ADR-0004's combined open-transaction bound depends on it, so it is
    measured here instead of trusted.
    """
    from confluent_kafka import KafkaException

    _producer({"transactional.id": "p", "message.timeout.ms": PINNED_TRANSACTION_TIMEOUT_MS})
    with pytest.raises((KafkaException, ValueError)):
        _producer(
            {"transactional.id": "p", "message.timeout.ms": PINNED_TRANSACTION_TIMEOUT_MS + 1}
        )


# --------------------------------------------------------------------------
# Every known derivation is neutralised by an explicit, identical setting
# --------------------------------------------------------------------------


@pytest.mark.parametrize("derivation", DERIVATIONS, ids=lambda d: d.dependent)
def test_a_derived_property_is_set_explicitly_and_identically_in_both(
    derivation: Derivation, configurations: dict[str, RunConfiguration]
) -> None:
    """The gate. Proven red by removing the explicit setting from _producer_base.

    Present in both and equal in both. Present matters because an unset property is
    where the client's own derivation takes over; equal matters because that is the
    INV-P3 rule itself.
    """
    good, baseline = configurations[GOOD], configurations[BASELINE]
    for section in PRODUCER_SECTIONS:
        good_conf = good.client_sections()[section]
        baseline_conf = baseline.client_sections()[section]

        assert derivation.dependent in good_conf, (
            f"{section}: {derivation.dependent} is unset in the good configuration, so "
            f"the client decides it. {derivation.relation}"
        )
        assert derivation.dependent in baseline_conf, (
            f"{section}: {derivation.dependent} is unset in the baseline, so the client "
            f"decides it. {derivation.relation}"
        )
        assert good_conf[derivation.dependent] == baseline_conf[derivation.dependent], (
            f"{section}: {derivation.dependent} differs between the configurations "
            f"({good_conf[derivation.dependent]} vs "
            f"{baseline_conf[derivation.dependent]}), which is an INV-P3 difference "
            f"nobody pre-registered"
        )


def test_the_trigger_of_each_derivation_is_itself_allow_listed() -> None:
    """A derivation only matters when its trigger is a permitted difference.

    If the trigger were the same in both configurations there would be nothing to
    derive differently, so an entry here whose trigger is not allow-listed would be
    describing a hazard that cannot arise, and would eventually be deleted as noise
    by someone who did not check.
    """
    for derivation in DERIVATIONS:
        assert derivation.trigger in ALLOW_LIST, (
            f"{derivation.dependent} is listed as derived from {derivation.trigger}, "
            f"which is not on the INV-P3 allow-list, so the two configurations cannot "
            f"differ on it and nothing can be derived differently from it"
        )


def test_the_trigger_really_does_differ_between_the_configurations(
    configurations: dict[str, RunConfiguration],
) -> None:
    """And that it differs in the direction that makes the derivation bite.

    transactional.id is present in the good configuration and absent from the
    baseline, which is precisely the asymmetry that produced two different delivery
    deadlines from one unset property.
    """
    good, baseline = configurations[GOOD], configurations[BASELINE]
    for section in PRODUCER_SECTIONS:
        assert "transactional.id" in good.client_sections()[section]
        assert "transactional.id" not in baseline.client_sections()[section]


def test_the_explicit_value_sits_below_the_cap(settings: Settings) -> None:
    """Otherwise the transactional producer could not be constructed at all.

    The value has to be acceptable to both a transactional and a non-transactional
    producer, or setting it identically would be impossible and the leak would have
    to be closed some other way.
    """
    assert settings.producer_message_timeout_ms <= PINNED_TRANSACTION_TIMEOUT_MS
    _producer(
        {
            "transactional.id": "proofbench.derived.probe",
            "message.timeout.ms": settings.producer_message_timeout_ms,
        }
    )
    _producer({"message.timeout.ms": settings.producer_message_timeout_ms})


def test_the_shared_consumer_session_timeout_is_shared(
    settings: Settings, configurations: dict[str, RunConfiguration]
) -> None:
    """Added for wall clock, so it must not become a difference between the two.

    A SIGKILLed consumer does not leave its group, so a restarted subscribe waits
    out the dead member's session. Shortening that is worth roughly ten minutes
    across the matrix, and it is only free if both configurations carry it.
    """
    good, baseline = configurations[GOOD], configurations[BASELINE]
    for section in ("consumer", "verifier"):
        assert (
            good.client_sections()[section]["session.timeout.ms"]
            == baseline.client_sections()[section]["session.timeout.ms"]
            == settings.consumer_session_timeout_ms
        )


def test_the_broker_accepts_the_shortened_session_timeout(settings: Settings) -> None:
    """6000 is the broker's group.min.session.timeout.ms floor, checked offline.

    A value below the floor is rejected by the coordinator at join time rather than
    by the client at construction, which would surface as every process restart
    failing in the middle of the matrix rather than as a failed build.
    """
    from confluent_kafka import Consumer

    assert settings.consumer_session_timeout_ms >= 6000
    Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "proofbench.derived.probe",
            "session.timeout.ms": settings.consumer_session_timeout_ms,
        },
        logger=_SILENT,
    )


def test_the_enumeration_is_not_empty() -> None:
    """The parametrised rule above would pass vacuously with an empty list.

    A gate that checks nothing looks exactly like a gate that finds nothing, and
    this file exists because one real derivation was already missed.
    """
    assert DERIVATIONS, "the derived-default gate has nothing to check"
