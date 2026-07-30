"""C3, replay determinism: rebuilding a sink from the event log and checksumming it.

CLAIMS.md states C3 as "replaying the full event log through the committed consumer
rebuilds the sink byte-identical to the original run, verified by checksum". The
literal reading is impossible and the ADR-0004 fork settles what is measured instead.

**Raw topic bytes cannot match, and that is not a defect in the replay.** A replay gets
different offsets, different timestamps and a different producer epoch, all of which are
broker-assigned rather than produced by the consumer under test. Comparing them would
fail for reasons that have nothing to do with determinism, so a checksum over the
partition would be a checksum over the broker's bookkeeping.

So the comparison is over the **effect log**: the rebuilt ``SideEffectRecord`` ledger,
which is the faithful reading of "the sink" for a harness whose entire subject is side
effects. ``evidence.canonical_ledger_payload`` fixes the form: ordered by ``(sequence,
idempotency_key)`` so arrival order cannot make two identical logs disagree, and with
every occurrence kept so a duplicated side effect stays duplicated. Collapsing
duplicates would make C3 blind to precisely the thing C1 counts.

**Which runs are replayed**, fixed before any result: every good-configuration run that
reached a scoreable status, up to all 21. The evaluator names the denominator and every
run excluded from it, so nobody has to reverse-engineer why 21 became 19.

**The replay uses its own consumer group**, derived from the run's scope. Without that
it would resume at the original run's committed offset, which sits at the end of the
input topic, and read nothing at all: a replay that read nothing would rebuild an empty
ledger, and an empty ledger compared against an empty ledger is a C3 pass that means
nothing. ``group.id`` is already an identity key, so this needs no allow-list change.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from proofbench.config import Settings
from proofbench.core.configs import RunConfiguration, RunTopics
from proofbench.core.evidence import ledger_checksum
from proofbench.core.recovery import ApparatusFailure
from proofbench.interfaces.ledger import SideEffectRecord

REPLAY_SCOPE = "replay"


@dataclass(frozen=True, slots=True)
class ReplayOutcome:
    """What one replay rebuilt, and whether it matched."""

    run_id: int
    configuration: str
    original_checksum: str
    replayed_checksum: str
    original_records: int
    replayed_records: int
    sink: str

    @property
    def matched(self) -> bool:
        return self.original_checksum == self.replayed_checksum

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "configuration": self.configuration,
            "sink": self.sink,
            "original_checksum": self.original_checksum,
            "replayed_checksum": self.replayed_checksum,
            "original_records": self.original_records,
            "replayed_records": self.replayed_records,
            "matched": self.matched,
        }


def replay_topics(configuration: RunConfiguration) -> RunTopics:
    """The sink topics a replay writes into: fresh, and never the originals.

    Writing into the original sinks would append to what is already there and the
    comparison would be against a doubled ledger. The input topic is shared, because
    replaying the same event log is the whole point.
    """
    return RunTopics(
        input=configuration.topics.input,
        sink_a=f"{configuration.topics.sink_a}.{REPLAY_SCOPE}",
        sink_b=f"{configuration.topics.sink_b}.{REPLAY_SCOPE}",
    )


def replay_configuration(configuration: RunConfiguration, settings: Settings) -> RunConfiguration:
    """The same configuration, pointed at fresh sinks and a fresh consumer group.

    Everything else is identical, which is what makes this a replay through the
    committed consumer rather than through a second implementation of it. C3 asks
    whether the consumer is deterministic; handing the replay a different consumer
    would answer a different question.
    """
    from types import MappingProxyType

    def regrouped(section: Any, suffix: str) -> Any:
        conf = dict(section)
        conf["group.id"] = f"{conf['group.id']}.{REPLAY_SCOPE}.{suffix}"
        return MappingProxyType(conf)

    return RunConfiguration(
        name=configuration.name,
        topics=replay_topics(configuration),
        ingest_producer=configuration.ingest_producer,
        sink_producer=configuration.sink_producer,
        consumer=regrouped(configuration.consumer, "process"),
        verifier=regrouped(configuration.verifier, "verify"),
        offset_commit=configuration.offset_commit,
        transaction_boundary=configuration.transaction_boundary,
    )


def compare(
    original: tuple[SideEffectRecord, ...],
    replayed: tuple[SideEffectRecord, ...],
    run_id: int,
    configuration: str,
    sink: str,
) -> ReplayOutcome:
    """Checksum both ledgers in the canonical form and report whether they agree.

    Refuses to compare against an empty original, because an empty ledger and an empty
    replay produce the same checksum and would report a match. That is the C3 shape of
    the vacuous-guard problem: a replay that read nothing would pass.
    """
    if not original:
        raise ApparatusFailure(
            f"run {run_id} under {configuration} has an empty original ledger for {sink}, "
            f"so a replay that also read nothing would checksum identically and report a "
            f"match. C3 cannot be evaluated against nothing."
        )
    return ReplayOutcome(
        run_id=run_id,
        configuration=configuration,
        sink=sink,
        original_checksum=ledger_checksum(original),
        replayed_checksum=ledger_checksum(replayed),
        original_records=len(original),
        replayed_records=len(replayed),
    )
