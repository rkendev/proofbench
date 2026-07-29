"""Expand a run seed and the committed trace into the expected ledger.

This is the workload. CLAIMS.md models an LLM agent's tool-call side effects as a
saga of three steps (create_ticket, charge_card, send_confirmation), where a
duplicated side effect is a double charge and a lost one is a silently dropped
step. This module turns a run's seed plus the committed tool-call trace into the
ordered ledger of side effects that run is supposed to produce: 200 sagas of 3
steps, both from the frozen schedule, so 600 records per run.

Pure by construction. This module reads no file, writes no file, and imports no
client. The trace arrives as data, which is the boundary that matters: the
committed artifact is read once by the run driver, and the expansion itself
depends on nothing but its arguments. Determinism is not delegated to ``random``;
every derived value is integer arithmetic on a SHA-256 digest.

Two things here exist for claims that are not measured until later, and both are
cheaper to build now than to retrofit:

- ``idempotency_key`` is what makes duplication detectable at all. It is derived
  from the saga and the step, so two records sharing a key are the same intended
  effect applied twice.
- ``payload_checksum`` covers the payload as it goes on the wire, using the same
  canonical encoding the producer sends. Claim C3 asks whether a replay rebuilds
  the sink byte-identically, and comparing keys alone would call a replay
  identical even if it rebuilt a record with different contents.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from proofbench.config import Settings
from proofbench.core.trace import AgentTrace
from proofbench.interfaces.ledger import SideEffectRecord


def canonical_bytes(payload: dict[str, Any]) -> bytes:
    """Return the one encoding of a payload that anything is allowed to use.

    The checksum covers exactly the bytes the producer puts on the wire, so the
    two cannot disagree. A separate "encode for sending" path would let a
    replay's checksum match while its bytes differed, which is the failure claim
    C3 exists to detect and which would therefore be invisible to it.

    ``sort_keys`` makes the encoding independent of dict insertion order, and the
    compact separators keep the wire form stable against a formatting change.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def payload_checksum(payload: dict[str, Any]) -> str:
    """Return the SHA-256 of a payload's canonical encoding, as lowercase hex."""
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()


def saga_id_for(run_seed: str, saga_index: int) -> str:
    """Return the identifier for one saga of one run.

    The run seed is part of it, so a key from run 3 can never collide with a key
    from run 7 even though both runs expand the same 200 saga indices. Zero
    padded so the natural string order matches the stream order, which makes an
    evidence file readable without sorting it first.
    """
    return f"{run_seed}-{saga_index:04d}"


def idempotency_key_for(saga_id: str, step_name: str) -> str:
    """Return the idempotency key for one step of one saga.

    Readable rather than hashed, deliberately. INV-P2's point is that a reported
    figure can be traced back to the specific side effects behind it, and a
    reader who opens a diff should be able to see which saga and which step a
    duplicate or a loss refers to without decoding anything.
    """
    return f"{saga_id}:{step_name}"


def variant_digest(run_seed: str, saga_index: int, step_name: str) -> bytes:
    """Return the digest that selects which tool-call template this step uses.

    Keyed by the run seed as well as the position, so two runs draw different
    payloads from the same committed pool. Derived independently per step, so
    inserting a step or resizing the pool cannot silently shift every later
    selection.
    """
    return hashlib.sha256(f"{run_seed}:saga:{saga_index}:{step_name}".encode()).digest()


@dataclass(frozen=True, slots=True)
class SagaStep:
    """One step of one saga: the ledger record and the payload behind it.

    The record is what the differ compares. The payload is what the producer
    sends. They are carried together because separating them is how a checksum
    stops describing the bytes it is supposed to cover.
    """

    record: SideEffectRecord
    payload: dict[str, Any]

    def payload_bytes(self) -> bytes:
        """Return the exact bytes to put on the wire for this step."""
        return canonical_bytes(self.payload)


@dataclass(frozen=True, slots=True)
class Saga:
    """One saga: the unit the good configuration wraps in a single transaction.

    ``transaction_boundary`` is frozen at per_saga, so this is the granularity at
    which the run driver begins, commits, or aborts. It is also the granularity
    of the restart and resume contract in ADR-0003: a restart resumes at the
    first saga not known to be durably complete, so a saga is the unit of replay
    as well as the unit of atomicity.
    """

    saga_index: int
    saga_id: str
    steps: tuple[SagaStep, ...]

    @property
    def records(self) -> tuple[SideEffectRecord, ...]:
        """The ledger records this saga is supposed to produce, in order."""
        return tuple(step.record for step in self.steps)


def expand_sagas(run_seed: str, settings: Settings, trace: AgentTrace) -> tuple[Saga, ...]:
    """Expand one run's seed and the committed trace into its ordered sagas.

    Deterministic in its three arguments and nothing else. The same seed, the
    same frozen constants, and the same committed trace produce the same sagas on
    any machine and any Python version.
    """
    sagas: list[Saga] = []
    for saga_index in range(settings.sagas_per_run):
        saga_id = saga_id_for(run_seed, saga_index)
        steps: list[SagaStep] = []

        for step_index, step_name in enumerate(settings.saga_step_names):
            variants = trace.variants_for(step_name)
            digest = variant_digest(run_seed, saga_index, step_name)
            template = variants[int.from_bytes(digest[:8], "big") % len(variants)]

            key = idempotency_key_for(saga_id, step_name)
            # The key travels inside the payload as well as on the message key, so
            # a sink topic is self-describing: a reader can tell what a record is
            # without depending on how the harness happened to set the Kafka key.
            payload: dict[str, Any] = {
                "idempotency_key": key,
                "saga_id": saga_id,
                "saga_index": saga_index,
                "step_index": step_index,
                "step_name": step_name,
                "tool": template.tool,
                "call_id": template.call_id,
                "arguments": dict(template.arguments),
            }

            steps.append(
                SagaStep(
                    record=SideEffectRecord(
                        idempotency_key=key,
                        saga_id=saga_id,
                        step_name=step_name,
                        sequence=saga_index * settings.steps_per_saga + step_index,
                        payload_checksum=payload_checksum(payload),
                    ),
                    payload=payload,
                )
            )

        sagas.append(Saga(saga_index=saga_index, saga_id=saga_id, steps=tuple(steps)))

    return tuple(sagas)


def expected_ledger(sagas: tuple[Saga, ...]) -> tuple[SideEffectRecord, ...]:
    """Flatten sagas into the ordered expected ledger the differ consumes."""
    return tuple(step.record for saga in sagas for step in saga.steps)


def observed_record(payload: dict[str, Any], steps_per_saga: int) -> SideEffectRecord:
    """Rebuild a ledger record from a payload read back off a sink topic.

    The checksum is recomputed from the payload rather than trusted from a field
    inside it. A record that carried its own checksum would agree with itself no
    matter what happened to the bytes in between, which would make the integrity
    half of the diff decorative.

    ``sequence`` is likewise recomputed from the saga and step indices rather than
    read from a field, so the observed record's ordering information comes from
    the same arithmetic the expansion used. The step count comes from the schedule
    entry, which is the authority for the shape of the run; a payload does not
    carry it, because it is a property of the run rather than of a record.
    """
    return SideEffectRecord(
        idempotency_key=str(payload["idempotency_key"]),
        saga_id=str(payload["saga_id"]),
        step_name=str(payload["step_name"]),
        sequence=int(payload["saga_index"]) * steps_per_saga + int(payload["step_index"]),
        payload_checksum=payload_checksum(payload),
    )
