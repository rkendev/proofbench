"""INV-P2: no count is ever taken by eye.

Every reported duplication or loss count comes from committed code diffing the sink
ledger against the expected saga ledger. This module is the interface that makes that
structural rather than a promise: a reported number has to come out of a
``LedgerDiffer``, and a ``LedgerDiffer`` consumes two ledgers and returns a
``LedgerDiff``. There is nowhere in that shape for a hand-counted figure to enter.

PB-T1 lands the interface only. There is deliberately no implementation here and no
measurement anywhere in this run: the harness, the broker, and the first count arrive
in later prompts. The types are frozen so that when an implementation does arrive, it
is written against a shape that was fixed before any result was known.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class SideEffectRecord:
    """One side effect of one saga step, as recorded in a ledger.

    ``idempotency_key`` is what makes duplication detectable: the key is derived
    from the saga and step, so two records sharing a key are the same intended
    effect applied twice. That is a double charge when the step is charge_card, and
    it is the reason the key is part of the record rather than an implementation
    detail of a sink.

    ``payload_checksum`` covers claim C3 (replay determinism). Comparing keys alone
    would call a replay identical even if it rebuilt a record with different
    contents, so the checksum is what lets a byte-identical rebuild be verified
    rather than assumed.

    Frozen because a ledger is evidence. A record that could be edited after the
    diff read it would make the count unreproducible, which is the failure mode
    INV-P2 exists to prevent.
    """

    idempotency_key: str
    saga_id: str
    step_name: str
    sequence: int
    payload_checksum: str


@dataclass(frozen=True, slots=True)
class LedgerDiff:
    """The outcome of one comparison: what was duplicated and what was lost.

    Both are the records themselves, not counts. A bare pair of integers would be a
    number with no way to check it; carrying the records means any reported figure
    can be traced back to the specific side effects behind it.
    """

    duplicated: tuple[SideEffectRecord, ...]
    lost: tuple[SideEffectRecord, ...]

    @property
    def is_clean(self) -> bool:
        """True when nothing was duplicated and nothing was lost.

        This is the shape of claim C1's pass condition and of the control run's
        required result. It is deliberately a property of the diff rather than a
        judgement made at the reporting layer.
        """
        return not self.duplicated and not self.lost


@runtime_checkable
class LedgerDiffer(Protocol):
    """Compares an observed sink ledger against the expected saga ledger.

    ``expected`` is what the saga stream should have produced, derived from the
    seeded schedule. ``observed`` is what the sink actually holds after the run,
    including whatever the injected fault did to it. Implementations must be pure
    with respect to their inputs: given the same two ledgers they must return the
    same diff, or a reported count would not be reproducible from committed
    evidence.
    """

    def diff(
        self,
        expected: Sequence[SideEffectRecord],
        observed: Sequence[SideEffectRecord],
    ) -> LedgerDiff:
        """Return the duplicated and lost records between the two ledgers."""
        ...
