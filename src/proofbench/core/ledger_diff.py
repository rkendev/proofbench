"""INV-P2 in force: the committed code that turns two ledgers into a count.

``interfaces/ledger.py`` fixed the shape at PB-T1, before any result was known.
This is the implementation written against it, and it is the only place a
duplication or loss figure can come from. It is pure: given the same two ledgers
it returns the same diff, on any machine, with no clock, no filesystem, and no
client anywhere near it.

What the two words mean here, stated because a count is only checkable if its
definition is:

- **lost**: an expected record whose idempotency key appears nowhere in the
  observed ledger. The intended side effect never landed.
- **duplicated**: one entry per observed occurrence beyond the first for a given
  key, so ``len(diff.duplicated)`` is the number of excess side effects rather
  than the number of keys affected. A key seen three times contributes two. That
  is the number that matters when the step is charge_card, because it is the
  number of extra charges.

Anything the frozen ``LedgerDiff`` cannot express raises ``LedgerIntegrityError``
rather than being folded into one of those two buckets. There are exactly three
such conditions, and all three are apparatus defects rather than measurable
outcomes:

- an observed key that is not in the expected ledger at all
- a checksum mismatch on a key present in both
- a duplicate key inside the expected ledger

Raising is what INV-P2 requires here. The alternative is inventing a bucket, and
a count that quietly absorbed a corrupted payload into "clean" would be exactly
the eye-count the invariant exists to prevent. Failing loudly costs a run; failing
quietly costs the claim.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence

from proofbench.interfaces.ledger import LedgerDiff, SideEffectRecord


class LedgerIntegrityError(Exception):
    """The two ledgers cannot be compared as a duplication-and-loss diff.

    Distinct from a non-clean diff on purpose. A non-clean diff is a result, and
    under a kill run it may even be the expected one. This is the apparatus
    reporting that it cannot produce a result at all, which blocks the matrix
    rather than feeding it.
    """


class KeyedLedgerDiffer:
    """Compares two ledgers on the idempotency key. Satisfies ``LedgerDiffer``.

    Named for what it keys on, because that is the whole design. The key is
    derived from the saga and the step, so two records sharing one are the same
    intended effect applied twice, and every question this class answers reduces
    to counting keys and comparing the checksums behind them.
    """

    def diff(
        self,
        expected: Sequence[SideEffectRecord],
        observed: Sequence[SideEffectRecord],
    ) -> LedgerDiff:
        """Return the duplicated and lost records between the two ledgers."""
        expected_by_key: dict[str, SideEffectRecord] = {}
        for record in expected:
            if record.idempotency_key in expected_by_key:
                raise LedgerIntegrityError(
                    f"the expected ledger holds idempotency key "
                    f"{record.idempotency_key!r} more than once; the workload "
                    f"expansion is defective, so no count can be taken from it"
                )
            expected_by_key[record.idempotency_key] = record

        observed_counts = Counter(record.idempotency_key for record in observed)

        unexpected = sorted(set(observed_counts) - set(expected_by_key))
        if unexpected:
            raise LedgerIntegrityError(
                f"the observed ledger holds {len(unexpected)} key(s) the expected "
                f"ledger does not contain, starting with {unexpected[0]!r}; that is "
                f"neither a duplication nor a loss and the frozen LedgerDiff has no "
                f"slot for it, so it is reported as an apparatus defect"
            )

        for record in observed:
            expected_record = expected_by_key[record.idempotency_key]
            if record.payload_checksum != expected_record.payload_checksum:
                raise LedgerIntegrityError(
                    f"the observed record for {record.idempotency_key!r} carries "
                    f"checksum {record.payload_checksum} where the expected ledger "
                    f"says {expected_record.payload_checksum}; the side effect landed "
                    f"but its payload changed, which is neither a duplication nor a loss"
                )

        # Both sides are returned in expected order rather than in observation
        # order, so a diff read as evidence lists side effects in the order the
        # run was supposed to produce them and two runs are comparable line by line.
        lost = tuple(record for record in expected if observed_counts[record.idempotency_key] == 0)
        duplicated = tuple(
            record
            for record in expected
            for _ in range(max(observed_counts[record.idempotency_key] - 1, 0))
        )
        return LedgerDiff(duplicated=duplicated, lost=lost)
