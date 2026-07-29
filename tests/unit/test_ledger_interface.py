"""INV-P2: the ledger interface has the shape a checkable count requires.

PB-T1 lands the interface only, so this pins shape, immutability, and protocol
conformance. It deliberately computes no diff and measures nothing: there is no
implementation to test yet, and a test that quietly invented one would be the first
count taken outside committed harness code.
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from proofbench.interfaces.ledger import LedgerDiff, LedgerDiffer, SideEffectRecord


def _record(key: str = "saga-1:charge_card") -> SideEffectRecord:
    return SideEffectRecord(
        idempotency_key=key,
        saga_id="saga-1",
        step_name="charge_card",
        sequence=1,
        payload_checksum="0" * 64,
    )


def test_a_side_effect_record_is_keyed_and_immutable() -> None:
    """A ledger is evidence, so a record must not be editable after the fact.

    If a record could be mutated after the diff read it, the reported count would
    not be reproducible from the committed ledger, which is exactly what INV-P2
    exists to prevent.
    """
    record = _record()
    assert record.idempotency_key == "saga-1:charge_card"
    with pytest.raises(AttributeError):
        record.sequence = 2  # type: ignore[misc]


def test_records_with_the_same_key_compare_equal() -> None:
    """Duplication detection rests on the key, so equality has to follow it.

    Two records carrying the same key are the same intended effect applied twice,
    which is a double charge when the step is charge_card.
    """
    assert _record() == _record()
    assert _record("saga-2:charge_card") != _record()


def test_a_clean_diff_is_the_pass_condition_for_c1_and_the_control() -> None:
    empty = LedgerDiff(duplicated=(), lost=())
    assert empty.is_clean

    duplicated = LedgerDiff(duplicated=(_record(),), lost=())
    lost = LedgerDiff(duplicated=(), lost=(_record(),))
    assert not duplicated.is_clean
    assert not lost.is_clean


def test_a_diff_carries_the_records_not_just_counts() -> None:
    """A bare integer is a number with no way to check it.

    Carrying the records means any reported figure can be traced back to the
    specific side effects behind it, which is what makes the count auditable.
    """
    record = _record()
    diff = LedgerDiff(duplicated=(record,), lost=())
    assert diff.duplicated[0] is record
    with pytest.raises(AttributeError):
        diff.duplicated = ()  # type: ignore[misc]


def test_the_differ_protocol_is_satisfiable_and_discriminating() -> None:
    """The protocol has to accept a correct shape and reject a wrong one.

    A runtime-checkable Protocol that accepted anything would document nothing. No
    diff is computed here: the stub returns a fixed empty result purely to prove the
    signature is implementable.
    """

    class Stub:
        def diff(
            self,
            expected: Sequence[SideEffectRecord],
            observed: Sequence[SideEffectRecord],
        ) -> LedgerDiff:
            return LedgerDiff(duplicated=(), lost=())

    class NotADiffer:
        pass

    stub: LedgerDiffer = Stub()
    assert isinstance(stub, LedgerDiffer)
    assert not isinstance(NotADiffer(), LedgerDiffer)
