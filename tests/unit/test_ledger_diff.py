"""The differ counts what it says it counts, on hand-built ledgers.

Hand-built on purpose. Every other test in this repository derives its inputs
from the frozen constants, but a differ tested only against the real expansion
would be checked against ledgers that are always clean, and the cases that matter
are the ones a clean run never produces. These ledgers are small enough to read,
so a reader can confirm the expected count by eye here without any count ever
being taken by eye in the harness.

Cases covered, per the acceptance gate: clean, one duplicate, one loss, several of
each, and an empty observed ledger. Plus the three integrity conditions that raise
rather than being folded into a bucket.
"""

from __future__ import annotations

import pytest

from proofbench.core.ledger_diff import KeyedLedgerDiffer, LedgerIntegrityError
from proofbench.interfaces.ledger import LedgerDiff, LedgerDiffer, SideEffectRecord


def record(key: str, checksum: str = "a" * 64, sequence: int = 0) -> SideEffectRecord:
    """One record. The key is what the differ compares; the rest is context."""
    saga_id, step_name = key.split(":", 1)
    return SideEffectRecord(
        idempotency_key=key,
        saga_id=saga_id,
        step_name=step_name,
        sequence=sequence,
        payload_checksum=checksum,
    )


def ledger(*keys: str) -> tuple[SideEffectRecord, ...]:
    return tuple(record(key, sequence=index) for index, key in enumerate(keys))


EXPECTED = ledger("s1:create_ticket", "s1:charge_card", "s1:send_confirmation")


@pytest.fixture
def differ() -> KeyedLedgerDiffer:
    return KeyedLedgerDiffer()


def test_it_satisfies_the_frozen_protocol(differ: KeyedLedgerDiffer) -> None:
    """The interface was frozen at PB-T1, before any result was known.

    An implementation that had drifted from it would mean a count coming out of
    a shape nobody pre-registered.
    """
    assert isinstance(differ, LedgerDiffer)


def test_a_clean_run_is_clean(differ: KeyedLedgerDiffer) -> None:
    """C1's pass condition and the control run's required result."""
    result = differ.diff(EXPECTED, EXPECTED)
    assert result == LedgerDiff(duplicated=(), lost=())
    assert result.is_clean


def test_observation_order_does_not_matter(differ: KeyedLedgerDiffer) -> None:
    """A sink topic is read in offset order, which need not be stream order.

    If order mattered, a partitioned or reordered sink would report losses and
    duplicates that never happened.
    """
    shuffled = (EXPECTED[2], EXPECTED[0], EXPECTED[1])
    assert differ.diff(EXPECTED, shuffled).is_clean


def test_one_duplicate(differ: KeyedLedgerDiffer) -> None:
    observed = (*EXPECTED, record("s1:charge_card"))
    result = differ.diff(EXPECTED, observed)

    assert not result.is_clean
    assert result.lost == ()
    assert len(result.duplicated) == 1
    assert result.duplicated[0].idempotency_key == "s1:charge_card"


def test_one_loss(differ: KeyedLedgerDiffer) -> None:
    observed = (EXPECTED[0], EXPECTED[2])
    result = differ.diff(EXPECTED, observed)

    assert not result.is_clean
    assert result.duplicated == ()
    assert [r.idempotency_key for r in result.lost] == ["s1:charge_card"]


def test_several_of_each(differ: KeyedLedgerDiffer) -> None:
    """Duplication and loss are independent and are counted independently."""
    expected = ledger(
        "s1:create_ticket",
        "s1:charge_card",
        "s1:send_confirmation",
        "s2:create_ticket",
        "s2:charge_card",
    )
    observed = (
        expected[0],
        expected[0],  # duplicate 1
        expected[1],
        expected[1],
        expected[1],  # two excess occurrences, so duplicates 2 and 3
        # s1:send_confirmation and s2:create_ticket never landed
        expected[4],
    )
    result = differ.diff(expected, observed)

    assert [r.idempotency_key for r in result.duplicated] == [
        "s1:create_ticket",
        "s1:charge_card",
        "s1:charge_card",
    ]
    assert [r.idempotency_key for r in result.lost] == [
        "s1:send_confirmation",
        "s2:create_ticket",
    ]


def test_a_key_seen_three_times_counts_as_two_excess_effects(
    differ: KeyedLedgerDiffer,
) -> None:
    """The count is excess side effects, not keys affected.

    When the step is charge_card that distinction is the difference between one
    extra charge and two, so it is pinned rather than left to the reader.
    """
    observed = (*EXPECTED, EXPECTED[1], EXPECTED[1])
    assert len(differ.diff(EXPECTED, observed).duplicated) == 2


def test_an_empty_observed_ledger_is_total_loss(differ: KeyedLedgerDiffer) -> None:
    """The shape a run reports when the sink topic was never written at all.

    Worth its own case because an empty input is where a fold or a comprehension
    silently returns "clean", which would read as a passing run.
    """
    result = differ.diff(EXPECTED, ())
    assert result.duplicated == ()
    assert len(result.lost) == len(EXPECTED)
    assert not result.is_clean


def test_two_empty_ledgers_are_clean_but_that_is_not_a_passing_run(
    differ: KeyedLedgerDiffer,
) -> None:
    """Documented rather than guarded, because the guard belongs upstream.

    Nothing expected and nothing observed is genuinely a clean diff. What makes
    it not a result is that the run produced no expectation, which the run driver
    catches by asserting the expected ledger has the frozen length. Pinning it
    here records that the differ is not where that check lives.
    """
    assert differ.diff((), ()).is_clean


def test_the_diff_carries_the_records_so_a_figure_can_be_traced(
    differ: KeyedLedgerDiffer,
) -> None:
    """INV-P2: a bare integer is a number with no way to check it."""
    observed = (EXPECTED[0], EXPECTED[0])
    result = differ.diff(EXPECTED, observed)

    assert result.duplicated[0].saga_id == "s1"
    assert result.duplicated[0].step_name == "create_ticket"
    assert {r.step_name for r in result.lost} == {"charge_card", "send_confirmation"}


def test_an_unexpected_key_raises_rather_than_being_counted(
    differ: KeyedLedgerDiffer,
) -> None:
    """It is neither a duplication nor a loss, and the frozen shape has no slot.

    Folding it into either bucket would be inventing a count, which is the thing
    INV-P2 forbids.
    """
    observed = (*EXPECTED, record("s9:charge_card"))
    with pytest.raises(LedgerIntegrityError, match="the expected ledger does not contain"):
        differ.diff(EXPECTED, observed)


def test_a_checksum_mismatch_raises(differ: KeyedLedgerDiffer) -> None:
    """The side effect landed but its payload changed.

    Also neither bucket. Counting it as clean would let a corrupted replay pass
    claim C3 by never being looked at.
    """
    observed = (EXPECTED[0], record("s1:charge_card", checksum="b" * 64), EXPECTED[2])
    with pytest.raises(LedgerIntegrityError, match="carries checksum"):
        differ.diff(EXPECTED, observed)


def test_a_duplicate_key_in_the_expected_ledger_raises(differ: KeyedLedgerDiffer) -> None:
    """The workload expansion is defective, so no count can be taken from it.

    Caught here as well as in the saga tests because this is the last point
    before a number would be reported.
    """
    broken = (*EXPECTED, record("s1:charge_card"))
    with pytest.raises(LedgerIntegrityError, match="more than once"):
        differ.diff(broken, EXPECTED)


def test_the_differ_is_pure(differ: KeyedLedgerDiffer) -> None:
    """Same inputs, same diff, and the inputs come back untouched.

    A reported count has to be reproducible from committed evidence, which it is
    not if running the diff twice can disagree or if it mutated what it read.
    """
    observed = (EXPECTED[0], EXPECTED[0], EXPECTED[1])
    expected_before = tuple(EXPECTED)
    observed_before = tuple(observed)

    first = differ.diff(EXPECTED, observed)
    second = differ.diff(EXPECTED, observed)

    assert first == second
    assert expected_before == EXPECTED
    assert observed == observed_before
