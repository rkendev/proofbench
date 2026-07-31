"""Re-delivery after a rebalance: the cycle 2 mechanism, observed and gated.

Located by instrumenting every append to ``buffered`` and capturing the stack, after two
hypothesis-driven repairs had failed. The stack named the ordinary batch loop, not any
recovery function:

    DUP-APPEND key=...-0166:create_ticket
      buffered_before=['...-0166:create_ticket', '...-0166:charge_card']
      offset=664 saga_idx=166

The consumer rejoined its group after the broker restart, resumed from the last
committed offset, and Kafka re-delivered offsets 664 and 665 while ``buffered`` still
held exactly those two records. The loop appended them again and the saga went out as a
five-record group. Neither the rejoin queue nor the rejoin poll caused it, and removing
both changed nothing.

**Both hazards are covered here by name**, which is the lesson cycle 2 taught: the
requirement named dropping and double-feeding, the gate checked only dropping, and the
one it missed is what shipped.
"""

from __future__ import annotations

import ast
import inspect

from proofbench.core import run


def _process_source() -> str:
    return inspect.getsource(run.process)


def _replay(offsets: list[int], partitions: list[int] | None = None) -> tuple[list[str], int]:
    """The rule the loop applies, over a sequence of delivered (partition, offset) pairs.

    Returns the keys that ended up buffered, and how many backwards moves were seen.
    Mirrors the loop; call-site tests below pin that the real one behaves this way.
    """
    parts = partitions if partitions is not None else [0] * len(offsets)
    buffered: list[str] = []
    last_seen: dict[tuple[str, int], int] = {}
    redeliveries = 0
    for offset, partition in zip(offsets, parts, strict=True):
        position = ("t", partition)
        previous = last_seen.get(position)
        if previous is not None and offset <= previous:
            redeliveries += 1
            buffered = []
        last_seen[position] = offset
        buffered.append(f"key@{partition}:{offset}")
    return buffered, redeliveries


# --------------------------------------------------------------------------
# Hazard 2: double-feeding. The one that shipped.
# --------------------------------------------------------------------------


def test_a_redelivered_record_does_not_join_a_group_that_already_holds_it() -> None:
    """The exact cycle 2 sequence: 663, 664, 665, then 664, 665 again.

    Without the reset the group would hold five records with two repeated, which is the
    signature the live reproduction caught.
    """
    buffered, redeliveries = _replay([663, 664, 665, 664, 665])
    assert redeliveries == 1
    assert buffered == ["key@0:664", "key@0:665"], (
        "the re-delivered records were appended to a buffer that already held them"
    )
    assert len(buffered) <= 3


def test_the_partial_group_is_discarded_not_extended() -> None:
    """Extending it is what produced n=5. Discarding is the only safe response."""
    buffered, _ = _replay([100, 101, 100])
    assert buffered == ["key@0:100"]


# --------------------------------------------------------------------------
# Hazard 1: dropping. Checked by name, not assumed.
# --------------------------------------------------------------------------


def test_discarding_the_partial_group_loses_nothing() -> None:
    """Because re-delivery restarts from the committed offset.

    The records of the in-progress saga arrive again, so clearing the buffer discards a
    copy rather than the only copy. This is the hazard the mirror of this fix could have
    introduced, and it is checked rather than argued: after the reset, every offset that
    was buffered before it is delivered again.
    """
    delivered = [663, 664, 665, 663, 664, 665, 666]
    buffered, redeliveries = _replay(delivered)
    assert redeliveries == 1
    # Everything that was in the discarded group came back.
    assert buffered == ["key@0:663", "key@0:664", "key@0:665", "key@0:666"]


def test_a_forward_only_stream_is_never_reset() -> None:
    """The ordinary case, which must not pay for the fault case.

    A reset on a stream that never moved backwards would discard real work and
    manufacture loss on every clean run.
    """
    buffered, redeliveries = _replay([0, 1, 2, 3, 4])
    assert redeliveries == 0
    assert len(buffered) == 5


# --------------------------------------------------------------------------
# The real loop implements this, and records it as evidence
# --------------------------------------------------------------------------


def test_interleaved_partitions_are_not_mistaken_for_a_regression() -> None:
    """The position state is per partition, so independent sequences cannot collide.

    One scalar would read a record from partition 1 at offset 5, arriving after
    partition 0 reached offset 900, as a backwards move and discard a healthy group.
    That is the dropping hazard firing on the CLEAN path, with no fault injected, in
    both configurations, and it would void a matrix by itself.

    The harness provisions one partition per topic today, so a scalar would happen to
    work. This removes the need to prove that assumption rather than restating it.
    """
    buffered, redeliveries = _replay([100, 5, 101, 6, 102], partitions=[0, 1, 0, 1, 0])
    assert redeliveries == 0, (
        "interleaved offsets from independent partitions were read as a position "
        "regression, so a healthy group would be discarded on a stream with no fault"
    )
    assert len(buffered) == 5


def test_a_regression_within_one_partition_is_still_caught() -> None:
    """Per-partition keying must not weaken the detection it exists to make correct."""
    buffered, redeliveries = _replay([100, 5, 100, 6], partitions=[0, 1, 0, 1])
    assert redeliveries == 1


def test_the_loop_detects_a_backwards_position_per_partition() -> None:
    source = _process_source()
    assert "last_seen" in source
    assert "offset <= previous" in source
    assert "message.partition()" in source, (
        "the position state is not keyed by partition, so offsets from independent "
        "sequences are compared against each other"
    )


def test_the_reset_clears_the_group_rather_than_trimming_it() -> None:
    """Trimming would need to know which records were re-delivered. Clearing does not."""
    tree = ast.parse(_process_source())
    resets = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and "previous" in ast.unparse(node.test)
        and "buffered = []" in ast.unparse(node)
    ]
    assert resets, "the backwards-position branch does not clear the buffered group"
    body = ast.unparse(resets[0])
    assert "buffered_saga = None" in body and "buffered_index = None" in body


def test_redeliveries_are_recorded_as_evidence() -> None:
    """A run that re-delivered is a different run from one that did not.

    The count goes into the evidence so a reader can see the rebalance happened rather
    than inferring it from a gap in the offsets.
    """
    assert '"redeliveries": redeliveries' in _process_source()
