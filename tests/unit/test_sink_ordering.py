"""Sink A is written and flushed before sink B is attempted.

This gate exists because the control run cannot catch the ordering being
reversed. On a run with no fault both sinks end up holding everything either way,
so a no-fault result is identical under both orders. PB-T3's
consumer_sigkill_between_sinks depends on the ordering entirely: the kill has to
leave A present and B absent, which is the partial write CLAIMS.md names. With the
order reversed that fault would produce the mirror image of the case the contract
describes, and nothing in PB-T2 would have noticed.

A frozen decision that no test can see is a comment. This is the test.
"""

from __future__ import annotations

from proofbench.core.run import write_saga_to_sinks

SINKS = ("run.sink_a", "run.sink_b")
RECORDS = [("s0:create_ticket", b"0"), ("s0:charge_card", b"1"), ("s0:send_confirmation", b"2")]


class RecordingWriter:
    """Records what was produced and when it was flushed, in order.

    A flush is recorded as its own entry rather than as a flag on a produce,
    because the question is whether a flush separates the two sinks, and that is
    a question about sequence.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self.calls.append(("produce", topic))

    def flush(self) -> None:
        self.calls.append(("flush", ""))

    @property
    def topics_in_order(self) -> list[str]:
        return [topic for kind, topic in self.calls if kind == "produce"]


def test_every_record_reaches_sink_a_before_any_reaches_sink_b() -> None:
    writer = RecordingWriter()
    write_saga_to_sinks(writer, SINKS, RECORDS)

    order = writer.topics_in_order
    assert order == [SINKS[0]] * len(RECORDS) + [SINKS[1]] * len(RECORDS), (
        f"the saga's records must all go to sink A before any goes to sink B; got {order}"
    )


def test_a_flush_separates_the_two_sinks() -> None:
    """Without it the ordering is nominal rather than real.

    Both sets of records would sit in one producer queue and reach the broker
    together, so "A before B" would describe the order of two function calls
    rather than the order of two writes, and a kill between them could not leave
    A present and B absent.
    """
    writer = RecordingWriter()
    write_saga_to_sinks(writer, SINKS, RECORDS)

    kinds = [kind for kind, _ in writer.calls]
    first_b = writer.calls.index(("produce", SINKS[1]))
    assert "flush" in kinds[:first_b], "sink A is not flushed before sink B is produced to"
    assert writer.calls[-1] == ("flush", ""), "sink B is not flushed before the saga completes"


def test_the_order_comes_from_the_argument_not_from_a_literal() -> None:
    """The caller passes configuration.topics.sinks, so this is the whole chain.

    If the function hard-coded a topic name, swapping the configuration's sink
    order would silently do nothing, and the ordering would be unfixable from the
    place that owns it.
    """
    writer = RecordingWriter()
    write_saga_to_sinks(writer, ("other.first", "other.second"), RECORDS[:1])
    assert writer.topics_in_order == ["other.first", "other.second"]


def test_an_empty_saga_writes_nothing_but_still_flushes_both_sinks() -> None:
    """Degenerate, but it must not silently skip a sink.

    A saga with no steps cannot arise from the frozen expansion. If one ever did,
    writing nothing is correct and quietly writing to only one sink is not.
    """
    writer = RecordingWriter()
    write_saga_to_sinks(writer, SINKS, [])
    assert writer.topics_in_order == []
    assert writer.calls == [("flush", ""), ("flush", "")]
