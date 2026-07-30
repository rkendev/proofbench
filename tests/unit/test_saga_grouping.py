"""Sagas are grouped by identity, and the batch size the schedule froze is used.

Two defects fixed together, both invisible to the control run.

**The frozen batch size reached no client.** ``consumer_max_batch_records`` is frozen
at 100, is emitted into ``docs/run_schedule.json``, and ``config.py`` calls it "the
direct determinant of C2 loss" because it bounds what has been committed but not yet
applied at the kill instant. ADR-0002's cross-client table maps it to the
``num_messages`` argument of ``Consumer.consume()``. PB-T2's ``process`` used
``poll``, which hands over one record at a time, so the stored offset ran at most one
record ahead instead of up to a hundred: the frozen artifact described a window the
code did not have.

**Grouping by counting to M breaks on a resumed stream.** After a kill the baseline's
committed offset can sit mid-saga, so a restarted consumer starts mid-saga and a
count-of-three grouping staples the tail of one saga to the head of the next. Nothing
in a no-fault run can see that, because a stream read from offset 0 is always
aligned.

The grouping is exercised here against a recorded writer rather than a broker, for the
same reason ADR-0003 section 4 gives for the sink-ordering test: a property that only
a live kill run could observe is a property nobody checks.
"""

from __future__ import annotations

import json
from typing import Any

from proofbench.config import Settings
from proofbench.core.run import write_saga_to_sinks

SINKS = ("run.sink_a", "run.sink_b")


class RecordingWriter:
    """Records produces and flushes in order. Same shape as the ordering test's."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self.calls.append(("produce", key))

    def flush(self) -> None:
        self.calls.append(("flush", ""))

    def keys_for(self, topic_index: int) -> list[str]:
        """The keys produced in the ``topic_index``-th flush-terminated run."""
        groups: list[list[str]] = [[]]
        for kind, key in self.calls:
            if kind == "flush":
                groups.append([])
            else:
                groups[-1].append(key)
        return groups[topic_index]


def _record(saga_index: int, step_index: int) -> tuple[str, bytes]:
    """One input-topic record as the process phase sees it."""
    saga_id = f"seed-{saga_index:04d}"
    step_name = ("create_ticket", "charge_card", "send_confirmation")[step_index]
    payload: dict[str, Any] = {
        "idempotency_key": f"{saga_id}:{step_name}",
        "saga_id": saga_id,
        "saga_index": saga_index,
        "step_index": step_index,
        "step_name": step_name,
    }
    return str(payload["idempotency_key"]), json.dumps(payload, sort_keys=True).encode()


def _group_by_saga_id(records: list[tuple[str, bytes]]) -> list[list[str]]:
    """The grouping rule ``process`` applies, extracted so it can be checked.

    Mirrors the loop in ``process``: close the buffered group when the payload's
    ``saga_id`` changes, and close whatever remains at the end of the stream. Kept as
    a separate function here rather than imported because ``process`` needs a live
    consumer, and the rule is what is under test rather than the plumbing.
    """
    groups: list[list[str]] = []
    buffered: list[str] = []
    buffered_saga: str | None = None
    for key, value in records:
        saga_id = str(json.loads(value.decode("utf-8"))["saga_id"])
        if buffered and saga_id != buffered_saga:
            groups.append(buffered)
            buffered = []
        buffered.append(key)
        buffered_saga = saga_id
    if buffered:
        groups.append(buffered)
    return groups


# --------------------------------------------------------------------------
# The batch size the schedule froze is the one the client is given
# --------------------------------------------------------------------------


def test_the_frozen_batch_size_is_what_process_asks_for() -> None:
    """The constant reaches the client, which it did not before.

    Read out of the source rather than asserted about behaviour, because the
    alternative needs a broker and a kill. What matters is that the call is
    ``consume`` with the frozen number, not ``poll`` with none.
    """
    import ast
    import inspect

    from proofbench.core import run

    source = inspect.getsource(run.process)
    assert "consumer.consume(" in source, (
        "process no longer calls consume, so the frozen consumer_max_batch_records "
        "reaches no client and the committed-but-not-applied window collapses to one "
        "record. ADR-0002 maps the constant to consume's num_messages argument."
    )
    assert "num_messages=settings.consumer_max_batch_records" in source

    # The batch must come from consume, never from poll, or the frozen size reaches no
    # client. A poll DOES appear in process now, in the rejoin path added to repair the
    # cycle 1 void, so the rule is tightened rather than relaxed: any poll must be
    # inside rejoin_consumer, which serves the queue to restore group membership and
    # never feeds the batch.
    tree = ast.parse(source)
    rejoin = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "rejoin_consumer"
    )
    rejoin_polls = {
        id(node)
        for node in ast.walk(rejoin)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "poll"
    }
    stray = [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "poll"
        and id(node) not in rejoin_polls
    ]
    assert not stray, (
        f"process polls outside the rejoin path: {stray}. A poll on the batch path hands "
        f"over one record at a time and defeats the frozen consumer_max_batch_records."
    )


def test_the_frozen_batch_is_a_substantial_fraction_of_the_run() -> None:
    """ADR-0002's stated reason for the value, restated as arithmetic.

    "100 of 600 effects is a substantial, bounded window." If the batch were 1 the
    loss window would be one record and C2 would measure almost nothing; if it were
    600 a single kill could lose the entire run and the seeded fault point would be
    meaningless.
    """
    settings = Settings(_env_file=None)
    total = settings.sagas_per_run * settings.steps_per_saga
    assert 1 < settings.consumer_max_batch_records < total
    assert settings.consumer_max_batch_records / total < 0.5


# --------------------------------------------------------------------------
# Grouping by saga identity
# --------------------------------------------------------------------------


def test_an_aligned_stream_groups_exactly_as_counting_to_m_would() -> None:
    """So the control run is unchanged by the switch.

    This is what makes the change safe to make before the matrix: on the stream the
    control reads, identity grouping and count-of-M grouping are the same function.
    """
    settings = Settings(_env_file=None)
    records = [_record(saga, step) for saga in range(4) for step in range(3)]
    groups = _group_by_saga_id(records)

    assert len(groups) == 4
    assert all(len(group) == settings.steps_per_saga for group in groups)
    assert groups[0] == [
        "seed-0000:create_ticket",
        "seed-0000:charge_card",
        "seed-0000:send_confirmation",
    ]


def test_a_stream_resumed_mid_saga_does_not_staple_two_sagas_together() -> None:
    """The defect count-of-M grouping would produce on every baseline kill run.

    The committed offset lands after saga 0's first step, so the resumed consumer
    sees two steps of saga 0 and then all of saga 1. Counting to three would emit
    [0.charge, 0.confirm, 1.create] as one "saga" and wrap it in one transaction,
    which is not a saga and not the frozen per-saga boundary.
    """
    resumed = [_record(0, 1), _record(0, 2), *[_record(1, step) for step in range(3)]]
    groups = _group_by_saga_id(resumed)

    assert [len(group) == 2 or len(group) == 3 for group in groups] == [True, True]
    assert groups[0] == ["seed-0000:charge_card", "seed-0000:send_confirmation"]
    assert groups[1] == [
        "seed-0001:create_ticket",
        "seed-0001:charge_card",
        "seed-0001:send_confirmation",
    ]
    # Every group holds records from exactly one saga, which is the whole property.
    for group in groups:
        assert len({key.split(":", 1)[0] for key in group}) == 1


def test_a_trailing_partial_group_is_still_a_group() -> None:
    """Written, not raised. Raising would manufacture loss.

    PB-T2 raised on a stream ending mid-saga, calling it malformed. That is true of a
    stream read from its beginning and false of one resumed from a committed offset
    that happened to land mid-saga, which is precisely what the baseline's commit
    placement produces. Discarding the tail would drop side effects the harness had
    already read and report them as lost.
    """
    truncated = [*[_record(0, step) for step in range(3)], _record(1, 0), _record(1, 1)]
    groups = _group_by_saga_id(truncated)

    assert len(groups) == 2
    assert len(groups[1]) == 2, "the trailing partial saga was dropped"


def test_a_partial_group_still_writes_both_sinks_in_the_frozen_order() -> None:
    """A short group must not become a reason to skip a sink.

    The ordering is what makes consumer_sigkill_between_sinks mean what CLAIMS.md says
    it means, and a partial group arriving at the end of a resumed stream is exactly
    the case where a special path would be tempting.
    """
    writer = RecordingWriter()
    partial = [_record(7, 1), _record(7, 2)]
    write_saga_to_sinks(writer, SINKS, partial)

    assert writer.keys_for(0) == ["seed-0007:charge_card", "seed-0007:send_confirmation"]
    assert writer.keys_for(1) == ["seed-0007:charge_card", "seed-0007:send_confirmation"]
    assert [kind for kind, _ in writer.calls].count("flush") == 2


def test_the_stopping_rule_is_the_same_code_in_both_configurations() -> None:
    """INV-P3 over control flow, which the allow-list gate cannot see.

    tests/unit/test_configs_allowlist.py compares the two configurations' settings.
    It is blind to a branch, so a consume loop that stalled differently under the
    baseline would satisfy every INV-P3 rule while making C2's number partly a
    property of the stopping rule rather than of commit placement.

    PB-T3 rewrote that loop, adding a batch wait and a cumulative stall budget, so
    the property is asserted rather than asserted about. The only configuration
    branch permitted anywhere in ``process`` is the transaction bracket, which is the
    difference CLAIMS.md names; the stopping rule reads neither the configuration nor
    its name.
    """
    import ast
    import inspect

    from proofbench.core import run
    from proofbench.core.configs import CONFIGURATION_NAMES

    source = inspect.getsource(run.process)
    tree = ast.parse(source)

    # Docstrings are prose and legitimately discuss both configurations by name, so
    # they are removed before the code is examined. Checking the raw text instead
    # would flag the module's own explanation of what it does.
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Module)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    named = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and node.value in CONFIGURATION_NAMES
    }
    assert not named, (
        f"process carries the configuration name(s) {sorted(named)} as a value in its "
        f"code, so some part of the run path can branch on which configuration is "
        f"under test"
    )

    # `configuration.name` is never read at all on this path, for anything.
    reads = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "configuration"
    }
    assert "name" not in reads, "process reads configuration.name"

    # And the only one of them that gates control flow is the transaction bracket,
    # which is the difference CLAIMS.md names.
    branch_reads: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If):
            for inner in ast.walk(node.test):
                if (
                    isinstance(inner, ast.Attribute)
                    and isinstance(inner.value, ast.Name)
                    and inner.value.id == "configuration"
                ):
                    branch_reads.add(inner.attr)
    assert branch_reads <= {"transactional"}, (
        f"process branches on configuration attribute(s) {sorted(branch_reads)}. Only "
        f"the transaction bracket may differ, which is the difference CLAIMS.md names."
    )

    # And the stopping rule reads only the shared settings, not the configuration.
    assert "settings.consume_stall_budget_ms" in source
    assert "settings.consume_batch_wait_ms" in source


def test_the_control_flow_walk_can_actually_see_a_violation() -> None:
    """The positive control for the gate above, which passes by absence.

    Added after a gate in commit 8 produced a false GREEN rather than refusing to go
    red. The gate asserts that three sets are empty, and an empty set is what a broken
    walk returns, so without this a typo in the node types would look identical to a
    stopping rule that is genuinely configuration-blind.

    The same extractions are run against source that does branch on the configuration,
    and each has to find it.
    """
    import ast

    guilty = ast.parse(
        "def process(configuration, settings):\n"
        "    budget = settings.consume_stall_budget_ms\n"
        '    if configuration.name == "baseline":\n'
        "        budget = budget / 2\n"
        "    return budget\n"
    )

    named = {
        node.value
        for node in ast.walk(guilty)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value in ("good", "baseline")
    }
    assert named == {"baseline"}, "the string-literal walk cannot see a configuration name"

    reads = {
        node.attr
        for node in ast.walk(guilty)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "configuration"
    }
    assert "name" in reads, "the attribute walk cannot see configuration.name"

    branch_reads: set[str] = set()
    for node in ast.walk(guilty):
        if isinstance(node, ast.If):
            for inner in ast.walk(node.test):
                if (
                    isinstance(inner, ast.Attribute)
                    and isinstance(inner.value, ast.Name)
                    and inner.value.id == "configuration"
                ):
                    branch_reads.add(inner.attr)
    assert branch_reads == {"name"}, "the branch walk cannot see a configuration-gated if"


def test_grouping_is_driven_by_the_payload_not_by_arrival_position() -> None:
    """So the rule cannot be fooled by where the stream resumes.

    saga_id travels inside the payload, which is what makes a sink topic
    self-describing (see core/saga.py). That is the identity the grouping keys on, so
    the group boundaries are a property of the data rather than of the offset the
    consumer happened to start at.
    """
    single_step = [_record(5, 2)]
    assert _group_by_saga_id(single_step) == [["seed-0005:send_confirmation"]]

    # And a repeated saga, as a baseline ingest replay can produce, closes and
    # reopens rather than merging into one oversized group.
    repeated = [_record(9, 0), _record(9, 1), _record(10, 0), _record(9, 0)]
    groups = _group_by_saga_id(repeated)
    assert [len(group) for group in groups] == [2, 1, 1]


# --------------------------------------------------------------------------
# What the rejoin poll does with the records it pulls off the queue
# --------------------------------------------------------------------------


def test_the_rejoin_poll_neither_drops_nor_reprocesses_a_record() -> None:
    """The part of the cycle 1 repair that could have invented a number.

    The rejoin poll serves the consumer's queue, and serving a queue returns whatever
    is on it. Dropping those records would manufacture loss; handing them to the
    processing stream a second time would manufacture duplication. Either would be the
    apparatus producing a figure out of its own recovery rather than out of the
    configuration under test, which is exactly what INV-P2 exists to prevent.

    The handling: they go to a pending list, and the main loop drains that before
    calling consume again. Read out of the source because the alternative needs a live
    broker mid-rebalance, and a test that could only run there is a test nobody runs.
    """
    import inspect

    from proofbench.core import run

    source = inspect.getsource(run.process)

    # The poll appends rather than discarding.
    assert "pending.append(message)" in source, (
        "the rejoin poll discards what it pulls off the queue, which manufactures loss"
    )
    # The main loop drains pending BEFORE consuming anything new, so nothing is seen
    # twice and offset order is preserved: anything the poll returned is strictly
    # earlier than anything a later consume would return.
    assert "if pending:" in source
    assert source.index("batch = list(pending)") < source.index("batch = consumer.consume(")
    assert "pending.clear()" in source


def test_pending_records_go_through_the_same_loop_body() -> None:
    """So a partition-EOF event pulled off by the rejoin poll still ends the drain.

    The rejoin poll cannot distinguish a data record from an EOF event and must not
    try. Both are put on the pending list and both are handled by the branch that
    already handles everything consume() returns.
    """
    import ast
    import inspect

    from proofbench.core import run

    tree = ast.parse(inspect.getsource(run.process))
    # Exactly one `for message in batch:` loop, so pending and consumed records cannot
    # be handled by two different code paths.
    loops = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.For)
        and isinstance(node.target, ast.Name)
        and node.target.id == "message"
        and isinstance(node.iter, ast.Name)
        and node.iter.id == "batch"
    ]
    assert len(loops) == 1, f"expected one batch-handling loop, found {len(loops)}"
