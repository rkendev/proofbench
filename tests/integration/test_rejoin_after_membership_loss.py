"""The cycle 1 void, converted from a diagnosis into a gate.

Four good broker runs exhausted the recovery budget on repeated
``UNKNOWN_MEMBER_ID`` from ``send_offsets_to_transaction``, and the matrix voided. The
cause: a broker restart invalidates the consumer's group membership, that call reads
membership from ``consumer_group_metadata()``, a consumer rejoins only when the client
is polled, and the recovery loop replayed the sink write without ever polling.

**This test does not restart a broker.** Runs 09 and 12 survived the same fault on a
race, so a single passing run proves nothing, and a test whose setup is itself a race
would inherit that. Membership is invalidated deterministically instead: a member joins,
leaves, and is replaced, and the metadata captured before the replacement is exactly the
stale membership a restart produces.

That turns "we changed something and it stopped failing" into a property with a name.
"""

from __future__ import annotations

import pytest

from proofbench.core.topics import delete, provision

TOPIC = "proofbench.selftest.rejoin"
GROUP = "proofbench.selftest.rejoin.group"
TXN_ID = "proofbench.selftest.rejoin.txn"


@pytest.fixture
def scratch(broker: str):
    provision(broker, [TOPIC])
    try:
        yield TOPIC
    finally:
        delete(broker, [TOPIC])


def _consumer(broker: str):
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 6000,
        }
    )


def _joined(consumer) -> None:
    """Poll until the client has completed its join and holds an assignment."""
    for _ in range(40):
        consumer.poll(0.5)
        if consumer.assignment():
            return
    raise AssertionError("the consumer never joined its group")


def test_stale_membership_fails_and_a_poll_is_what_fixes_it(broker: str, scratch: str) -> None:
    """The void's exact error, reproduced deterministically, and its repair.

    Without the poll the call fails with UNKNOWN_MEMBER_ID, which is precisely what
    runs 03, 06, 15 and 18 hit four times each before exhausting the budget. With
    membership obtained after a poll it succeeds.

    ADR-0003 section 6 fixes the response to an abortable error as "abort, then replay
    that saga". Under the good configuration a saga replay commits offsets inside the
    transaction, which needs membership, which needs a poll, so the poll is a mechanical
    precondition of the frozen action rather than an addition to it.
    """
    from confluent_kafka import KafkaError, KafkaException, Producer, TopicPartition

    producer = Producer({"bootstrap.servers": broker})
    producer.produce(scratch, key=b"k", value=b"v")
    assert producer.flush(30) == 0

    first = _consumer(broker)
    first.subscribe([scratch])
    _joined(first)
    stale = first.consumer_group_metadata()
    first.close()  # the generation this metadata names is retired

    second = _consumer(broker)
    second.subscribe([scratch])
    _joined(second)

    transactional = Producer(
        {
            "bootstrap.servers": broker,
            "transactional.id": TXN_ID,
            "enable.idempotence": True,
            "message.timeout.ms": 15000,
        }
    )
    transactional.init_transactions(30)
    transactional.begin_transaction()

    try:
        with pytest.raises(KafkaException) as caught:
            transactional.send_offsets_to_transaction([TopicPartition(scratch, 0, 1)], stale, 30)
        assert caught.value.args[0].code() == KafkaError.UNKNOWN_MEMBER_ID, (
            f"expected the void's error, got {caught.value.args[0].name()}"
        )

        # The abortable error leaves the transaction unusable, exactly as the recovery
        # contract says, so it is aborted before the replay.
        transactional.abort_transaction(30)
        transactional.begin_transaction()

        # Membership obtained after polling. This is what rejoin_consumer restores.
        transactional.send_offsets_to_transaction(
            [TopicPartition(scratch, 0, 1)], second.consumer_group_metadata(), 30
        )
        transactional.commit_transaction(30)
    finally:
        second.close()


def test_the_recovery_path_polls_before_replaying(broker: str) -> None:
    """And it does so unconditionally, with no branch on the configuration.

    The baseline never reaches the abortable-error branch, because it makes no
    transactional call, so an unconditional poll costs it nothing while keeping
    INV-P3's control-flow gate clean. A configuration branch here would be a difference
    in the recovery path, which is what that gate exists to forbid.
    """
    import ast
    import inspect

    from proofbench.core import run

    tree = ast.parse(inspect.getsource(run.process))
    recovery = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "write_group_with_recovery"
    )
    calls = [
        node.func.id
        for node in ast.walk(recovery)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    assert "rejoin_consumer" in calls, (
        "the recovery path does not poll the consumer before replaying, which is the "
        "cycle 1 void: a saga replay commits offsets and that needs group membership"
    )

    # No configuration branch guards it.
    guarded: set[str] = set()
    for node in ast.walk(recovery):
        if isinstance(node, ast.If) and any(
            isinstance(inner, ast.Call)
            and isinstance(inner.func, ast.Name)
            and inner.func.id == "rejoin_consumer"
            for inner in ast.walk(node)
        ):
            guarded |= {
                ast.unparse(inner)
                for inner in ast.walk(node.test)
                if isinstance(inner, ast.Name | ast.Attribute)
            }
    assert "transactional" not in guarded, (
        f"the rejoin poll is guarded by {sorted(guarded)}, so the recovery path branches "
        f"on the configuration"
    )
