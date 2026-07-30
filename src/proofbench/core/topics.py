"""Provision the three topics one run uses, and read a sink topic back.

The client is imported lazily inside each function rather than at module import,
so importing anything under ``proofbench`` neither loads librdkafka nor dials a
broker. Test collection stays offline and CI boots nothing.

Provisioning is delete-then-create, scoped to exactly the three names the run
uses and never a wildcard. Two reasons it is explicit rather than left to broker
auto-creation, which docker-compose.yml turns off:

- a rerun of a run must start from an empty topic, or the previous attempt's
  records would be counted as duplicates of this one
- a typo in a topic name fails loudly here instead of silently producing into a
  fresh empty topic, which would report total loss for an apparatus reason
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Sequence
from typing import Any

# One partition per topic. Total ordering over the saga stream, which makes
# PB-T3's seeded fault point a single well-defined position in the stream rather
# than a position per partition. Single-node KRaft cannot hold a replication
# factor above 1 in any case, which CLAIMS.md already records as the v1 scope
# limit. Neither number enters docs/run_schedule.json: they are apparatus shape,
# not frozen experiment constants.
PARTITIONS = 1
REPLICATION_FACTOR = 1

# Bounds on the delete-then-create dance. Kafka deletes asynchronously, so a
# create issued immediately after a delete can be rejected or, worse, silently
# resurrect the old log. Polling is bounded so a broker that never converges
# fails the run rather than hanging the matrix.
_SETTLE_TIMEOUT_S = 60.0
_SETTLE_POLL_S = 0.25
_ADMIN_TIMEOUT_S = 30.0


class TopicProvisioningError(Exception):
    """The topics a run needs could not be brought to a known-empty state."""


def _admin(bootstrap: str) -> Any:
    """Construct an AdminClient. The single client import site in this module."""
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap})


def _existing(admin: Any, names: Sequence[str]) -> set[str]:
    """Return which of ``names`` the broker currently holds."""
    metadata = admin.list_topics(timeout=_ADMIN_TIMEOUT_S)
    return {name for name in names if name in metadata.topics}


def _await_absent(admin: Any, names: Sequence[str]) -> None:
    deadline = time.monotonic() + _SETTLE_TIMEOUT_S
    while time.monotonic() < deadline:
        if not _existing(admin, names):
            return
        time.sleep(_SETTLE_POLL_S)
    raise TopicProvisioningError(
        f"topic(s) {sorted(_existing(admin, names))} still exist "
        f"{_SETTLE_TIMEOUT_S:.0f}s after a delete was requested"
    )


def _await_present(admin: Any, names: Sequence[str]) -> None:
    deadline = time.monotonic() + _SETTLE_TIMEOUT_S
    while time.monotonic() < deadline:
        if set(_existing(admin, names)) == set(names):
            return
        time.sleep(_SETTLE_POLL_S)
    missing = sorted(set(names) - _existing(admin, names))
    raise TopicProvisioningError(
        f"topic(s) {missing} were not created within {_SETTLE_TIMEOUT_S:.0f}s"
    )


def provision(bootstrap: str, names: Sequence[str]) -> None:
    """Bring exactly ``names`` to a known-empty state, deleting first if needed."""
    from confluent_kafka.admin import NewTopic

    admin = _admin(bootstrap)

    stale = sorted(_existing(admin, names))
    if stale:
        for name, future in admin.delete_topics(stale, operation_timeout=_ADMIN_TIMEOUT_S).items():
            try:
                future.result(_ADMIN_TIMEOUT_S)
            except Exception as exc:  # noqa: BLE001
                raise TopicProvisioningError(f"could not delete topic {name}: {exc}") from exc
        _await_absent(admin, stale)

    requested = [
        NewTopic(name, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR)
        for name in names
    ]
    for name, future in admin.create_topics(requested, operation_timeout=_ADMIN_TIMEOUT_S).items():
        try:
            future.result(_ADMIN_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001
            raise TopicProvisioningError(f"could not create topic {name}: {exc}") from exc
    _await_present(admin, names)


def delete(bootstrap: str, names: Sequence[str]) -> None:
    """Remove exactly ``names`` if they exist. Used to tidy up after a probe."""
    admin = _admin(bootstrap)
    present = sorted(_existing(admin, names))
    if not present:
        return
    for future in admin.delete_topics(present, operation_timeout=_ADMIN_TIMEOUT_S).values():
        # Tidying is best effort: a failure here cannot corrupt a measurement,
        # and raising would turn a cleanup problem into a run failure. Provisioning
        # deletes before it creates, so a topic left behind is picked up there.
        with contextlib.suppress(Exception):
            future.result(_ADMIN_TIMEOUT_S)


def delete_consumer_groups(bootstrap: str, group_ids: Sequence[str]) -> list[str]:
    """Remove exactly ``group_ids`` if they exist. Returns the ones actually deleted.

    Provisioning deletes and recreates the topics a run uses, but the consumer group
    survives, and the group id is stable per run and configuration
    (``proofbench.rNN.<configuration>``) precisely so that a restarted phase resumes
    where the killed one stopped. Across two matrix executions that stability becomes
    a hazard: the input topic is recreated empty while the group still holds a
    committed offset of 600.

    Today that survives only by accident. The stale offset is past the new high
    watermark, the broker answers ``OFFSET_OUT_OF_RANGE``, and
    ``auto.offset.reset=earliest`` quietly rescues the run. Any scenario where the
    stale offset lands *inside* the new topic's range instead, which a shorter run or
    a partially rewritten topic would produce, makes the consumer skip records that
    were never processed and the harness reports loss that no configuration caused.
    A measurement harness cannot rest on an accident of arithmetic.

    Called once per execution, before the first attempt, and **never between
    attempts**: deleting it between attempts would destroy the committed offset that
    ADR-0003 section 7 makes the process phase's durable state, which is the whole
    resume mechanism.

    A group with a live member cannot be deleted, and a SIGKILLed consumer stays
    registered until its session expires, so the non-empty error is tolerated and
    retried rather than raised.
    """
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient

    # Matched on the error code rather than on the message text. The text is a client
    # rendering that a version bump may reword, and the two conditions here mean
    # opposite things: one is the desired state and the other is a reason to wait.
    absent = {KafkaError.GROUP_ID_NOT_FOUND, KafkaError.UNKNOWN_MEMBER_ID}
    busy = {KafkaError.NON_EMPTY_GROUP}

    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + _SETTLE_TIMEOUT_S
    remaining = list(group_ids)
    deleted: list[str] = []

    while remaining and time.monotonic() < deadline:
        still_present: list[str] = []
        for name, future in admin.delete_consumer_groups(
            remaining, request_timeout=_ADMIN_TIMEOUT_S
        ).items():
            try:
                future.result(_ADMIN_TIMEOUT_S)
                deleted.append(name)
            except KafkaException as exc:
                code = exc.args[0].code()
                if code in absent:
                    # Never existed, or already gone. Both are the desired state, and
                    # a fresh run reaches this every time.
                    continue
                if code in busy:
                    # A zombie member from a SIGKILLed attempt still holds it. It ages
                    # out at session.timeout.ms, so wait rather than fail.
                    still_present.append(name)
                    continue
                raise TopicProvisioningError(
                    f"could not delete consumer group {name}: {exc}"
                ) from exc
            except Exception as exc:  # noqa: BLE001
                raise TopicProvisioningError(
                    f"could not delete consumer group {name}: {exc}"
                ) from exc
        remaining = still_present
        if remaining:
            time.sleep(_SETTLE_POLL_S)

    if remaining:
        raise TopicProvisioningError(
            f"consumer group(s) {sorted(remaining)} still held a live member "
            f"{_SETTLE_TIMEOUT_S:.0f}s after deletion was requested; a stale committed "
            f"offset would make the next run skip records it never processed"
        )
    return deleted


def read_to_end_with_offsets(conf: dict[str, Any], topic: str) -> list[tuple[int, bytes]]:
    """Read every currently available message from ``topic`` as ``(offset, value)``.

    Terminates on the partition EOF event rather than on a poll timeout. A timeout
    that fired early would under-read the sink and report loss that never
    happened, and a measurement harness cannot afford a stopping rule that can be
    wrong. ``enable.partition.eof`` is set in the shared consumer base, so both
    configurations get the same deterministic signal.

    The partition is assigned explicitly rather than subscribed, so there is no
    group rebalance to wait for and no chance of reading a partial assignment. It also
    commits nothing, so reading a topic back never disturbs the committed offset that
    ADR-0003 section 7 makes the process phase's durable state.

    The offsets are carried because PB-T3 needs them for two things the sink ledgers
    cannot answer on their own: which saga indices are durably complete in the input
    topic (the ingest resume rule), and whether a lost side effect falls inside a
    recorded offset gap (the attributability invariant).
    """
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    consumer = Consumer(conf)
    try:
        consumer.assign([TopicPartition(topic, partition, 0) for partition in range(PARTITIONS)])
        records: list[tuple[int, bytes]] = []
        remaining = PARTITIONS
        while remaining > 0:
            message = consumer.poll(_ADMIN_TIMEOUT_S)
            if message is None:
                raise TopicProvisioningError(
                    f"reading {topic} stalled for {_ADMIN_TIMEOUT_S:.0f}s without "
                    f"reaching the end of the partition; the observed ledger would "
                    f"be short and the run would report loss that did not happen"
                )
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    remaining -= 1
                    continue
                raise TopicProvisioningError(f"reading {topic} failed: {error}")
            records.append((int(message.offset()), bytes(message.value())))
        return records
    finally:
        consumer.close()


def read_to_end(conf: dict[str, Any], topic: str) -> list[bytes]:
    """Read every currently available message value from ``topic``, in offset order."""
    return [value for _, value in read_to_end_with_offsets(conf, topic)]
