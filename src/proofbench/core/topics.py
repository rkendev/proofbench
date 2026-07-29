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


def read_to_end(conf: dict[str, Any], topic: str) -> list[bytes]:
    """Read every currently available message value from ``topic``, in offset order.

    Terminates on the partition EOF event rather than on a poll timeout. A timeout
    that fired early would under-read the sink and report loss that never
    happened, and a measurement harness cannot afford a stopping rule that can be
    wrong. ``enable.partition.eof`` is set in the shared consumer base, so both
    configurations get the same deterministic signal.

    The partition is assigned explicitly rather than subscribed, so there is no
    group rebalance to wait for and no chance of reading a partial assignment.
    """
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    consumer = Consumer(conf)
    try:
        consumer.assign([TopicPartition(topic, partition, 0) for partition in range(PARTITIONS)])
        values: list[bytes] = []
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
            values.append(bytes(message.value()))
        return values
    finally:
        consumer.close()
