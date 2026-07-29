"""Broker reachability, so the integration suite skips rather than fails offline.

CI runs the whole offline chain and boots nothing. These tests need a live broker,
so they have to be absent from that run without being silently absent: a skip
carries a named reason, and the reason names the target that would make them run.

Two-layer probe, the pattern the sibling repository uses. A cheap check that the
address is even configured, then a real connection attempt, so a stale
PB_BROKER_BOOTSTRAP_SERVERS pointing at nothing skips with an accurate reason
instead of hanging the suite on a client retry loop.

``socket`` is imported here rather than anywhere under src/proofbench, where
INV-P1's network denylist bans it outright. A test may look at a port; the harness
run path may not.
"""

from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest

from proofbench.config import Settings

_CONNECT_TIMEOUT_S = 2.0


def _reachable(bootstrap: str) -> bool:
    """True when something is listening at the first address in ``bootstrap``."""
    first = bootstrap.split(",")[0].strip()
    host, _, port = first.rpartition(":")
    if not host or not port.isdigit():
        return False
    try:
        with socket.create_connection((host, int(port)), timeout=_CONNECT_TIMEOUT_S):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def settings() -> Settings:
    """Frozen constants, with the broker address read from the environment.

    The constants must not come from the ambient environment, or a stray PB_
    variable would change what the run expects. The broker address is the one
    thing that has to, because it is the only value that differs between one
    machine and another.
    """
    ambient = Settings()
    return Settings(_env_file=None, broker_bootstrap_servers=ambient.broker_bootstrap_servers)


@pytest.fixture(scope="session")
def broker(settings: Settings) -> Iterator[str]:
    """The reachable broker address, or skip with a reason naming how to get one."""
    bootstrap = settings.broker_bootstrap_servers
    if not bootstrap:
        pytest.skip(
            "no broker configured: PB_BROKER_BOOTSTRAP_SERVERS is unset. "
            "Run `make broker-up`, which prints the value to export."
        )
    if not _reachable(bootstrap):
        pytest.skip(
            f"no broker reachable at {bootstrap}: nothing is listening there. "
            f"Run `make broker-up`, then `make broker-status`."
        )
    yield bootstrap
