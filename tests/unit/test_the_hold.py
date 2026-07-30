"""The D4 hold: giving the configuration's own commit mechanism one opportunity.

``baseline_auto_commit_interval_ms`` is frozen at 5000 and the whole baseline
execution measures about 2 seconds, so the first interval tick lands after the run has
ended, and librdkafka's commit on ``close`` is skipped by a SIGKILL. No offset is ever
committed during the baseline process phase, which means commit-before-processing, the
defect C2 exists to measure, never fires at all. Worse, the restarted consumer then
finds no committed offset and replays from offset 0, contradicting ADR-0003 section 7's
"it never replays from the start of the run".

Two frozen constants that are mutually inoperative, and neither may move. ADR-0004
records the full argument, including the part that runs against it: the hold does not
restore a natural race, it flips a determined outcome. What makes it a validity repair
rather than tuning is that it decides only *whether* the mechanism acts, never *how
much* is lost. Once the commit has fired, the quantity is fixed entirely by
``consumer_max_batch_records`` and the frozen fault point.

Three properties are gated here: the hold is long enough to guarantee a tick after the
offsets were stored, it is identical in both configurations, and it never sits inside
an open transaction.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest

from proofbench.config import Settings
from proofbench.core.faults import (
    FAULT_BROKER_STOP_START,
    FAULT_CONSUMER_SIGKILL,
    FAULT_PRODUCER_SIGKILL,
    FaultInjectingWriter,
    NoFault,
    SeededFault,
)
from proofbench.core.recovery import ApparatusFailure
from proofbench.core.state import RunState

FAULT_SAGA = 108


def _entry(fault_type: str) -> dict[str, Any]:
    return {
        "run_id": 2,
        "fault_type": fault_type,
        "fault_point": {"saga_index": FAULT_SAGA, "step_index": 1},
        "control": False,
    }


def _fault(fault_type: str, tmp_path: Path, hold_ms: int) -> tuple[SeededFault, RunState]:
    """A live injector whose hold is short enough to measure without waiting 10s."""
    settings = Settings(_env_file=None).model_copy(
        update={"fault_hold_intervals": 1, "baseline_auto_commit_interval_ms": hold_ms}
    )
    state = RunState(run_id=2, configuration="good", entry_names_a_fault=True)
    return SeededFault(_entry(fault_type), state, tmp_path / "state.json", settings), state


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self.calls.append("produce")

    def flush(self) -> None:
        self.calls.append("flush")


# --------------------------------------------------------------------------
# It holds where and only where it should
# --------------------------------------------------------------------------


def test_the_hold_happens_at_the_seeded_saga(tmp_path: Path) -> None:
    fault, _ = _fault(FAULT_CONSUMER_SIGKILL, tmp_path, hold_ms=200)
    started = time.monotonic()
    fault.saga_started(FAULT_SAGA)
    assert time.monotonic() - started >= 0.2


def test_the_hold_does_not_happen_at_any_other_saga(tmp_path: Path) -> None:
    """A hold on every saga would add hours and change the whole run's profile."""
    fault, _ = _fault(FAULT_CONSUMER_SIGKILL, tmp_path, hold_ms=2000)
    started = time.monotonic()
    for saga in (0, 1, FAULT_SAGA - 1, FAULT_SAGA + 1, 199):
        fault.saga_started(saga)
    assert time.monotonic() - started < 0.5


def test_the_hold_does_not_happen_in_the_ingest_phase(tmp_path: Path) -> None:
    """There is no consumer and no commit timer in ingest, so holding is cargo cult.

    A phase property, not a configuration property, so INV-P3 is untouched.
    """
    fault, _ = _fault(FAULT_PRODUCER_SIGKILL, tmp_path, hold_ms=2000)
    started = time.monotonic()
    fault.saga_started(FAULT_SAGA)
    assert time.monotonic() - started < 0.5


def test_the_broker_fault_holds_too(tmp_path: Path) -> None:
    """It is a process-phase fault, and the seam is the same one.

    The outage itself is five times the commit interval, so a broker run would get its
    commit anyway. Holding regardless keeps the seam one rule rather than two.
    """
    fault, _ = _fault(FAULT_BROKER_STOP_START, tmp_path, hold_ms=200)
    started = time.monotonic()
    fault.saga_started(FAULT_SAGA)
    assert time.monotonic() - started >= 0.2


def test_no_hold_happens_on_a_run_with_no_fault() -> None:
    """34 of the 42 executions run with NoFault, and none of them may pay 10 seconds."""
    started = time.monotonic()
    for saga in range(200):
        NoFault().saga_started(saga)
    assert time.monotonic() - started < 0.1


# --------------------------------------------------------------------------
# It is identical in both configurations
# --------------------------------------------------------------------------


def test_the_hold_duration_does_not_depend_on_the_configuration(tmp_path: Path) -> None:
    """Unconditional and identical, so it cannot become an INV-P3 difference.

    Inert under `good` in effect, because auto-commit is off and there is no timer to
    give an opportunity to, but the code and the duration are the same. A hold that
    ran only under the baseline would be a timing difference produced by the harness
    rather than by an allow-listed setting, which is the D5 class of leak.
    """
    durations = []
    for configuration in ("good", "baseline"):
        settings = Settings(_env_file=None).model_copy(
            update={"fault_hold_intervals": 1, "baseline_auto_commit_interval_ms": 200}
        )
        state = RunState(run_id=2, configuration=configuration, entry_names_a_fault=True)
        fault = SeededFault(
            _entry(FAULT_CONSUMER_SIGKILL), state, tmp_path / f"{configuration}.json", settings
        )
        started = time.monotonic()
        fault.saga_started(FAULT_SAGA)
        durations.append(time.monotonic() - started)

    assert abs(durations[0] - durations[1]) < 0.15, (
        f"the hold took {durations[0]:.3f}s under good and {durations[1]:.3f}s under "
        f"baseline, so its duration depends on the configuration"
    )


def test_the_hold_is_two_full_intervals_by_default() -> None:
    """One interval does not guarantee a tick AFTER the offsets were stored.

    The commit timer is periodic from consumer construction, so a tick can land
    microseconds before the store. Two guarantee one after it, with margin for the
    commit round trip.
    """
    settings = Settings(_env_file=None)
    assert settings.fault_hold_ms == 2 * settings.baseline_auto_commit_interval_ms
    assert settings.fault_hold_ms == 10_000


# --------------------------------------------------------------------------
# It never sits inside an open transaction
# --------------------------------------------------------------------------


def test_a_saga_that_begins_with_a_transaction_open_is_refused() -> None:
    """What licenses excluding the hold from the combined open-transaction bound.

    ADR-0004 reserves broker_outage_ms + txn_headroom_ms inside the pinned 60s
    transaction timeout and deliberately leaves the 10s hold out. That is sound only
    if the hold never sits inside a transaction. With it inside, the slack falls from
    15s to 5s and a busy host could push a broker run's transaction into a timeout,
    which presents as a fatal error and voids the matrix from a single run.

    The check is an invariant of the process loop rather than a property of the fault:
    each saga's transaction begins after this point and ends before the next saga
    starts, so nothing may be open here on any run at all.
    """
    writer = FaultInjectingWriter(_Recorder(), NoFault(), transaction_open=lambda: True)
    with pytest.raises(ApparatusFailure, match="while a transaction was still open"):
        writer.saga_started(FAULT_SAGA)


def test_a_saga_that_begins_cleanly_is_allowed() -> None:
    writer = FaultInjectingWriter(_Recorder(), NoFault(), transaction_open=lambda: False)
    writer.saga_started(FAULT_SAGA)


def test_the_check_runs_on_every_saga_not_only_the_seeded_one() -> None:
    """It is a loop invariant, so it has to hold everywhere or it holds nowhere."""
    writer = FaultInjectingWriter(_Recorder(), NoFault(), transaction_open=lambda: True)
    for saga in (0, 5, FAULT_SAGA, 199):
        with pytest.raises(ApparatusFailure):
            writer.saga_started(saga)


def test_the_process_loop_wires_the_real_producer_into_the_check() -> None:
    """A probe that always answered False would make the invariant decorative.

    The same shape as the boundary control: a check nobody asks a real question is not
    a check. This asserts the writer is constructed with the sender's own
    transaction state rather than the permissive default.
    """
    import inspect

    from proofbench.core import run

    source = inspect.getsource(run.process)
    assert "FaultInjectingWriter(sender, injector, lambda: sender.txn.transaction_open)" in source


def test_the_hold_happens_before_the_transaction_bracket_opens() -> None:
    """The ordering the exclusion depends on, read out of the process loop.

    saga_started, which is where the hold lives, must be called before
    begin_transaction. Reversed, the hold would sit inside the transaction and the
    combined bound's arithmetic would be wrong by 10 seconds.
    """
    import inspect

    from proofbench.core import run

    source = inspect.getsource(run.process)
    assert source.index("writer.saga_started(") < source.index("sender.txn.begin()")
