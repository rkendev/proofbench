"""The run state that outlives a SIGKILL, and the invariant that replaced a lost check.

Two of the three fault types kill the process running a phase, so anything the run
knows about itself in memory is gone. Three things the frozen contract depends on are
exactly that kind of knowledge: the recovery budget ADR-0003 section 6 bounds at three
re-initialisations per run, the transaction counts section 3 makes load-bearing
evidence, and whether the seeded fault has already fired.

The last is the one that must be durable before ``os.kill`` rather than eventually. A
restarted phase that reads "not yet fired" fires it again, and the run never ends.

The attributability invariant is the other half of this file. PB-T2 asserted
``sagas_done == expected_sagas`` at the end of the process phase. That assertion could
not survive a restart under either configuration, since a resumed phase processes only
the remainder, so it would have apparatus-failed every process-phase kill run and gutted
C1's coverage rather than only C2's. But it was protecting something real, and deleting
it without a replacement would let a harness bug that dropped sagas surface as loss and
ship as a C1 failure. The replacement asks the question the old check was really asking.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from proofbench.core.recovery import ApparatusFailure, RecoveryBudget, TransactionOutcome
from proofbench.core.state import (
    MAX_ATTEMPTS_PER_PHASE,
    OUTCOME_COMPLETED,
    OUTCOME_KILLED,
    PHASE_INGEST,
    PHASE_PROCESS,
    Attempt,
    RunState,
    unattributable_losses,
)
from proofbench.core.txn import PHASE_INGEST as TXN_PHASE_INGEST
from proofbench.core.txn import ROLE_INGEST


def _state() -> RunState:
    return RunState(run_id=8, configuration="baseline", entry_names_a_fault=True)


# --------------------------------------------------------------------------
# Durability
# --------------------------------------------------------------------------


def test_the_recovery_budget_survives_a_round_trip(tmp_path: Path) -> None:
    """The bound is per run, and PB-T2 made it per attempt.

    ADR-0003 section 6 bounds a run at three producer re-initialisations.
    RecoveryBudget was constructed inside execute_run, so a killed and restarted phase
    started again from zero: three re-inits in each of four attempts is twelve, and the
    run would still have reported a result.
    """
    state = _state()
    state.budget.record(TransactionOutcome.REINIT_PRODUCER, "producer fenced")
    state.budget.record(TransactionOutcome.ABORT_AND_REPLAY, "transaction unusable")
    path = tmp_path / "state.json"
    state.save(path)

    reloaded = RunState.load(path)
    assert reloaded is not None
    assert reloaded.budget.reinits == 1
    assert reloaded.budget.aborts == 1
    assert reloaded.budget.reasons == [
        "reinit_producer: producer fenced",
        "abort_and_replay: transaction unusable",
    ]


def test_the_budget_is_exhausted_across_attempts_not_within_one(tmp_path: Path) -> None:
    """Three re-inits spread over three attempts still exhausts the run's budget."""
    path = tmp_path / "state.json"
    state = _state()
    state.save(path)

    for _ in range(RecoveryBudget().max_reinits):
        loaded = RunState.load(path)
        assert loaded is not None
        loaded.budget.record(TransactionOutcome.REINIT_PRODUCER, "producer fenced")
        loaded.save(path)

    final = RunState.load(path)
    assert final is not None
    assert final.budget_exhausted
    with pytest.raises(ApparatusFailure, match="the run is abandoned"):
        final.budget.record(TransactionOutcome.REINIT_PRODUCER, "again")


def test_the_transaction_counts_survive_a_round_trip(tmp_path: Path) -> None:
    """A ledger that reset on restart would report the last attempt as the run."""
    state = _state()
    state.transactions.counts(TXN_PHASE_INGEST, ROLE_INGEST).commits = 96
    state.transactions.observe_open_ms(41.5)
    path = tmp_path / "state.json"
    state.save(path)

    reloaded = RunState.load(path)
    assert reloaded is not None
    assert reloaded.transactions.committed == 96
    assert reloaded.transactions.max_open_ms == 41.5


def test_the_fired_flag_survives_a_round_trip(tmp_path: Path) -> None:
    """The field that must be durable before os.kill, not eventually."""
    state = _state()
    state.fault_fired = True
    state.fault_fired_phase = PHASE_PROCESS
    state.fault_fired_saga = 108
    path = tmp_path / "state.json"
    state.save(path)

    reloaded = RunState.load(path)
    assert reloaded is not None
    assert reloaded.fault_fired
    assert reloaded.fault_fired_phase == PHASE_PROCESS
    assert reloaded.fault_fired_saga == 108


def test_an_absent_state_is_a_fresh_run(tmp_path: Path) -> None:
    assert RunState.load(tmp_path / "nothing.json") is None


def test_an_empty_state_file_is_refused_rather_than_treated_as_fresh(tmp_path: Path) -> None:
    """Because "fresh" is the value that re-fires the fault and loops the run.

    The atomic writer should make this unreachable. It is checked anyway, because the
    consequence of guessing wrong here is not a wrong number but a run that never
    terminates, and a defensive branch is cheaper than that.
    """
    path = tmp_path / "state.json"
    path.write_text("", encoding="utf-8")
    with pytest.raises(ApparatusFailure, match="cannot be determined"):
        RunState.load(path)


def test_the_state_is_written_atomically(tmp_path: Path) -> None:
    """It is written by the process that is about to be killed."""
    path = tmp_path / "state.json"
    _state().save(path)
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []
    assert json.loads(path.read_text(encoding="utf-8"))["run_id"] == 8


def test_the_window_state_shape_is_what_the_boundary_needs() -> None:
    """RunState is the real WindowState the fault-window boundary consumes."""
    from proofbench.core.window import is_within_fault_window

    state = _state()
    assert not is_within_fault_window(state), "the fault has not fired yet"
    state.fault_fired = True
    assert is_within_fault_window(state)
    state.fault_window_closed = True
    assert not is_within_fault_window(state)


# --------------------------------------------------------------------------
# Attempts and the crash-loop backstop
# --------------------------------------------------------------------------


def test_a_healthy_kill_run_uses_two_attempts() -> None:
    state = _state()
    assert state.next_attempt_number(PHASE_PROCESS) == 1
    state.record_attempt(Attempt(PHASE_PROCESS, 1, OUTCOME_KILLED, resumed_at=0, last_applied=287))
    assert state.next_attempt_number(PHASE_PROCESS) == 2


def test_a_crash_loop_ends_the_run_rather_than_running_forever() -> None:
    """The backstop, distinct from the arm-once guard.

    Arm-once stops the injector firing twice. This stops a phase that is failing for
    a reason the injector did not cause from being restarted indefinitely, which would
    stall the matrix rather than fail it.
    """
    state = _state()
    for number in range(1, MAX_ATTEMPTS_PER_PHASE + 1):
        state.record_attempt(Attempt(PHASE_PROCESS, number, OUTCOME_KILLED))
    with pytest.raises(ApparatusFailure, match="past the limit"):
        state.next_attempt_number(PHASE_PROCESS)


def test_attempts_are_counted_per_phase() -> None:
    """Ingest restarts must not consume the process phase's budget."""
    state = _state()
    for number in range(1, MAX_ATTEMPTS_PER_PHASE + 1):
        state.record_attempt(Attempt(PHASE_INGEST, number, OUTCOME_KILLED))
    assert state.next_attempt_number(PHASE_PROCESS) == 1


# --------------------------------------------------------------------------
# Offset gaps
# --------------------------------------------------------------------------


def test_a_clean_run_records_no_gap() -> None:
    state = _state()
    state.record_attempt(
        Attempt(PHASE_PROCESS, 1, OUTCOME_COMPLETED, resumed_at=0, last_applied=600)
    )
    assert state.offset_gaps() == []


def test_a_restart_that_resumed_where_it_stopped_records_no_gap() -> None:
    """The good configuration's shape: offsets travelled inside the transaction.

    An aborted attempt commits neither the work nor the offsets, so the next attempt
    resumes exactly where the last durable one ended and nothing was skipped.
    """
    state = _state()
    state.record_attempt(Attempt(PHASE_PROCESS, 1, OUTCOME_KILLED, resumed_at=0, last_applied=288))
    state.record_attempt(
        Attempt(PHASE_PROCESS, 2, OUTCOME_COMPLETED, resumed_at=288, last_applied=600)
    )
    assert state.offset_gaps() == []


def test_a_restart_that_resumed_ahead_records_the_gap() -> None:
    """The baseline's shape, and the loss C2 measures.

    The offset was committed before the work was applied, so the restarted consumer
    resumes past records nothing ever processed. 288 to 300 is the
    committed-but-not-applied window.
    """
    state = _state()
    state.record_attempt(Attempt(PHASE_PROCESS, 1, OUTCOME_KILLED, resumed_at=0, last_applied=288))
    state.record_attempt(
        Attempt(PHASE_PROCESS, 2, OUTCOME_COMPLETED, resumed_at=300, last_applied=600)
    )
    assert state.offset_gaps() == [(288, 300)]


def test_gaps_survive_a_round_trip(tmp_path: Path) -> None:
    state = _state()
    state.record_attempt(Attempt(PHASE_PROCESS, 1, OUTCOME_KILLED, resumed_at=0, last_applied=288))
    state.record_attempt(
        Attempt(PHASE_PROCESS, 2, OUTCOME_COMPLETED, resumed_at=300, last_applied=600)
    )
    path = tmp_path / "state.json"
    state.save(path)
    reloaded = RunState.load(path)
    assert reloaded is not None
    assert reloaded.offset_gaps() == [(288, 300)]


# --------------------------------------------------------------------------
# The attributability invariant
# --------------------------------------------------------------------------


def test_a_loss_inside_a_recorded_gap_is_attributable() -> None:
    """The baseline's loss, explained by the mechanism CLAIMS.md names."""
    offsets = {"s-0096:charge_card": 289, "s-0097:create_ticket": 291}
    assert unattributable_losses(list(offsets), offsets, [(288, 300)]) == []


def test_a_loss_outside_every_recorded_gap_is_not_attributable() -> None:
    """The seeded violation, and the reason this replaced the old assertion.

    A side effect that went missing where no restart skipped anything is a harness
    defect. Reporting it as loss would let an apparatus bug ship as "exactly-once did
    not hold", which is the specific untruth C1's floor would otherwise publish.
    """
    offsets = {"s-0004:charge_card": 13}
    assert unattributable_losses(list(offsets), offsets, [(288, 300)]) == ["s-0004:charge_card"]


def test_any_loss_at_all_is_unattributable_when_no_restart_happened() -> None:
    """The good configuration's case, and the direction that matters most.

    Under `good` the gap list is empty by construction, so a loss cannot be explained
    and the run ends as an apparatus failure rather than as a C1 failure. A harness
    defect must not be allowed to ship as a FAILED headline.
    """
    offsets = {"s-0100:send_confirmation": 302}
    assert unattributable_losses(list(offsets), offsets, []) == ["s-0100:send_confirmation"]


def test_a_key_with_no_recorded_offset_is_unattributable() -> None:
    """It is not in the input topic the run read back, so nothing can place it."""
    assert unattributable_losses(["ghost:charge_card"], {}, [(0, 600)]) == ["ghost:charge_card"]


def test_the_gap_is_half_open() -> None:
    """The offset a restart resumed at was processed, so it is not in the gap."""
    offsets = {"at_start": 288, "at_end": 300}
    assert unattributable_losses(["at_start"], offsets, [(288, 300)]) == []
    assert unattributable_losses(["at_end"], offsets, [(288, 300)]) == ["at_end"]


# --------------------------------------------------------------------------
# Provisioning belongs to the parent
# --------------------------------------------------------------------------


def test_execute_run_can_be_told_not_to_provision() -> None:
    """A restarted phase must never delete the topics it is resuming into.

    Doing so would destroy the durable state the ADR-0003 section 7 resume contract
    reads, and the run would report total loss for an apparatus reason. The matrix
    provisions once in the parent and passes provision_topics=False.
    """
    import inspect

    from proofbench.core.run import execute_run, prepare_topics

    signature = inspect.signature(execute_run)
    assert signature.parameters["provision_topics"].default is True
    assert callable(prepare_topics)


def test_provisioning_and_group_deletion_happen_in_the_same_place() -> None:
    """They are one decision: a recreated topic paired with a stale offset is the bug.

    The group id is stable per run and configuration precisely so a restarted phase
    resumes where the killed one stopped. Across two matrix executions that stability
    means the input topic can be recreated empty while the group still holds an offset
    of 600, which today survives only because the stale offset is out of range and
    auto.offset.reset rescues it.
    """
    import inspect

    from proofbench.core.run import prepare_topics

    source = inspect.getsource(prepare_topics)
    tree = ast.parse(source)
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert {"provision", "delete_consumer_groups"} <= called


# --------------------------------------------------------------------------
# The third attribution route, added before the matrix ran
# --------------------------------------------------------------------------


def test_a_baseline_broker_loss_has_no_gap_and_no_transaction_to_explain_it() -> None:
    """The case that forced the third route, constructed rather than argued.

    A baseline broker run has one process-phase attempt, because nothing SIGKILLs it,
    so offset_gaps() is empty. It is also non-transactional, so there is no aborted
    transaction either. A genuine loss therefore had no route to attribution at all and
    would become apparatus_failure, which under matrix-validity rule 4 voids the whole
    matrix: destroying the exact signal C2 measures, in the only runs where it can still
    appear.
    """
    state = RunState(run_id=3, configuration="baseline", entry_names_a_fault=True)
    state.record_attempt(
        Attempt(PHASE_PROCESS, 1, OUTCOME_COMPLETED, resumed_at=0, last_applied=600)
    )
    assert state.offset_gaps() == [], "a run with one attempt cannot produce a gap"

    lost = ["seed-0149:charge_card"]
    offsets = {"seed-0149:charge_card": 447}
    assert unattributable_losses(lost, offsets, state.offset_gaps()) == lost


def test_a_loss_with_a_recorded_per_record_failure_is_attributable() -> None:
    """The third route. Proven red by removing it: the loss becomes unattributable.

    The record's own send was recorded as permanently failed inside an open fault
    window, so the loss is explained. The invariant's stated purpose is that an
    UNEXPLAINED loss is an apparatus defect, and this one is explained.
    """
    lost = ["seed-0149:charge_card"]
    offsets = {"seed-0149:charge_card": 447}
    assert unattributable_losses(lost, offsets, [], permanently_failed_keys=lost) == []


def test_a_loss_without_a_recorded_failure_is_still_unattributable() -> None:
    """RECORD-LEVEL, not window-level, and this is the assertion that keeps it so.

    "Any loss during a fault window is attributable" would absorb a genuine apparatus
    break that happened to coincide with the outage, and the invariant would lose its
    teeth. A second record, lost in the same run and the same window as one with a
    recorded failure, is still unattributable because its own send was never recorded
    as failed.

    Proven red by widening the route to accept any loss while a window was open.
    """
    explained = "seed-0149:charge_card"
    unexplained = "seed-0031:send_confirmation"
    offsets = {explained: 447, unexplained: 93}

    result = unattributable_losses(
        [explained, unexplained], offsets, [], permanently_failed_keys=[explained]
    )
    assert result == [unexplained], (
        "a loss with no per-record failure recorded against it must remain an apparatus "
        "defect even inside a fault window, or the route becomes window-level"
    )


def test_the_recorded_keys_survive_a_round_trip(tmp_path: Path) -> None:
    """The phase that records them is a different process from the one that verifies."""
    state = RunState(run_id=3, configuration="baseline", entry_names_a_fault=True)
    state.permanently_failed_keys = ["seed-0149:charge_card"]
    path = tmp_path / "state.json"
    state.save(path)

    reloaded = RunState.load(path)
    assert reloaded is not None
    assert reloaded.permanently_failed_keys == ["seed-0149:charge_card"]
