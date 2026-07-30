"""The run state that has to outlive a SIGKILL.

Two of the three fault types kill the process running a phase. Everything the run
knows about itself at that instant is lost unless it is on disk, and three things
the contract depends on are exactly that kind of knowledge:

- **The recovery budget.** ADR-0003 section 6 bounds a run at three producer
  re-initialisations. PB-T2 constructed ``RecoveryBudget`` inside ``execute_run``, so
  the bound was per-attempt rather than per-run and was therefore unenforceable
  across the kill it exists to bound. Three re-inits in each of four attempts is
  twelve, and the run would still have reported a result.
- **The transaction counts.** A ledger that reset on restart would report the last
  attempt's activity as though it were the run's, which is the same class of untruth
  as the derived count it replaced.
- **Whether the fault has already fired.** The seeded fault fires exactly once. The
  restarted phase has to know that the killed one already fired, or it fires again,
  and again, and the run loops forever. This is the field that must be durable
  *before* ``os.kill``, not merely eventually.

So the state is a file, written atomically, and the parent process owns it. Written
atomically because the writer is the process being killed: ``Path.write_text``
truncates and then writes, and a marker caught mid-write reads as "not yet fired",
which is precisely the value that restarts the loop.

It also carries the per-attempt offset record that the attributability invariant
consumes. That invariant is what replaced PB-T2's ``sagas_done == expected_sagas``
assertion, which could not survive a restart under either configuration: a resumed
phase processes only the remainder, so the assertion would have apparatus-failed
every process-phase kill run and gutted C1's coverage rather than only C2's.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from proofbench.core.evidence import write_json
from proofbench.core.recovery import ApparatusFailure, RecoveryBudget
from proofbench.core.txn import TransactionLedger

# How many times one phase may be restarted before the run is abandoned. A backstop
# against a crash loop, not a contract number: the seeded fault fires once, so a
# healthy kill run uses exactly two attempts, and anything past a handful means the
# phase is failing for a reason the injector did not cause.
MAX_ATTEMPTS_PER_PHASE = 4

PHASE_INGEST = "ingest"
PHASE_PROCESS = "process"

OUTCOME_COMPLETED = "completed"
OUTCOME_KILLED = "killed"
OUTCOME_FAILED = "apparatus_failure"


@dataclass(frozen=True, slots=True)
class Attempt:
    """One attempt at one phase, and the offsets it covered.

    ``resumed_at`` and ``last_applied`` are what make a loss attributable. The gap
    between one attempt's ``last_applied`` and the next attempt's ``resumed_at`` is
    the set of input-topic offsets nothing ever processed, which under the baseline
    is exactly the committed-but-not-applied window that produces loss. A lost side
    effect whose record does not fall in such a gap was lost for some other reason,
    and that other reason is an apparatus defect rather than a measurement.
    """

    phase: str
    number: int
    outcome: str
    resumed_at: int | None = None
    last_applied: int | None = None
    detail: str = ""

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "attempt": self.number,
            "outcome": self.outcome,
            "resumed_at_offset": self.resumed_at,
            "last_applied_offset": self.last_applied,
            "detail": self.detail,
        }

    @classmethod
    def from_jsonable(cls, payload: dict[str, Any]) -> Attempt:
        return cls(
            phase=str(payload["phase"]),
            number=int(payload["attempt"]),
            outcome=str(payload["outcome"]),
            resumed_at=payload["resumed_at_offset"],
            last_applied=payload["last_applied_offset"],
            detail=str(payload.get("detail", "")),
        )


@dataclass(slots=True)
class RunState:
    """Everything about one execution that has to survive the process running it."""

    run_id: int
    configuration: str
    attempts: list[Attempt] = field(default_factory=list)
    budget: RecoveryBudget = field(default_factory=RecoveryBudget)
    transactions: TransactionLedger = field(default_factory=TransactionLedger)

    # The arm-once state. ``fault_fired`` is the field that must be durable before
    # os.kill; the phase and saga record where it fired, which the evidence needs in
    # order to say what the fault did rather than only that one happened.
    fault_fired: bool = False
    fault_fired_phase: str | None = None
    fault_fired_saga: int | None = None
    fault_window_closed: bool = False

    # Set when a restarted phase discovers the fault already fired and the injector
    # would have fired it again. Treated as an apparatus failure rather than absorbed,
    # because a fault that fires twice is not the fault the schedule describes.
    fault_fired_twice: bool = False

    # ------------------------------------------------------------------
    # The fault window, as core/window.py's WindowState
    # ------------------------------------------------------------------

    entry_names_a_fault: bool = False

    @property
    def fault_has_fired(self) -> bool:
        return self.fault_fired

    @property
    def window_closed(self) -> bool:
        return self.fault_window_closed

    @property
    def budget_exhausted(self) -> bool:
        return self.budget.reinits >= self.budget.max_reinits

    # ------------------------------------------------------------------
    # Attempts
    # ------------------------------------------------------------------

    def attempts_for(self, phase: str) -> list[Attempt]:
        return [attempt for attempt in self.attempts if attempt.phase == phase]

    def next_attempt_number(self, phase: str) -> int:
        """The attempt number a phase is about to make, refusing a crash loop."""
        so_far = len(self.attempts_for(phase))
        if so_far >= MAX_ATTEMPTS_PER_PHASE:
            raise ApparatusFailure(
                f"the {phase} phase has already been attempted {so_far} times, past the "
                f"limit of {MAX_ATTEMPTS_PER_PHASE}; the seeded fault fires once, so a "
                f"healthy kill run needs two. The run is abandoned and reports no result."
            )
        return so_far + 1

    def record_attempt(self, attempt: Attempt) -> None:
        self.attempts.append(attempt)

    def offset_gaps(self, phase: str = PHASE_PROCESS) -> list[tuple[int, int]]:
        """Half-open input-topic offset ranges that no attempt ever processed.

        One gap per restart, from where the previous attempt stopped applying work to
        where the next one resumed. Under the good configuration this list must be
        empty: the offsets travelled inside the transaction, so an aborted attempt
        committed neither the work nor the offsets and the next attempt resumes
        exactly where the last durable one ended. Under the baseline it is the
        committed-but-not-applied window, which is the loss C2 measures.

        Reported as a list rather than a total because a reader checking a lost record
        needs to know which gap it fell in, not merely how many records were skipped.
        """
        gaps: list[tuple[int, int]] = []
        applied_through: int | None = None
        for attempt in self.attempts_for(phase):
            if (
                applied_through is not None
                and attempt.resumed_at is not None
                and attempt.resumed_at > applied_through
            ):
                gaps.append((applied_through, attempt.resumed_at))
            if attempt.last_applied is not None:
                applied_through = (
                    attempt.last_applied
                    if applied_through is None
                    else max(applied_through, attempt.last_applied)
                )
        return gaps

    # ------------------------------------------------------------------
    # Durability
    # ------------------------------------------------------------------

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "configuration": self.configuration,
            "entry_names_a_fault": self.entry_names_a_fault,
            "fault": {
                "fired": self.fault_fired,
                "fired_in_phase": self.fault_fired_phase,
                "fired_at_saga": self.fault_fired_saga,
                "window_closed": self.fault_window_closed,
                "fired_twice": self.fault_fired_twice,
            },
            "attempts": [attempt.to_jsonable() for attempt in self.attempts],
            "recovery": self.budget.to_jsonable(),
            "transactions": self.transactions.to_jsonable(),
        }

    @classmethod
    def from_jsonable(cls, payload: dict[str, Any]) -> RunState:
        fault = dict(payload.get("fault", {}))
        recovery = dict(payload.get("recovery", {}))
        budget = RecoveryBudget(
            max_reinits=int(recovery.get("max_reinits", RecoveryBudget().max_reinits)),
            reinits=int(recovery.get("producer_reinits", 0)),
            retries=int(recovery.get("retries", 0)),
            aborts=int(recovery.get("aborts", 0)),
            reasons=list(recovery.get("history", [])),
        )
        return cls(
            run_id=int(payload["run_id"]),
            configuration=str(payload["configuration"]),
            attempts=[Attempt.from_jsonable(row) for row in payload.get("attempts", [])],
            budget=budget,
            transactions=TransactionLedger.from_jsonable(dict(payload.get("transactions", {}))),
            fault_fired=bool(fault.get("fired", False)),
            fault_fired_phase=fault.get("fired_in_phase"),
            fault_fired_saga=fault.get("fired_at_saga"),
            fault_window_closed=bool(fault.get("window_closed", False)),
            fault_fired_twice=bool(fault.get("fired_twice", False)),
            entry_names_a_fault=bool(payload.get("entry_names_a_fault", False)),
        )

    def save(self, path: Path) -> None:
        """Write the state so a SIGKILL cannot corrupt it.

        Atomic, and fsynced before the rename, because the process that writes this is
        the process that is about to be killed. A truncated marker reads as "the fault
        has not fired", which is exactly the value that makes a restarted phase fire
        it again and loop the run forever.
        """
        write_json(path, self.to_jsonable())

    @classmethod
    def load(cls, path: Path) -> RunState | None:
        """Read the state a previous attempt left, or None if there is none."""
        if not path.exists():
            return None
        text = path.read_text(encoding="utf-8")
        if not text.strip():
            # Should be unreachable given the atomic writer, and checked anyway:
            # treating a truncated marker as absent is the failure mode that reruns
            # the fault, so it is refused loudly rather than defaulted.
            raise ApparatusFailure(
                f"the run state at {path} is empty, so whether the fault already fired "
                f"cannot be determined; the run is abandoned rather than risking a "
                f"second injection"
            )
        return cls.from_jsonable(json.loads(text))


def unattributable_losses(
    lost_keys: list[str],
    key_offsets: dict[str, int],
    gaps: list[tuple[int, int]],
) -> list[str]:
    """Return the lost keys that no recorded offset gap explains.

    The attributability invariant, and the reason it replaced PB-T2's
    ``sagas_done == expected_sagas`` check rather than merely relaxing it.

    That assertion was protecting against an apparatus bug that silently dropped
    sagas, which is a real hazard: without it, a harness defect would surface as loss
    and C1 would ship FAILED for something Kafka did not do. But it could not survive
    a restart under either configuration, since a resumed phase processes only the
    remainder. Removing it and putting nothing in its place would have left the hazard
    open.

    This is the replacement, and it is stronger. Every lost side effect must be
    explained by a specific range of input-topic offsets that a specific restart
    skipped. Under the good configuration the gap list is empty, so any loss at all is
    unattributable and the run fails as apparatus rather than reporting a C1 failure.
    Under the baseline the loss is explained by the exact committed-but-not-applied
    window that commit-before-processing produced, which is the mechanism C2 names.

    A key with no recorded offset is unattributable too: it means the record is not in
    the input topic the run read back, so the harness cannot say where it went.
    """
    unexplained: list[str] = []
    for key in lost_keys:
        offset = key_offsets.get(key)
        if offset is None or not any(start <= offset < end for start, end in gaps):
            unexplained.append(key)
    return unexplained
