"""The fault injector: three fault types, armed once, identical in both configurations.

Everything here reads the frozen schedule entry and the durable run state. **Nothing
here reads the configuration under test**, and that is the property INV-P3 needs at
this layer: the allow-list gate compares settings dictionaries, so an injector that
fired at a different point, or not at all, under one configuration would satisfy every
INV-P3 rule while making the matrix a comparison of two different experiments.
``tests/unit/test_fault_injector.py`` walks this module and asserts the configuration
is unreachable from it.

**Inert by default.** ``NoFault`` is the implementation the run path gets unless a
schedule entry names a fault, the fault belongs to the phase being run, and the marker
says it has not already fired. A control run cannot construct a live injector even by
mistake: ``SeededFault`` refuses an entry without a fault point.

**The three seams**, chosen so that ``write_saga_to_sinks`` and its unit test are not
touched. That function is the only guard on a frozen ordering decision the control run
cannot observe (ADR-0003 section 4), and code that must not be touched should not be
touched. So the process-phase faults arrive through a decorator over the existing
``SinkWriter`` protocol rather than through an edit to the writer:

- ``producer_sigkill_mid_send`` fires in **ingest**, after the seeded step has been
  handed to ``produce`` and before the saga is flushed. That is the mid-send window:
  the records are in the client's queue and may or may not have reached the broker,
  which is exactly the ambiguity a real producer crash creates.
- ``consumer_sigkill_between_sinks`` fires in **process**, after sink A's flush and
  before anything is produced to sink B. A's flush before the kill is what makes the
  partial write real rather than nominal.
- ``broker_stop_start`` fires at the same seam and does not kill anything: it hands
  control to the supervising parent, which takes the broker away while the child waits.
  The child then produces to sink B and commits with the broker demonstrably down, so
  the failing call is guaranteed to be inside the outage rather than racing it.

**Arm once, and the guard is a cross-check rather than a flag.** The seeded fault fires
exactly one time. After the kill the restarted phase must not re-fire it, or the run
loops forever. The marker records that it fired, and the marker is fsynced *before*
``os.kill``, because a marker written after an uncatchable signal is a marker that is
never written. But a flag alone cannot detect its own loss, so the guard compares two
independent durable facts: the marker's ``fault_fired``, and the attempt history's
record of whether this phase has already been killed. A phase that was killed while the
marker says the fault never fired means the marker was lost or tampered with, and the
run is stopped rather than allowed to inject a second time.

``os.kill`` appears in this module and nowhere else under ``src/proofbench``, pinned by
an AST gate.
"""

from __future__ import annotations

import os
import signal
import time
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from proofbench.config import Settings
from proofbench.core.evidence import write_json
from proofbench.core.recovery import ApparatusFailure
from proofbench.core.state import OUTCOME_KILLED, RunState

# The fault menu, verbatim from CLAIMS.md, mapped to the phase that hosts each one.
#
# producer_sigkill_mid_send targets the INGEST producer. CLAIMS.md says the harness
# kills "producers, consumers, and the broker", three components for three faults;
# consumer_sigkill_between_sinks already kills the process that owns the sink producer;
# and ADR-0003 section 7 specifies ingest-phase restart semantics that would otherwise
# be dead contract. ADR-0004 records the consequence for C2, which is that these seven
# runs cannot lose a side effect and the claim therefore cannot reach its floor.
FAULT_PRODUCER_SIGKILL = "producer_sigkill_mid_send"
FAULT_CONSUMER_SIGKILL = "consumer_sigkill_between_sinks"
FAULT_BROKER_STOP_START = "broker_stop_start"

PHASE_OF_FAULT: dict[str, str] = {
    FAULT_PRODUCER_SIGKILL: "ingest",
    FAULT_CONSUMER_SIGKILL: "process",
    FAULT_BROKER_STOP_START: "process",
}

# Files the child and the supervising parent use to hand control back and forth for
# broker_stop_start. Plain files rather than a socket or a pipe because the child may
# be killed at any moment by a different fault type, and a file that outlives a dead
# process is exactly what is wanted.
RENDEZVOUS_FILE = "broker_fault_reached.json"
RESUME_TOKEN_FILE = "broker_fault_released.json"

# How long the child waits for the supervisor to take the broker down before giving up.
# Generous, because the alternative to waiting is proceeding with the broker still up,
# which would produce a run labelled broker_stop_start in which no outage happened.
_RENDEZVOUS_TIMEOUT_S = 120.0
_RENDEZVOUS_POLL_S = 0.1


@runtime_checkable
class SagaFaultInjector(Protocol):
    """What the run path calls. Every method is a no-op in the inert implementation.

    Deliberately narrow. The run path tells the injector where it is; the injector
    decides whether that is the seeded point. Nothing here returns a value, so no
    caller can branch on what the injector did, which keeps the injected and
    non-injected paths the same code.
    """

    def saga_started(self, saga_index: int) -> None:
        """The process phase is about to handle this saga, before any sink write."""

    def step_produced(self, saga_index: int, step_index: int) -> None:
        """The ingest phase has handed this step to produce, before the saga flush."""

    def sink_flushed(self, ordinal: int) -> None:
        """A sink has been flushed. Ordinal 0 is sink A, 1 is sink B."""


class NoFault:
    """The injector every non-fault run gets, and every phase that hosts no fault.

    Not a null object for tidiness: it is what makes "the fault fires exactly once"
    enforceable. The parent installs this whenever the entry names no fault, the
    fault belongs to another phase, or the marker says it has already fired, so the
    live injector never exists in a context where firing would be wrong.
    """

    def saga_started(self, saga_index: int) -> None:
        return None

    def step_produced(self, saga_index: int, step_index: int) -> None:
        return None

    def sink_flushed(self, ordinal: int) -> None:
        return None


class SeededFault:
    """The live injector for one run, armed to fire exactly once at the seeded point.

    Holds the schedule entry and the durable state. It never receives, imports, or
    reads a configuration.
    """

    def __init__(
        self,
        entry: dict[str, Any],
        state: RunState,
        state_path: Path,
        settings: Settings,
    ) -> None:
        fault_point = entry["fault_point"]
        if fault_point is None:
            raise ApparatusFailure(
                f"run {entry['run_id']} carries no fault point, so no fault can be armed "
                f"for it. A control run must never construct a live injector."
            )
        self._fault_type = str(entry["fault_type"])
        self._saga_index = int(fault_point["saga_index"])
        self._step_index = int(fault_point["step_index"])
        self._state = state
        self._state_path = state_path
        self._settings = settings
        self._current_saga: int | None = None

    # ------------------------------------------------------------------
    # The seams
    # ------------------------------------------------------------------

    def saga_started(self, saga_index: int) -> None:
        self._current_saga = saga_index

    def step_produced(self, saga_index: int, step_index: int) -> None:
        if self._fault_type != FAULT_PRODUCER_SIGKILL:
            return
        if saga_index != self._saga_index or step_index != self._step_index:
            return
        # The mid-send window: the step is in the producer's queue and the saga has
        # not been flushed, so whether it reached the broker is genuinely undecided.
        # Under the good configuration it does not matter, because either way the
        # records sit inside the open transaction that init_transactions will abort.
        self._fire(saga_index)

    def sink_flushed(self, ordinal: int) -> None:
        if self._fault_type not in (FAULT_CONSUMER_SIGKILL, FAULT_BROKER_STOP_START):
            return
        if ordinal != 0 or self._current_saga != self._saga_index:
            return
        # Sink A is durable and sink B has not been touched. Under the baseline that
        # partial write survives, which is the case CLAIMS.md names; under the good
        # configuration the abort removes both, and preventing the partial write is
        # the property being demonstrated.
        self._fire(self._saga_index)

    # ------------------------------------------------------------------
    # Firing
    # ------------------------------------------------------------------

    def _fire(self, saga_index: int) -> None:
        """Record that the fault fired, durably, and then inflict it.

        The order is not negotiable. ``os.kill`` with SIGKILL cannot be caught,
        deferred, or wrapped in a ``finally``, so anything not on disk before the call
        never happens. A marker written afterwards is a marker never written, and a
        restarted phase that reads "not yet fired" fires again, and the run loops.
        """
        self._state.fault_fired = True
        self._state.fault_fired_phase = PHASE_OF_FAULT[self._fault_type]
        self._state.fault_fired_saga = saga_index
        self._state.save(self._state_path)

        if self._fault_type == FAULT_BROKER_STOP_START:
            self._await_outage()
            return
        _sigkill_self()

    def _await_outage(self) -> None:
        """Hand control to the supervisor and wait until the broker is actually down.

        The child stops here rather than merely signalling and carrying on. An
        external supervisor cannot reliably hit a window measured in milliseconds, so
        without the wait the outage would begin at some unpredictable point after the
        fault point, and a run labelled broker_stop_start might contain a produce that
        never met an outage at all. Blocking makes the failing call deterministic
        while leaving the outage itself genuinely external.
        """
        directory = self._state_path.parent
        write_json(
            directory / RENDEZVOUS_FILE,
            {
                "fault_type": self._fault_type,
                "saga_index": self._saga_index,
                "step_index": self._step_index,
            },
        )

        token = directory / RESUME_TOKEN_FILE
        deadline = time.monotonic() + _RENDEZVOUS_TIMEOUT_S
        while time.monotonic() < deadline:
            if token.exists():
                return
            time.sleep(_RENDEZVOUS_POLL_S)

        raise ApparatusFailure(
            f"the supervisor did not take the broker down within "
            f"{_RENDEZVOUS_TIMEOUT_S:.0f}s of the fault point being reached. Continuing "
            f"would produce a run labelled {self._fault_type} in which no outage "
            f"happened, so the run reports no result instead."
        )


def _sigkill_self() -> None:
    """Die exactly here, the way an externally killed process dies.

    The only ``os.kill`` in ``src/proofbench``, pinned by an AST gate in
    tests/unit/test_fault_injector.py.

    SIGKILL rather than SIGTERM because it cannot be caught, blocked, or handled: no
    ``finally`` runs, no ``atexit`` hook fires, no client flushes its queue on the way
    out. That is what makes the kill a faithful stand-in for a crash rather than a
    polite shutdown, and it is why the marker has to be durable before this line.

    Self-inflicted rather than supervisor-inflicted because the window between two
    flushes is sub-millisecond, and an external process cannot be scheduled inside it.
    The process dies at a known instruction, which is what makes the fault point
    reproducible from the seed rather than from timing.
    """
    os.kill(os.getpid(), signal.SIGKILL)


class FaultInjectingWriter:
    """Wraps a ``SinkWriter`` so the process-phase faults need no edit to the writer.

    ``write_saga_to_sinks`` is untouched, and so is
    ``tests/unit/test_sink_ordering.py``, which is the repository's only guard on the
    frozen sink ordering (ADR-0003 section 4 explains why the control run cannot see
    it). Decorating rather than editing keeps that guard exactly as it was.

    The decorator counts flushes because the ordering is positional: the first flush
    of a saga is sink A's, the second is sink B's. ``write_saga_to_sinks`` produces
    every record to A, flushes, produces every record to B, flushes, so counting is
    sufficient and needs no knowledge of topic names.
    """

    def __init__(self, inner: Any, injector: SagaFaultInjector) -> None:
        self._inner = inner
        self._injector = injector
        self._flushes = 0

    def saga_started(self, saga_index: int) -> None:
        """Reset the per-saga flush counter and tell the injector where we are."""
        self._flushes = 0
        self._injector.saga_started(saga_index)

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self._inner.produce(topic, key, value)

    def flush(self) -> None:
        self._inner.flush()
        ordinal = self._flushes
        self._flushes += 1
        # After the inner flush, so sink A is genuinely durable before anything fires.
        self._injector.sink_flushed(ordinal)


def select_injector(
    entry: dict[str, Any],
    state: RunState,
    state_path: Path,
    phase: str,
    settings: Settings,
) -> SagaFaultInjector:
    """Return the injector this phase of this attempt should run with.

    Inert unless every condition for firing holds. Each ``NoFault`` below is a
    different reason, and none of them is the configuration under test:

    - the entry names no fault, so there is nothing to arm
    - the fault belongs to a different phase
    - the marker says it has already fired

    **The arm-once guard.** A flag cannot detect its own loss, so two independent
    durable facts are cross-checked: the marker's ``fault_fired``, and the attempt
    history's record of whether this phase has already been killed. A phase that was
    killed while the marker says the fault never fired means the marker was lost,
    truncated, or removed. Arming again would fire a second time, and the phase would
    be killed again, and the run would never finish. So it is stopped, and recorded as
    an apparatus failure rather than absorbed: a fault that fires twice is not the
    fault the frozen schedule describes, and a run that contains it has no claim to
    report.
    """
    fault_type = str(entry["fault_type"])
    if entry["fault_point"] is None:
        return NoFault()
    if PHASE_OF_FAULT.get(fault_type) != phase:
        return NoFault()

    killed_before = sum(
        1 for attempt in state.attempts_for(phase) if attempt.outcome == OUTCOME_KILLED
    )
    if state.fault_fired:
        return NoFault()
    if killed_before:
        state.fault_fired_twice = True
        state.save(state_path)
        raise ApparatusFailure(
            f"the {phase} phase has already been killed {killed_before} time(s), but the "
            f"durable marker does not record the fault as fired. The marker was lost or "
            f"disarmed, and arming again would inject a second time and loop the run. "
            f"The run is abandoned and reports no result."
        )
    return SeededFault(entry, state, state_path, settings)
