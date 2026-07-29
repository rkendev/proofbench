"""Typed settings object, populated entirely from the environment (INV-1).

This module is the single authority for every connection detail, path, log level,
and experiment constant in ProofBench. No secret or endpoint literal, and no copy
of any experiment constant, appears anywhere else in the codebase. Example values
are mirrored in .env.example for local overrides.

INV-1: connection fields carry no literal value here. They default to None (or to
a plain filesystem path, which is neither an endpoint nor a secret) and are
populated from the environment when the harness run path needs them in a later
prompt.

The experiment constants below are FROZEN. They were fixed before the first broker
boot and before any measurement, and they are emitted into docs/run_schedule.json,
which a byte-equality test pins (tests/unit/test_schedule_frozen.py). Changing any
of them makes that gate go red, which is the point: the numbers that determine a
claim's outcome cannot be moved after seeing the outcome. ADR-0002 records why each
value was chosen and when.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# CLAIMS.md names exactly three saga steps (create_ticket, charge_card,
# send_confirmation), so the step count is fixed by the frozen contract rather
# than being a tunable. The validator below holds the configured list to it.
SAGA_STEPS_REQUIRED = 3


class Settings(BaseSettings):
    """Environment-populated settings for the ProofBench harness.

    Every field reads from an environment variable prefixed ``PB_`` (for example
    ``PB_SAGAS_PER_RUN``), optionally sourced from a local ``.env`` file. The
    experiment constants keep their authoritative frozen defaults here;
    connection details do not carry defaults at all.
    """

    model_config = SettingsConfigDict(
        env_prefix="PB_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # Connection details and paths (INV-1: no endpoint or secret literal)
    # ------------------------------------------------------------------

    # The broker the harness will dial. None until an environment supplies it;
    # PB-T1 boots no broker, so nothing reads this yet.
    broker_bootstrap_servers: str | None = None

    # Where a harness run writes its ledgers and evidence. A relative filesystem
    # path is neither an endpoint nor a secret; it is git-ignored.
    run_output_dir: Path = Path("runs")

    # The committed frozen schedule. Read by the writer script and the
    # byte-equality gate; both resolve it relative to the repository root.
    schedule_path: Path = Path("docs/run_schedule.json")

    # Log verbosity for the structured logger.
    log_level: str = "INFO"

    # ------------------------------------------------------------------
    # Frozen experiment constants (see ADR-0002)
    # ------------------------------------------------------------------

    # The master seed the whole 21-run schedule expands from. This is the
    # pre-registration date recorded at the top of CLAIMS.md, chosen as a
    # nothing-up-my-sleeve number: a reader who does not trust the author can
    # check it against the contract and see it could not have been selected
    # after seeing a result.
    master_seed: int = 20260728

    # The saga steps, in order, from CLAIMS.md. A duplicated charge_card is a
    # double charge; a lost send_confirmation is a silently dropped step.
    saga_step_names: tuple[str, ...] = (
        "create_ticket",
        "charge_card",
        "send_confirmation",
    )

    # Sagas per run (N). 200 sagas of 3 steps is 600 side effects per run. Fixed
    # for statistical power on claim C2 before the first boot: C2 requires the
    # known-bad baseline to lose at least one side effect in at least 80 percent
    # of the 20 kill runs, and an underpowered stream would fail C2 for reasons
    # of experimental design rather than of reality (ADR-0002).
    sagas_per_run: int = 200

    # The number of seeded kill runs. CLAIMS.md C1 says "at least 20" and C2 says
    # "the same 20", so this is exactly 20.
    kill_runs: int = 20

    # The fault menu, verbatim from CLAIMS.md. Assigned to the kill runs
    # round-robin over the kill-run ordinal, which gives 7 / 7 / 6.
    fault_menu: tuple[str, ...] = (
        "producer_sigkill_mid_send",
        "consumer_sigkill_between_sinks",
        "broker_stop_start",
    )

    # The fault_type recorded for the no-fault control run, and the run_id it
    # occupies. The control proves the apparatus reads zero when nothing was
    # killed; it is not one of the 20 kill runs and alters no claim (ADR-0002).
    no_fault_label: str = "none"
    control_run_id: int = 0

    # The band of the saga stream a fault point may land in, as fractions of N.
    # The middle 60 percent keeps the fault away from the cold-start and drain
    # edges, where a miss would be an artifact of timing rather than of
    # configuration.
    fault_saga_band: tuple[float, float] = (0.2, 0.8)

    # Where one transaction begins and ends in the good configuration. Structural
    # rather than numeric, but it governs how much is in flight at the kill
    # instant as directly as the batch settings below, so it is frozen with them.
    transaction_boundary: str = "per_saga"

    # ------------------------------------------------------------------
    # Frozen client tuning, shared by both configurations (see ADR-0002)
    # ------------------------------------------------------------------
    # N is not what determines whether the known-bad baseline loses a side
    # effect: what is in flight at the instant of the kill is, and that is set
    # here. CLAIMS.md fixes the baseline's semantics but not its numeric tuning,
    # so these are frozen for the same power reason as N and at the same time.
    #
    # Spelled for librdkafka (confluent-kafka), not for the Java client. The
    # Java consumer's max.poll.records has no librdkafka property: the
    # equivalent is the batch size handed to Consumer.consume(), which is
    # consumer_max_batch_records below. ADR-0002 carries the full mapping.

    # librdkafka linger.ms (alias queue.buffering.max.ms). This is librdkafka's
    # current default; pinning a default is not a no-op, because a client
    # upgrade can change one and an undeclared default is an undeclared input.
    producer_linger_ms: int = 5

    # librdkafka batch.size, bytes. Not librdkafka's default (1000000) but the
    # familiar Java default, chosen because at roughly 200-byte records it puts
    # about 80 records in flight per batch: bounded, non-trivial, and enough
    # that a producer kill mid-batch reliably strands work in the non-idempotent
    # baseline. A 1 MB batch would hold an entire 600-effect run, which would
    # make the seeded fault point nearly meaningless.
    producer_batch_size_bytes: int = 16384

    # The number of records handed to the application per poll, passed as
    # num_messages to Consumer.consume(). With the baseline's
    # commit-before-processing placement this bounds what has been committed but
    # not yet applied at the kill instant, so it is the direct determinant of
    # C2 loss. 100 of 600 effects is a substantial, bounded window.
    consumer_max_batch_records: int = 100

    # librdkafka queued.min.messages, the client-side prefetch depth (default
    # 100000). Bounded here so kill timing is predictable rather than dependent
    # on how much the client happened to have buffered.
    consumer_queued_min_messages: int = 1000

    # ------------------------------------------------------------------
    # Frozen tuning for the known-bad baseline only (see ADR-0002)
    # ------------------------------------------------------------------

    # librdkafka auto.commit.interval.ms (librdkafka's default). Under the
    # baseline's commit-before-processing placement this is arguably the
    # dominant loss knob, so it is frozen with the rest. It applies to the
    # baseline configuration only; the good configuration commits offsets
    # inside the transaction and never auto-commits.
    baseline_auto_commit_interval_ms: int = 5000

    @property
    def steps_per_saga(self) -> int:
        """Steps per saga (M), derived from the step names rather than restated."""
        return len(self.saga_step_names)

    @property
    def total_runs(self) -> int:
        """Every run in the schedule: the kill runs plus the single control."""
        return self.kill_runs + 1

    @model_validator(mode="after")
    def _check_frozen_constants(self) -> Settings:
        """Hold the frozen constants to the shape the contract and the harness need.

        Each of these would otherwise fail somewhere far from its cause: a
        two-step saga would silently change what "mid-saga" means, a fault band
        outside the unit interval would index off the end of the stream, and a
        non-positive batch setting would make the kill instant meaningless. They
        are cheap to check and they check the frozen artifact's inputs, so they
        run every time settings are constructed.
        """
        if len(self.saga_step_names) != SAGA_STEPS_REQUIRED:
            raise ValueError(
                f"saga_step_names must hold exactly {SAGA_STEPS_REQUIRED} steps "
                f"(CLAIMS.md names three), got {len(self.saga_step_names)}"
            )
        if len(set(self.saga_step_names)) != len(self.saga_step_names):
            raise ValueError("saga_step_names must not repeat a step name")
        if not self.fault_menu:
            raise ValueError("fault_menu must not be empty")
        if self.no_fault_label in self.fault_menu:
            raise ValueError(
                f"no_fault_label {self.no_fault_label!r} must not also be a real fault type"
            )
        if self.kill_runs <= 0:
            raise ValueError(f"kill_runs must be positive, got {self.kill_runs}")

        low, high = self.fault_saga_band
        if not 0.0 <= low < high <= 1.0:
            raise ValueError(
                f"fault_saga_band must satisfy 0 <= low < high <= 1, got {self.fault_saga_band}"
            )
        # The band has to contain at least one whole saga, or there is no legal
        # fault point to draw and the generator would raise far from the cause.
        if int(low * self.sagas_per_run) >= int(high * self.sagas_per_run):
            raise ValueError(
                f"fault_saga_band {self.fault_saga_band} spans no whole saga at "
                f"sagas_per_run={self.sagas_per_run}"
            )

        positive_tuning = {
            "sagas_per_run": self.sagas_per_run,
            "producer_linger_ms": self.producer_linger_ms,
            "producer_batch_size_bytes": self.producer_batch_size_bytes,
            "consumer_max_batch_records": self.consumer_max_batch_records,
            "consumer_queued_min_messages": self.consumer_queued_min_messages,
            "baseline_auto_commit_interval_ms": self.baseline_auto_commit_interval_ms,
        }
        for name, value in positive_tuning.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value}")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance populated from the environment."""
    return Settings()


# Kept out of Settings on purpose: a repository-root path is a property of the
# checkout, not a configurable value, and making it settable would let a stray
# environment variable point the byte-equality gate at a different file.
def repo_root() -> Path:
    """The repository root, resolved from this module's location."""
    return Path(__file__).resolve().parents[2]
