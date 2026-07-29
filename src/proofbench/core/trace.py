"""The agent tool-call trace: a pure deterministic expansion of the master seed.

CLAIMS.md describes the workload as an LLM agent's tool-call side effects "modeled
as a saga". This module is what that modelling is. The trace holds the tool-call
templates an agent would emit for the three steps CLAIMS.md names, and
``core/saga.py`` binds them to a concrete run.

**Provenance, stated plainly because the README must not imply otherwise.** The
saga shape models an LLM agent's tool-call sequence. It was authored
deterministically from the master seed, not sampled from a live model.
Regenerating it from that seed reproduces the committed file byte for byte.

That choice is recorded in ADR-0003 with its reasoning. The short version: the
frozen schedule already fixes 3 steps and 200 sagas, so a live recording could
only vary payload text, which feeds nothing but ``payload_checksum``. A trace
expanded from a seed is regenerable and therefore gate-checkable, which is the
proof technique this repository already uses on docs/run_schedule.json; a recorded
one could carry only a checksum, and its provenance note would have to name a
model, forcing an exception into a house-style gate whose stated strength is that
it carries none.

Pure by construction, exactly like ``core/schedule.py``: ``build_trace`` takes
settings and returns data, ``serialize_trace`` turns that data into the exact bytes
of the committed artifact, and the only thing that touches disk is
``load_trace`` (which reads the committed artifact for a run) and
scripts/write_agent_trace.py (which writes it).

Determinism is not delegated to ``random``. Every value is derived by integer
arithmetic on a SHA-256 digest, so the artifact depends on nothing but the master
seed: no generator implementation, no Python version, no platform.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from proofbench.config import Settings

# Bumped only if the artifact's shape changes, which is a deliberate act: the
# committed trace is an input to every measurement, and a reader needs to know
# what shape they hold.
TRACE_SCHEMA_VERSION = 1

# Distinct tool-call templates per step. 200 sagas draw from this pool, so each
# template is reused roughly three times per run, and the saga binding in
# core/saga.py is what makes the resulting payloads unique per record. The pool
# exists to give the stream realistic variety, not to be a per-saga identity.
VARIANTS_PER_STEP = 64

# The argument vocabularies. Eight values per field so the templates vary across
# a plausible range rather than cycling through a handful, and so a reader can
# see at a glance that this is authored content.
_ARGUMENT_VOCABULARIES: dict[str, dict[str, tuple[str, ...]]] = {
    "create_ticket": {
        "queue": (
            "billing",
            "support",
            "fraud_review",
            "onboarding",
            "retention",
            "compliance",
            "technical",
            "escalations",
        ),
        "priority": (
            "low",
            "normal",
            "high",
            "urgent",
            "deferred",
            "scheduled",
            "blocking",
            "informational",
        ),
        "category": (
            "payment_failed",
            "plan_change",
            "refund_request",
            "access_issue",
            "address_update",
            "invoice_query",
            "cancellation",
            "upgrade_request",
        ),
    },
    "charge_card": {
        "currency": ("EUR", "USD", "GBP", "SEK", "DKK", "NOK", "CHF", "PLN"),
        "processor": (
            "primary",
            "fallback",
            "regional_eu",
            "regional_uk",
            "legacy",
            "reserve",
            "batch",
            "manual_review",
        ),
        "descriptor": (
            "subscription",
            "one_off",
            "proration",
            "reactivation",
            "overage",
            "setup_fee",
            "renewal",
            "adjustment",
        ),
    },
    "send_confirmation": {
        "channel": (
            "email",
            "sms",
            "push",
            "in_app",
            "webhook",
            "postal",
            "voice",
            "chat",
        ),
        "locale": (
            "en_GB",
            "nl_NL",
            "de_DE",
            "fr_FR",
            "es_ES",
            "it_IT",
            "sv_SE",
            "pl_PL",
        ),
        "template": (
            "ticket_opened",
            "charge_receipt",
            "welcome_back",
            "plan_updated",
            "refund_issued",
            "action_required",
            "summary_digest",
            "reminder",
        ),
    },
}

# A numeric argument for the step where one is meaningful. charge_card is the step
# CLAIMS.md calls a double charge when it duplicates, so an amount is what makes
# the payload read as the thing being protected rather than as filler.
_AMOUNT_FIELD = "amount_cents"
_AMOUNT_MIN = 500
_AMOUNT_MAX = 250_000


@dataclass(frozen=True, slots=True)
class ToolCall:
    """One tool-call template: what the agent asked for, before it is bound to a saga.

    Frozen because the trace is an input to every measurement. A template that
    could be edited after a run read it would make that run's ledger
    unreproducible, which is the same hazard ``SideEffectRecord`` is frozen for.
    """

    call_id: str
    step_name: str
    tool: str
    arguments: dict[str, Any]

    def to_jsonable(self) -> dict[str, Any]:
        """Return the plain-data form written into the committed artifact."""
        return {
            "call_id": self.call_id,
            "step_name": self.step_name,
            "tool": self.tool,
            "arguments": dict(self.arguments),
        }


@dataclass(frozen=True, slots=True)
class AgentTrace:
    """The whole committed trace: its provenance and its templates per step."""

    schema_version: int
    master_seed: int
    provenance: str
    variants_per_step: int
    step_names: tuple[str, ...]
    tool_calls: dict[str, tuple[ToolCall, ...]]

    def to_jsonable(self) -> dict[str, Any]:
        """Return the plain-data form written into the committed artifact."""
        return {
            "schema_version": self.schema_version,
            "master_seed": self.master_seed,
            "provenance": self.provenance,
            "variants_per_step": self.variants_per_step,
            "step_names": list(self.step_names),
            "tool_calls": {
                step: [call.to_jsonable() for call in calls]
                for step, calls in self.tool_calls.items()
            },
        }

    def variants_for(self, step_name: str) -> tuple[ToolCall, ...]:
        """Return the template pool for one step, or raise if the step is unknown."""
        try:
            return self.tool_calls[step_name]
        except KeyError:
            raise KeyError(
                f"the committed trace holds no tool calls for step {step_name!r}; "
                f"it covers {sorted(self.tool_calls)}"
            ) from None


# Stated in the artifact itself, not only in a README a reader may never open.
PROVENANCE = (
    "The saga shape models an LLM agent's tool-call sequence. It was authored "
    "deterministically from the master seed, not sampled from a live model. "
    "Regenerating it from that seed reproduces the committed file byte for byte. "
    "See ADR-0003."
)


def call_digest(master_seed: int, step_name: str, variant: int) -> bytes:
    """Return the SHA-256 digest every value for one template is derived from.

    Keyed by step name and variant index rather than by a running counter, so a
    template is reproducible on its own and adding a step or widening the pool
    cannot silently shift every template after it. This is the same independence
    property ``core/schedule.py`` gives each run's seed.
    """
    return hashlib.sha256(f"{master_seed}:trace:{step_name}:{variant}".encode()).digest()


def _build_call(master_seed: int, step_name: str, variant: int) -> ToolCall:
    """Return one tool-call template, derived entirely from the digest."""
    digest = call_digest(master_seed, step_name, variant)
    vocabularies = _ARGUMENT_VOCABULARIES[step_name]

    arguments: dict[str, Any] = {}
    # Sorted so the mapping from digest byte to field does not depend on the
    # declaration order of the vocabulary table above.
    for offset, field in enumerate(sorted(vocabularies)):
        choices = vocabularies[field]
        arguments[field] = choices[digest[offset] % len(choices)]

    if step_name == "charge_card":
        span = _AMOUNT_MAX - _AMOUNT_MIN
        arguments[_AMOUNT_FIELD] = _AMOUNT_MIN + int.from_bytes(digest[16:20], "big") % span

    return ToolCall(
        call_id=f"{step_name}-{variant:03d}",
        step_name=step_name,
        tool=step_name,
        arguments=arguments,
    )


def build_trace(settings: Settings) -> AgentTrace:
    """Expand the master seed into the committed agent tool-call trace."""
    missing = [step for step in settings.saga_step_names if step not in _ARGUMENT_VOCABULARIES]
    if missing:
        raise ValueError(
            f"no argument vocabulary is defined for saga step(s) {missing}; "
            f"core/trace.py covers {sorted(_ARGUMENT_VOCABULARIES)}"
        )

    return AgentTrace(
        schema_version=TRACE_SCHEMA_VERSION,
        master_seed=settings.master_seed,
        provenance=PROVENANCE,
        variants_per_step=VARIANTS_PER_STEP,
        step_names=settings.saga_step_names,
        tool_calls={
            step: tuple(
                _build_call(settings.master_seed, step, variant)
                for variant in range(VARIANTS_PER_STEP)
            )
            for step in settings.saga_step_names
        },
    )


def serialize_trace(trace: AgentTrace) -> str:
    """Return the exact text of the committed artifact.

    One canonical form, used by the writer script and the byte-equality gate
    alike, so the two cannot drift. Mirrors ``schedule.serialize_schedule``.
    """
    return json.dumps(trace.to_jsonable(), indent=2, sort_keys=True) + "\n"


def parse_trace(payload: dict[str, Any]) -> AgentTrace:
    """Return the trace held by an already-parsed artifact.

    Kept separate from ``load_trace`` so the parsing is testable without a file,
    and so the only disk read in this module is a single line.
    """
    return AgentTrace(
        schema_version=int(payload["schema_version"]),
        master_seed=int(payload["master_seed"]),
        provenance=str(payload["provenance"]),
        variants_per_step=int(payload["variants_per_step"]),
        step_names=tuple(payload["step_names"]),
        tool_calls={
            step: tuple(
                ToolCall(
                    call_id=str(call["call_id"]),
                    step_name=str(call["step_name"]),
                    tool=str(call["tool"]),
                    arguments=dict(call["arguments"]),
                )
                for call in calls
            )
            for step, calls in payload["tool_calls"].items()
        },
    )


def load_trace(path: Path) -> AgentTrace:
    """Read the committed trace from disk.

    A run reads the trace rather than rebuilding it, so what a run consumed is the
    artifact a reader can inspect. The byte-equality gate is what proves the two
    are the same thing.
    """
    parsed: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return parse_trace(parsed)
