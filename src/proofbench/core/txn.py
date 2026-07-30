"""Observed transaction accounting: counts that were measured, not derived.

Replaces a computed figure with an instrumented one, and the difference is not
cosmetic. PB-T2's ``run.py`` carried this:

    def _committed(result):
        if not result.configuration.transactional:
            return 0
        return len(result.expected) // int(result.schedule_entry["steps_per_saga"])

Under the clean control that returns 200, which looked right and was not: the run
commits one transaction per saga in ingest **and** one per saga in process, so the
observed total is 400 and the committed control evidence understated it by half.
Under a fault it would be wrong without bound, because transactions abort and a
formula cannot know that.

That is INV-P2 one level up. INV-P2 exists so no duplication or loss count is ever
taken by eye; a transaction count inferred from the configuration is the same
defect wearing different clothes, and ADR-0003 section 3 makes aborted counts
load-bearing evidence. This module is what makes them true.

Two things the shape buys, beyond correctness:

- **Per phase and per role.** ``aborted: 1`` is uninterpretable in a kill-run
  matrix: an abort in ingest and an abort in process say different things about
  what the fault did. The ledger keys every count by ``(phase, role)`` so the
  evidence records where an abort happened rather than only that one did.
- **One call site per transactional method.** ``AccountedProducer`` owns the only
  calls to ``init_transactions``, ``begin_transaction``, ``commit_transaction`` and
  ``abort_transaction`` on the run path. A count cannot drift from the calls it
  counts if there is nowhere else to make one, and
  ``tests/unit/test_txn_accounting.py`` walks the AST of the run path to keep it
  that way.

It also times every transaction. ``max_open_transaction_ms`` is written into each
run's evidence because ``transaction.timeout.ms`` is owned by the client pin rather
than by this repository (ADR-0003 section 8), so how close a run came to it is a
measured fact a later reader can hold the apparatus to, not an assumption that a
25-second broker outage left enough headroom.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol

# The two phases that own a producer, and the two producer roles. Named here rather
# than passed as free strings so a typo cannot silently open a third bucket that
# nobody totals.
PHASE_INGEST = "ingest"
PHASE_PROCESS = "process"
PHASES: tuple[str, ...] = (PHASE_INGEST, PHASE_PROCESS)

ROLE_INGEST = "ingest"
ROLE_SINK = "sink"
ROLES: tuple[str, ...] = (ROLE_INGEST, ROLE_SINK)

# The transactional id's role component is the role, so these have to agree with
# configs.transactional_id_for. A test pins that they do.
_SCOPE_SEPARATOR = "/"


class TransactionAccountingError(Exception):
    """A transactional call was made in a state that cannot be accounted for.

    Raised rather than tolerated. A begin inside an open transaction, or a commit
    with none open, means the caller's model of the producer disagrees with the
    producer's own, and a count taken from that disagreement would be fiction.
    """


class TransactionalClient(Protocol):
    """The four calls plus flush that a transactional producer has to offer.

    Structural rather than an import of ``confluent_kafka.Producer``, so the
    accounting is testable without a broker and without the client. The pinned
    client is verified to carry all five by
    ``tests/unit/test_client_contract.py``.
    """

    def init_transactions(self, timeout: float) -> None: ...

    def begin_transaction(self) -> None: ...

    def commit_transaction(self, timeout: float) -> None: ...

    def abort_transaction(self, timeout: float) -> None: ...


@dataclass(slots=True)
class ScopeCounts:
    """What one ``(phase, role)`` pair did, counted at the call site."""

    inits: int = 0
    begins: int = 0
    commits: int = 0
    aborts: int = 0

    def to_jsonable(self) -> dict[str, int]:
        return {
            "inits": self.inits,
            "begins": self.begins,
            "commits": self.commits,
            "aborts": self.aborts,
        }


@dataclass(slots=True)
class TransactionLedger:
    """Observed transactional activity for one run, keyed by phase and role.

    Serializable and mergeable, because a run that was SIGKILLed and restarted
    produces one of these per attempt and the evidence has to carry the total. A
    ledger that reset on restart would report the last attempt's activity as though
    it were the run's, which is the same class of untruth as a derived count.
    """

    scopes: dict[str, ScopeCounts] = field(default_factory=dict)
    max_open_ms: float = 0.0

    @staticmethod
    def scope_key(phase: str, role: str) -> str:
        """The ledger key for one phase and role, validated on the way in."""
        if phase not in PHASES:
            raise TransactionAccountingError(
                f"unknown phase {phase!r}; the run path has exactly {list(PHASES)}"
            )
        if role not in ROLES:
            raise TransactionAccountingError(
                f"unknown producer role {role!r}; ADR-0003 section 6 names exactly {list(ROLES)}"
            )
        return f"{phase}{_SCOPE_SEPARATOR}{role}"

    def counts(self, phase: str, role: str) -> ScopeCounts:
        """The counts for one scope, created on first use."""
        return self.scopes.setdefault(self.scope_key(phase, role), ScopeCounts())

    @property
    def committed(self) -> int:
        """Transactions this run actually committed, across every scope."""
        return sum(scope.commits for scope in self.scopes.values())

    @property
    def aborted(self) -> int:
        """Transactions this run actually aborted, across every scope."""
        return sum(scope.aborts for scope in self.scopes.values())

    def observe_open_ms(self, elapsed_ms: float) -> None:
        """Record how long one transaction stayed open, keeping the maximum."""
        self.max_open_ms = max(self.max_open_ms, elapsed_ms)

    def merge(self, other: TransactionLedger) -> None:
        """Add another attempt's activity into this one.

        Additive per scope, and the maximum for the duration, because "the longest
        any transaction stayed open during this run" is a maximum over attempts
        rather than a sum.
        """
        for key, counts in other.scopes.items():
            mine = self.scopes.setdefault(key, ScopeCounts())
            mine.inits += counts.inits
            mine.begins += counts.begins
            mine.commits += counts.commits
            mine.aborts += counts.aborts
        self.max_open_ms = max(self.max_open_ms, other.max_open_ms)

    def to_jsonable(self) -> dict[str, Any]:
        """The form written into a run's evidence.

        The per-scope breakdown comes first because it is the part that makes an
        abort interpretable; the totals are derived from it and are there so a
        reader does not have to add up four numbers to check one.
        """
        return {
            "by_phase_and_role": {
                key: self.scopes[key].to_jsonable() for key in sorted(self.scopes)
            },
            "committed": self.committed,
            "aborted": self.aborted,
            "max_open_transaction_ms": round(self.max_open_ms, 3),
        }

    @classmethod
    def from_jsonable(cls, payload: dict[str, Any]) -> TransactionLedger:
        """Rebuild a ledger the parent wrote out before a child was killed."""
        ledger = cls()
        for key, counts in dict(payload.get("by_phase_and_role", {})).items():
            ledger.scopes[str(key)] = ScopeCounts(
                inits=int(counts["inits"]),
                begins=int(counts["begins"]),
                commits=int(counts["commits"]),
                aborts=int(counts["aborts"]),
            )
        ledger.max_open_ms = float(payload.get("max_open_transaction_ms", 0.0))
        return ledger


class AccountedProducer:
    """A transactional producer whose four bracket calls are counted as they happen.

    The only place on the run path that calls ``init_transactions``,
    ``begin_transaction``, ``commit_transaction`` or ``abort_transaction``. That is
    enforced by an AST gate rather than by convention, because a second call site
    would be a transaction nobody counted, and the point of this module is that the
    numbers in the evidence are the numbers the run produced.

    **The wrapper's method names deliberately differ from the client's.** It offers
    ``init``, ``begin``, ``commit`` and ``abort`` where the client offers
    ``init_transactions``, ``begin_transaction``, ``commit_transaction`` and
    ``abort_transaction``. That is not stylistic: the AST gate distinguishes an
    accounted call from a raw one by name alone, and identical names would make the
    two indistinguishable and the gate unenforceable. The gate found this itself,
    on the first run, when ``init_transactions`` still shared its name.

    It also tracks whether a transaction is open, which two other rules depend on:
    the accounting itself (a commit with nothing open is a bug, not a count), and
    the D4 hold, which must never sit inside an open transaction because the
    combined open-transaction bound in ADR-0004 drops the hold's duration from the
    transaction-timeout budget on exactly that grounds.
    """

    __slots__ = ("_client", "_ledger", "_phase", "_role", "_opened_at", "_timeout_s")

    def __init__(
        self,
        client: TransactionalClient,
        ledger: TransactionLedger,
        phase: str,
        role: str,
        timeout_s: float,
    ) -> None:
        self._client = client
        self._ledger = ledger
        # Validated eagerly, so an unknown phase or role fails where it was passed
        # rather than at the first commit.
        TransactionLedger.scope_key(phase, role)
        self._phase = phase
        self._role = role
        self._timeout_s = timeout_s
        self._opened_at: float | None = None

    @property
    def transaction_open(self) -> bool:
        """True between a begin and its commit or abort."""
        return self._opened_at is not None

    def _counts(self) -> ScopeCounts:
        return self._ledger.counts(self._phase, self._role)

    def init(self) -> None:
        """Fence the previous epoch for this transactional id and start a new one.

        Counted because it is the recovery action ADR-0003 section 6 bounds at
        three per run: a clean result that needed three re-initialisations says
        something quite different about the apparatus than one that needed none.
        """
        self._client.init_transactions(self._timeout_s)
        self._counts().inits += 1

    def begin(self) -> None:
        if self._opened_at is not None:
            raise TransactionAccountingError(
                f"{self._phase}/{self._role}: begin_transaction with a transaction "
                f"already open; the caller's model of the producer is wrong and any "
                f"count taken from it would be fiction"
            )
        self._client.begin_transaction()
        self._counts().begins += 1
        self._opened_at = time.monotonic()

    def commit(self) -> None:
        self._close("commit_transaction")
        self._client.commit_transaction(self._timeout_s)
        self._counts().commits += 1

    def abort(self) -> None:
        self._close("abort_transaction")
        self._client.abort_transaction(self._timeout_s)
        self._counts().aborts += 1

    def _close(self, call: str) -> None:
        """Record the open duration and clear the open state, or refuse.

        The duration is recorded before the call rather than after, so a commit
        that itself blocks for a coordinator re-election does not inflate the
        measure of how long the transaction was open holding records. What the
        bound in ADR-0004 cares about is the span the broker sees as open, and the
        client's own timeout covers the call.
        """
        if self._opened_at is None:
            raise TransactionAccountingError(
                f"{self._phase}/{self._role}: {call} with no transaction open"
            )
        self._ledger.observe_open_ms((time.monotonic() - self._opened_at) * 1000.0)
        self._opened_at = None

    def forget_open_transaction(self) -> None:
        """Drop the open-transaction state without calling the client.

        For exactly one case: the producer is dead (``fatal()``, which is where
        fencing lands) and is about to be discarded and rebuilt with the same
        transactional id. Its open transaction cannot be committed or aborted
        through this object, so pretending otherwise would raise on the next
        begin. The transaction is not counted as aborted here, because this
        process did not abort it: the next epoch's ``init_transactions`` does, and
        that is counted as an init.
        """
        self._opened_at = None


@dataclass(frozen=True, slots=True)
class TransactionSpan:
    """One bracketed transaction, for callers that want the shape rather than the calls."""

    producer: AccountedProducer

    def __enter__(self) -> AccountedProducer:
        self.producer.begin()
        return self.producer

    def __exit__(self, exc_type: type[BaseException] | None, *_: object) -> None:
        # Deliberately no abort-on-exception here. Under the recovery contract an
        # exception is classified before anything is decided (retry, abort and
        # replay, or discard the producer), and a context manager that aborted
        # unconditionally would make that classification unreachable.
        return None


def scopes_in_order(ledger: TransactionLedger) -> Iterator[tuple[str, ScopeCounts]]:
    """Yield the ledger's scopes in a stable order, for reporting."""
    for key in sorted(ledger.scopes):
        yield key, ledger.scopes[key]
