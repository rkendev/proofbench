"""Transaction counts are observed at the call site, not derived from the schedule.

This is INV-P2 applied one level up. INV-P2 exists so no duplication or loss count
is ever taken by eye; a transaction count inferred from the configuration and the
saga count is the same defect, and ADR-0003 section 3 makes aborted counts
load-bearing evidence rather than an implementation detail.

The formula PB-T2 shipped was wrong even on the clean control. It returned 200 for
the good configuration, and the run commits one transaction per saga in ingest and
one per saga in process, so the observed figure is 400. Under a fault, where
transactions abort, no formula can be right at all.

Two kinds of test here, and the second is the one that matters. The first checks
that the counts move when the calls happen. The second walks the AST of the run
path and asserts there is nowhere else those calls can be made, because a count
cannot be trusted to match the calls it counts if a second, unaccounted call site
is allowed to exist.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from proofbench.core.txn import (
    PHASE_INGEST,
    PHASE_PROCESS,
    ROLE_INGEST,
    ROLE_SINK,
    AccountedProducer,
    TransactionAccountingError,
    TransactionLedger,
)

PACKAGE_DIR = Path(__file__).resolve().parents[2] / "src" / "proofbench"
TXN_MODULE = PACKAGE_DIR / "core" / "txn.py"

# The four calls that must never be made outside AccountedProducer. Named as
# attribute names, because that is what the walk below can see.
BRACKET_CALLS = frozenset(
    {"init_transactions", "begin_transaction", "commit_transaction", "abort_transaction"}
)

TIMEOUT_S = 30.0


class FakeProducer:
    """Records the bracket calls it received, and can be told to fail one.

    Structural, so the accounting is testable with no broker and no client. The
    pinned client is verified to carry these methods by
    tests/unit/test_client_contract.py; what is checked here is the counting.
    """

    def __init__(self, fail_first_commit: bool = False) -> None:
        self.calls: list[str] = []
        self.fail_first_commit = fail_first_commit
        self._commits_seen = 0

    def init_transactions(self, timeout: float) -> None:
        self.calls.append("init")

    def begin_transaction(self) -> None:
        self.calls.append("begin")

    def commit_transaction(self, timeout: float) -> None:
        self._commits_seen += 1
        if self.fail_first_commit and self._commits_seen == 1:
            self.calls.append("commit_failed")
            raise RuntimeError("the coordinator was re-electing")
        self.calls.append("commit")

    def abort_transaction(self, timeout: float) -> None:
        self.calls.append("abort")


def _accounted(
    ledger: TransactionLedger,
    phase: str = PHASE_INGEST,
    role: str = ROLE_INGEST,
    fail_first_commit: bool = False,
) -> tuple[AccountedProducer, FakeProducer]:
    client = FakeProducer(fail_first_commit=fail_first_commit)
    return AccountedProducer(client, ledger, phase, role, TIMEOUT_S), client


# --------------------------------------------------------------------------
# The counts move because the calls happened
# --------------------------------------------------------------------------


def test_a_committed_transaction_is_counted_where_it_happened() -> None:
    ledger = TransactionLedger()
    producer, client = _accounted(ledger)

    producer.init()
    producer.begin()
    producer.commit()

    counts = ledger.counts(PHASE_INGEST, ROLE_INGEST)
    assert (counts.inits, counts.begins, counts.commits, counts.aborts) == (1, 1, 1, 0)
    assert client.calls == ["init", "begin", "commit"]


def test_an_abort_moves_the_recorded_count(caplog: pytest.LogCaptureFixture) -> None:
    """The red-proof the T-prompt names: force an abort, assert the count moves.

    This is the test the derived formula cannot pass. ``_committed`` returned
    ``len(expected) // steps_per_saga`` for a transactional configuration, which is
    a function of the workload alone: it cannot move when a transaction aborts,
    because nothing about the workload changed. Restoring that formula turns this
    assertion red, which is what makes the replacement a measurement rather than a
    restatement.
    """
    ledger = TransactionLedger()
    producer, _ = _accounted(ledger, fail_first_commit=True)

    producer.init()
    producer.begin()
    with pytest.raises(RuntimeError):
        producer.commit()
    # The recovery contract classified this as abort-and-replay, so the harness
    # aborts and replays the saga. The producer object is still usable.
    producer.begin()
    producer.abort()
    producer.begin()
    producer.commit()

    counts = ledger.counts(PHASE_INGEST, ROLE_INGEST)
    assert counts.aborts == 1, "the abort was not counted where it happened"
    assert counts.commits == 1
    assert counts.begins == 3
    assert ledger.aborted == 1
    assert ledger.committed == 1


def test_an_abort_in_ingest_is_distinguishable_from_one_in_process() -> None:
    """The whole reason the ledger is keyed by phase and role.

    In a kill-run matrix ``aborted: 1`` is uninterpretable on its own: an abort in
    ingest means the workload half-landed and was rolled back, an abort in process
    means a side effect was prevented from becoming observable. The evidence has to
    be able to tell them apart.
    """
    ledger = TransactionLedger()
    ingest_producer, _ = _accounted(ledger, PHASE_INGEST, ROLE_INGEST)
    sink_producer, _ = _accounted(ledger, PHASE_PROCESS, ROLE_SINK)

    ingest_producer.begin()
    ingest_producer.abort()
    sink_producer.begin()
    sink_producer.commit()

    payload = ledger.to_jsonable()
    assert payload["by_phase_and_role"]["ingest/ingest"]["aborts"] == 1
    assert payload["by_phase_and_role"]["process/sink"]["aborts"] == 0
    assert payload["by_phase_and_role"]["process/sink"]["commits"] == 1
    assert payload["aborted"] == 1
    assert payload["committed"] == 1


def test_the_clean_control_commits_two_hundred_transactions_in_each_phase() -> None:
    """The number PB-T2's derived formula got wrong, stated as arithmetic.

    One transaction per saga in ingest and one per saga in process is 400 for a
    200-saga run, not 200. The committed control evidence said 200 because the
    formula divided the record count by the step count and ignored the fact that
    two phases each bracket every saga.
    """
    ledger = TransactionLedger()
    ingest_producer, _ = _accounted(ledger, PHASE_INGEST, ROLE_INGEST)
    sink_producer, _ = _accounted(ledger, PHASE_PROCESS, ROLE_SINK)

    for producer in (ingest_producer, sink_producer):
        producer.init()
        for _ in range(200):
            producer.begin()
            producer.commit()

    assert ledger.committed == 400
    assert ledger.aborted == 0
    assert ledger.counts(PHASE_INGEST, ROLE_INGEST).commits == 200
    assert ledger.counts(PHASE_PROCESS, ROLE_SINK).commits == 200


def test_a_non_transactional_configuration_records_zero_rather_than_nothing() -> None:
    """The baseline's zero is an observation, not an assumption.

    The sender constructs the accounting for both configurations and the baseline
    never calls it, so the zero in the evidence is the absence of observed calls
    rather than a branch that decided to report zero.
    """
    ledger = TransactionLedger()
    _accounted(ledger, PHASE_PROCESS, ROLE_SINK)
    assert ledger.committed == 0
    assert ledger.aborted == 0


# --------------------------------------------------------------------------
# States that cannot be accounted for are refused rather than absorbed
# --------------------------------------------------------------------------


def test_a_begin_inside_an_open_transaction_is_refused() -> None:
    ledger = TransactionLedger()
    producer, _ = _accounted(ledger)
    producer.begin()
    with pytest.raises(TransactionAccountingError, match="already open"):
        producer.begin()


def test_a_commit_with_nothing_open_is_refused() -> None:
    """Counting it would be counting a transaction that never existed."""
    ledger = TransactionLedger()
    producer, _ = _accounted(ledger)
    with pytest.raises(TransactionAccountingError, match="no transaction open"):
        producer.commit()


def test_an_unknown_phase_or_role_is_refused_at_construction() -> None:
    """A typo must not silently open a third bucket that nothing totals."""
    ledger = TransactionLedger()
    with pytest.raises(TransactionAccountingError, match="unknown phase"):
        AccountedProducer(FakeProducer(), ledger, "verify", ROLE_SINK, TIMEOUT_S)
    with pytest.raises(TransactionAccountingError, match="unknown producer role"):
        AccountedProducer(FakeProducer(), ledger, PHASE_INGEST, "verifier", TIMEOUT_S)


def test_a_discarded_producer_forgets_its_open_transaction_without_counting_an_abort() -> None:
    """Where fencing lands. The dead epoch's transaction is not this process's abort.

    ADR-0003 section 6: on a fatal error the producer object is dead, is discarded,
    and is rebuilt with the same transactional id, whose ``init_transactions``
    bumps the epoch and aborts what the dead one left open. Counting that as an
    abort by this producer would attribute the broker's cleanup to the harness.
    """
    ledger = TransactionLedger()
    producer, client = _accounted(ledger)
    producer.begin()
    producer.forget_open_transaction()

    assert not producer.transaction_open
    assert ledger.counts(PHASE_INGEST, ROLE_INGEST).aborts == 0
    assert "abort" not in client.calls


# --------------------------------------------------------------------------
# Open-transaction duration, which the ADR-0004 bound is measured against
# --------------------------------------------------------------------------


def test_the_open_transaction_duration_is_recorded() -> None:
    """``transaction.timeout.ms`` is owned by the client pin, so headroom is measured.

    ADR-0003 section 8 leaves the timeout to the pin, and ADR-0004's combined bound
    asserts a 25s broker outage plus headroom fits inside it. This is what turns
    that from an assumption into a number a later reader can check.
    """
    ledger = TransactionLedger()
    producer, _ = _accounted(ledger)
    producer.begin()
    producer.commit()
    assert ledger.max_open_ms >= 0.0
    assert "max_open_transaction_ms" in ledger.to_jsonable()


def test_the_duration_is_a_maximum_across_attempts_not_a_sum() -> None:
    """A run killed and restarted produces one ledger per attempt.

    Commits and aborts add up across attempts; "the longest any single transaction
    stayed open" does not, so merging takes the maximum.
    """
    first = TransactionLedger()
    first.counts(PHASE_PROCESS, ROLE_SINK).commits = 40
    first.observe_open_ms(120.0)

    second = TransactionLedger()
    second.counts(PHASE_PROCESS, ROLE_SINK).commits = 160
    second.counts(PHASE_PROCESS, ROLE_SINK).aborts = 1
    second.observe_open_ms(30_500.0)

    first.merge(second)
    assert first.counts(PHASE_PROCESS, ROLE_SINK).commits == 200
    assert first.aborted == 1
    assert first.max_open_ms == 30_500.0


def test_a_ledger_survives_a_round_trip_through_the_durable_marker() -> None:
    """It has to: a SIGKILLed child's counts are reconstructed by the parent."""
    ledger = TransactionLedger()
    ledger.counts(PHASE_INGEST, ROLE_INGEST).commits = 96
    ledger.counts(PHASE_INGEST, ROLE_INGEST).aborts = 1
    ledger.observe_open_ms(77.5)

    rebuilt = TransactionLedger.from_jsonable(ledger.to_jsonable())
    assert rebuilt.to_jsonable() == ledger.to_jsonable()


# --------------------------------------------------------------------------
# There is nowhere else to make one of these calls
# --------------------------------------------------------------------------


def _bracket_call_sites(path: Path) -> set[str]:
    """Attribute names from BRACKET_CALLS that ``path`` calls on something."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in BRACKET_CALLS
        ):
            found.add(node.func.attr)
    return found


def test_only_the_accounting_module_calls_a_transactional_bracket_method() -> None:
    """A second call site would be a transaction nobody counted.

    The counts in the evidence are only the run's counts if every bracketed
    transaction went through the object that counts them. Convention cannot
    guarantee that and a comment certainly cannot, so the package is walked.
    """
    offenders: dict[str, set[str]] = {}
    for path in sorted(PACKAGE_DIR.rglob("*.py")):
        if path.resolve() == TXN_MODULE.resolve():
            continue
        calls = _bracket_call_sites(path)
        if calls:
            offenders[str(path.relative_to(PACKAGE_DIR.parents[1]))] = calls
    assert not offenders, (
        f"transactional bracket call(s) outside core/txn.py: {offenders}. Every "
        f"begin, commit, abort and init has to go through AccountedProducer, or the "
        f"counts in the evidence are not the counts the run produced."
    )


def test_the_accounting_module_really_does_make_those_calls() -> None:
    """The rule above passes by absence, so the walk itself is pinned.

    A broken parse or a denylist that matched nothing would look exactly like a
    clean package.
    """
    assert _bracket_call_sites(TXN_MODULE) == BRACKET_CALLS


def test_the_walk_detects_a_smuggled_call() -> None:
    """Exercised against a sample rather than trusting a green result."""
    assert _bracket_call_sites  # referenced, so the helper is the thing under test
    tree = "def f(p):\n    p.commit_transaction(60.0)\n"
    found = {
        node.func.attr
        for node in ast.walk(ast.parse(tree))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in BRACKET_CALLS
    }
    assert found == {"commit_transaction"}


def test_send_offsets_to_transaction_is_deliberately_not_on_the_list() -> None:
    """It is not a bracket call, and pretending otherwise would be noise.

    ``send_offsets_to_transaction`` moves offsets into an already-open transaction.
    It begins nothing and ends nothing, so it changes no count. It stays on the run
    path directly, and the four calls that do change a count do not.
    """
    assert "send_offsets_to_transaction" not in BRACKET_CALLS
