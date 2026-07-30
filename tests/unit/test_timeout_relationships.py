"""The timeouts have to hold specific relationships to each other, so they are gated.

PB-T3 puts six durations in play at once: a 25s broker outage, a 15s delivery
deadline, a 10s hold at the fault point, a 6s consumer session, a 60s flush and poll
bound, and a 60s transaction timeout owned by the client pin. Several of them are
nested inside others, and every one of those nestings is load-bearing.

They were module literals in PB-T2, where nothing was nested inside anything and the
values could not collide. That stops being true the moment a fault takes the broker
away for 25 seconds while a transaction is open, so the relationships are asserted
here rather than left to hold by coincidence.

The bound that matters most is the last one. On a broker run under the good
configuration the outage sits **inside** an open per-saga transaction, followed by a
coordinator reload, an abort, and a replay, against a transaction timeout this
repository does not own. A transaction that timed out would present as a fatal error,
consume recovery budget, and land as ``apparatus_failure``, which under the
matrix-validity rule voids the entire matrix from a single run. "Probably enough
headroom" is not a gate; this is.
"""

from __future__ import annotations

import pytest

from proofbench.config import Settings
from proofbench.core.run import _FLUSH_TIMEOUT_S, _TXN_TIMEOUT_S
from proofbench.core.txn import PINNED_TRANSACTION_TIMEOUT_MS


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings(_env_file=None)


# --------------------------------------------------------------------------
# The combined open-transaction bound
# --------------------------------------------------------------------------


def test_the_outage_plus_headroom_fits_inside_the_pinned_transaction_timeout(
    settings: Settings,
) -> None:
    """The gate the plan review asked for, and the one that would have bitten.

    On a broker run under the good configuration the sequence inside one open
    transaction is: the outage, then the coordinator reload the client logs on
    reconnection, then the abort, then the replay of one saga. All of it has to
    finish inside a transaction timeout that ADR-0003 section 8 leaves to the client
    pin and that PB-T3 may not restate.

    Proven red by raising broker_outage_ms past 40000.
    """
    committed = settings.broker_outage_ms + settings.txn_headroom_ms
    assert committed <= PINNED_TRANSACTION_TIMEOUT_MS, (
        f"a broker run would commit {committed}ms of a "
        f"{PINNED_TRANSACTION_TIMEOUT_MS}ms transaction timeout ({settings.broker_outage_ms}ms "
        f"outage plus {settings.txn_headroom_ms}ms headroom). A transaction that timed out "
        f"would land as apparatus_failure and void the matrix from a single run."
    )


def test_the_bound_leaves_slack_rather_than_only_just_fitting(settings: Settings) -> None:
    """A bound satisfied with nothing to spare is a bound that fails on a busy host.

    Stated as its own assertion so that shrinking the slack is a visible decision
    rather than a side effect of nudging one of the two terms.
    """
    slack = PINNED_TRANSACTION_TIMEOUT_MS - (settings.broker_outage_ms + settings.txn_headroom_ms)
    assert slack >= 10_000, f"only {slack}ms of slack inside the pinned transaction timeout"


def test_excluding_the_hold_is_what_buys_the_slack(settings: Settings) -> None:
    """The precise value of moving the hold outside the transaction bracket.

    Stated exactly, because the imprecise version is tempting and wrong. Including
    the hold would **not** breach the bound: 25s outage plus 20s headroom plus 10s
    hold is 55s of a 60s budget, which fits. What it would do is cut the slack from
    15s to 5s, taking it below the comfort floor the test above asserts, so a busy
    host could push a broker run's transaction into a timeout that presented as a
    fatal error and voided the matrix.

    So the exclusion buys margin rather than legality, and that is the honest claim.
    The exclusion is only sound because no hold sits inside a transaction, which
    AccountedProducer enforces by refusing a hold while one is open. This test
    records the dependency, so moving the hold back inside a bracket makes the loss
    of margin visible instead of silent.
    """
    without_hold = PINNED_TRANSACTION_TIMEOUT_MS - (
        settings.broker_outage_ms + settings.txn_headroom_ms
    )
    with_hold = without_hold - settings.fault_hold_ms

    assert with_hold >= 0, (
        "the hold inside a transaction would now breach the bound outright rather "
        "than merely eating the margin, which changes ADR-0004's reasoning from a "
        "margin argument into a correctness one"
    )
    assert with_hold < 10_000 <= without_hold, (
        f"excluding the hold no longer changes which side of the comfort floor the "
        f"slack falls on ({with_hold}ms with it, {without_hold}ms without), so this "
        f"test has stopped recording a real dependency"
    )


def test_the_transactional_call_timeout_exceeds_the_outage(settings: Settings) -> None:
    """A commit or abort issued during the outage must be allowed to ride it out.

    _TXN_TIMEOUT_S is what the harness passes to commit_transaction and
    abort_transaction. If it were shorter than the outage, the call would give up
    while the broker was still down and the recovery contract would classify a
    harness impatience as a broker condition.
    """
    assert settings.broker_outage_ms < _TXN_TIMEOUT_S * 1000


# --------------------------------------------------------------------------
# What makes a broker outage able to lose a side effect at all
# --------------------------------------------------------------------------


def test_the_outage_outlasts_the_delivery_deadline(settings: Settings) -> None:
    """The only route by which a broker outage can produce a lost side effect.

    Under commit-before-processing, loss requires a committed offset ahead of applied
    work at the moment the phase gives up on that work. A send that merely slows down
    and then succeeds loses nothing. So the outage has to outlast
    message.timeout.ms, or the in-flight sink write completes when the broker returns
    and the six broker runs contribute nothing to anything.
    """
    assert settings.broker_outage_ms > settings.producer_message_timeout_ms, (
        f"a {settings.broker_outage_ms}ms outage does not outlast a "
        f"{settings.producer_message_timeout_ms}ms delivery deadline, so an in-flight "
        f"send would merely be slow and no side effect could be lost"
    )


def test_the_outage_outlasts_the_baseline_commit_interval(settings: Settings) -> None:
    """So the stored-but-unapplied offset is actually committed during the outage.

    This is why the broker runs need no separate hold: the outage is five times the
    commit interval, so the timer fires inside it regardless.
    """
    assert settings.broker_outage_ms > settings.baseline_auto_commit_interval_ms


def test_the_delivery_deadline_sits_below_the_flush_bound(settings: Settings) -> None:
    """What licenses treating a still-queued record as an apparatus break.

    _Sender.flush raises DeliveryFailure directly, without consulting the fault
    window, when records remain queued after the flush timeout. That is only correct
    if librdkafka was obliged to resolve every queued record inside that window, by
    delivery or by a reported error, which is exactly what message.timeout.ms below
    the flush bound guarantees. Reverse the two and a record still in the queue would
    be ambiguous, and a broker outage would surface as an apparatus failure instead
    of as a measurement.
    """
    assert settings.producer_message_timeout_ms < _FLUSH_TIMEOUT_S * 1000, (
        "message.timeout.ms is not below the flush bound, so a still-queued record no "
        "longer distinguishes a stuck client from an absent broker, and the sender's "
        "decision to treat it as an apparatus break is unsound"
    )


# --------------------------------------------------------------------------
# The consume loop has to outlast a rebalance plus an outage
# --------------------------------------------------------------------------


def test_the_stall_budget_outlasts_a_whole_broker_fault(settings: Settings) -> None:
    """The bound that decides whether a working fault reads as an apparatus failure.

    On a broker run the consumer legitimately makes no progress for the entire
    outage, and then for the coordinator reload and recovery that follow it. That is
    exactly ``broker_outage_ms + txn_headroom_ms``, the same span the combined
    open-transaction bound reserves. A stall budget below it would turn a fault that
    worked into a false ``apparatus_failure``, and under matrix-validity rule 4 a
    single one of those voids the entire matrix.

    This is the same failure mode as the batch-wait defect one layer up: a timeout
    chosen for the no-fault case, applied to a run whose whole point is that nothing
    happens for 25 seconds.

    Proven red by lowering the stall budget below the outage.
    """
    legitimate_ms = settings.broker_outage_ms + settings.txn_headroom_ms
    assert legitimate_ms < settings.consume_stall_budget_ms, (
        f"the stall budget is {settings.consume_stall_budget_ms}ms but a broker run "
        f"can legitimately make no progress for {legitimate_ms}ms "
        f"({settings.broker_outage_ms}ms outage plus {settings.txn_headroom_ms}ms of "
        f"coordinator reload and recovery). A working fault would be recorded as an "
        f"apparatus failure and void the matrix."
    )


def test_the_stall_budget_also_outlasts_a_rejoin_plus_an_outage(settings: Settings) -> None:
    """The other route to a legitimate silence, which is not the same span.

    A restarted process phase waits out the dead member's session before the
    coordinator completes the rebalance. That can compose with an outage, so both
    bounds are asserted rather than assuming one dominates.
    """
    worst_case_ms = settings.consumer_session_timeout_ms + settings.broker_outage_ms
    assert worst_case_ms < settings.consume_stall_budget_ms


def test_the_batch_wait_is_short_and_the_stall_budget_is_not(settings: Settings) -> None:
    """The two quantities that consume forced apart, kept apart.

    PB-T2 used one number for both, because with ``poll`` they coincided: a call
    returned one record or nothing, so "this call timed out" and "the topic stalled"
    were the same event. ``consume`` waits for the batch to FILL, so a 60s batch wait
    is paid in full on the last call of every run, where all that is left to collect
    is a single partition-EOF event. Measured at 62s per phase against 1.2s of real
    work before the two were split, and 3.4s after.

    The batch wait therefore has to be short, and the stall budget has to stay long
    enough to cover a legitimate outage. Collapsing them back into one number breaks
    one or the other, so the relationship is asserted rather than left to memory.
    """
    assert settings.consume_batch_wait_ms <= 5000, (
        "the per-call batch wait is long enough to be paid on every run's final call, "
        "which is the 60-second cost consume's fill-the-batch semantics produced"
    )
    assert settings.consume_batch_wait_ms < settings.consume_stall_budget_ms


def test_the_batch_wait_is_short_enough_to_not_dominate_a_run(settings: Settings) -> None:
    """It is paid once per empty return, so it bounds how coarsely a stall is detected.

    Kept well under the stall budget so the budget is measured in many short waits
    rather than in three long ones, which is what makes "no progress for N seconds"
    mean roughly N rather than up to N plus one batch wait.
    """
    assert settings.consume_stall_budget_ms >= 10 * settings.consume_batch_wait_ms


def test_the_session_timeout_is_at_or_above_the_broker_floor(settings: Settings) -> None:
    """Below the broker's group.min.session.timeout.ms the join is refused.

    That would surface as every process restart failing in the middle of the matrix
    rather than as a failed build, which is the worst place to find it.
    """
    assert settings.consumer_session_timeout_ms >= 6000


# --------------------------------------------------------------------------
# The hold gives the mechanism a full opportunity
# --------------------------------------------------------------------------


def test_the_hold_spans_at_least_two_commit_intervals(settings: Settings) -> None:
    """One interval does not guarantee a tick AFTER the offsets were stored.

    The commit timer is periodic from consumer construction, so a tick can land
    microseconds before the store. Two intervals guarantee one after it, with margin
    for the commit round trip. That is the whole content of the D4 repair: it decides
    whether the mechanism acts, never how much is lost.
    """
    assert settings.fault_hold_intervals >= 2
    assert settings.fault_hold_ms >= 2 * settings.baseline_auto_commit_interval_ms


def test_the_hold_is_derived_from_the_frozen_interval_not_declared(settings: Settings) -> None:
    """So it cannot drift away from the mechanism it exists to give time to.

    Changing the frozen interval has to change the hold with it. A separately
    declared duration would be a second authority, and the two would eventually
    disagree without anything noticing.
    """
    assert settings.fault_hold_ms == (
        settings.fault_hold_intervals * settings.baseline_auto_commit_interval_ms
    )
    stretched = settings.model_copy(update={"baseline_auto_commit_interval_ms": 7000})
    assert stretched.fault_hold_ms == 14_000


# --------------------------------------------------------------------------
# None of this reached the frozen artifact
# --------------------------------------------------------------------------


def test_the_new_durations_are_absent_from_the_frozen_schedule(settings: Settings) -> None:
    """PB-T3 may not touch docs/run_schedule.json, so it must not have to.

    The artifact's constants block is an explicit whitelist, so a new Settings field
    does not reach it. Asserted rather than trusted, because the byte-equality gate
    would catch it only after the file had already been rewritten.
    """
    import json

    from proofbench.config import repo_root

    text = (repo_root() / settings.schedule_path).read_text(encoding="utf-8")
    payload = json.loads(text)
    constants = payload["constants"]
    flattened = json.dumps(constants)

    for name in (
        "producer_message_timeout_ms",
        "consumer_session_timeout_ms",
        "broker_outage_ms",
        "txn_headroom_ms",
        "fault_hold_intervals",
    ):
        assert name not in flattened, f"{name} reached the frozen schedule artifact"

    # And the values themselves are not hiding in there under another name. The key is
    # asserted present first: .get with a default would return an empty mapping if the
    # artifact's shape changed, and "not in {}" is true of everything.
    assert "client_tuning" in constants, "the artifact has no client_tuning block to check"
    assert constants["client_tuning"], "the client_tuning block is empty, so this checks nothing"
    assert settings.broker_outage_ms not in constants["client_tuning"].values()
    assert settings.producer_message_timeout_ms not in constants["client_tuning"].values()


def test_the_harness_never_sets_the_transaction_timeout(settings: Settings) -> None:
    """ADR-0003 section 8 leaves it to the pin, and restating it needs its own ADR.

    So the constant PB-T3 reasons about is an observation, and the configurations
    must not carry the property at all.
    """
    from proofbench.core.configs import build_both

    both = build_both(0, settings.model_copy(update={"broker_bootstrap_servers": "placeholder:1"}))
    for configuration in both.values():
        for section in configuration.client_sections().values():
            assert "transaction.timeout.ms" not in section
