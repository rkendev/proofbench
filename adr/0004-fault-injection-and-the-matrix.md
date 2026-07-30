# ADR-0004: Fault injection, the matrix, and what C2 can attain

Status: accepted
Date: 2026-07-30
Supersedes: nothing. ADR-0002 and ADR-0003 remain authoritative in full.

## Context

PB-T3 injects the faults, executes the 42 matrix executions, and evaluates C1, C2 and
C3 against the floors CLAIMS.md froze on 2026-07-28.

**This document is written before the matrix executes.** Everything below, including
the prediction that C2 cannot reach its floor, is recorded while the result is still
unknown. That ordering is the whole point, and it is the same argument ADR-0003 makes
about the control run: a decision recorded after its consequences are visible is not the
same decision.

## The principle every repair below is measured against

Stated first, because five apparatus repairs follow and each one needs a test that is
not "did it help":

> **A repair that moves the CODE toward what was already FROZEN is a validity repair. A
> repair that moves the DECLARATION toward a desired result is tuning. The first is
> required, the second is forbidden.**

ADR-0003 section 1 already supplies the reason: an apparatus in which the claim is not
testable is not a neutral choice. A design that forces a claim to fail and one that
forces it to pass are equally uninformative.

---

## 1. Observed transaction accounting, and why a derived count is not evidence

`run.py` computed committed transactions from the configuration and the saga count.
Under the clean control it returned 200. The run commits one transaction per saga in
ingest **and** one per saga in process, so the observed figure is 400, and the committed
control evidence understated it by half. Under a fault it would be wrong without bound,
because transactions abort and a formula cannot know that.

That is INV-P2 one level up. INV-P2 exists so no duplication or loss count is ever taken
by eye; a transaction count inferred from the configuration is the same defect wearing
different clothes, and ADR-0003 section 3 already makes aborted counts load-bearing
evidence.

`core/txn.py` instruments the four bracket calls and counts them per `(phase, role)`,
because `aborted: 1` is uninterpretable in a kill-run matrix without knowing whether the
abort was in ingest or in process. An AST gate holds those four calls to one call site.

It also times every transaction. `transaction.timeout.ms` is owned by the client pin
(ADR-0003 section 8), so `max_open_transaction_ms` is recorded per run and the headroom
is a measured fact rather than an assumption. The clean control measures 53ms.

## 2. The fault-window boundary

PB-T2 escalated every delivery failure to `ApparatusFailure`, which is never scored.
Under `broker_stop_start` a produce during the outage genuinely fails, so all twelve
broker executions would have ended unscored.

The boundary is a conjunction of four conditions, **all** of which must hold for a
failure to count as part of the fault: the entry names a fault; the durable marker
records it as fired; the window has not closed; the recovery budget is not exhausted.

Condition two does the most work. "The entry names a fault" is never sufficient, so a
failure before the injector fires is still an apparatus failure. Condition four stops an
exhausted budget absorbing an unbounded number of real failures.

Getting this wrong is fatal in both directions and they fail differently. Too strict
loses the broker runs, visibly, as a hole in the matrix. Too loose scores a genuine
apparatus break as a finding, which is worse because the number looks like evidence.

**Found by running it, not by reading it:** the boundary was defined, tested from both
directions, and called from nowhere. A `DeliveryError` propagated straight out of the
phase. Separately, `send_offsets_to_transaction` and `commit_transaction` raise
`KafkaException` rather than reporting through the delivery callback, so the case
ADR-0003 section 6 is mostly about never reached the contract either. Both are wired
now, through the same boundary, because answering the question twice in two places is
how two answers start to differ.

## 3. The arm-once rule

The seeded fault fires exactly one time. The marker is written and fsynced **before**
`os.kill`, because SIGKILL cannot be caught and a marker written afterwards is never
written; a restarted phase reading "not yet fired" fires again and the run never ends.

A flag cannot detect its own loss, so the guard cross-checks two independent durable
facts: the marker's `fault_fired`, and whether the attempt history records this phase as
already killed. Both cannot be true. A phase killed while the marker says the fault
never fired means the marker was lost, and the run is stopped rather than injecting a
second time.

A SIGKILLed phase cannot write its own epitaph, so it writes it in advance: the attempt
is recorded as `started` before any work and replaced on exit. A record still saying
`started` after the process is gone is the record of a kill. Without this nothing marked
a killed attempt at all and the guard, though correct, was unreachable.

`os.kill` appears in one module, with both arguments pinned by an AST gate:
`os.getpid()` because a harness able to signal another process could take down the
broker container by a typo, and `SIGKILL` because a catchable signal would let a
`finally` run and make the kill a shutdown rather than a crash.

## 4. The matrix-validity rule

Apparatus failures are not scored, so letting an inconvenient run break removes it from
a denominator without anyone editing a number. Six rules, pre-registered here:

1. **A partial matrix never ships.** All 42 executions are attempted or there is no result.
2. **Every apparatus failure is diagnosed in writing.** None is absorbed.
3. **More than 2 apparatus failures out of 42 voids the entire matrix.**
4. **Independently of the count**, C1 and C2 are evaluated over exactly 20 kill runs per
   configuration. Any apparatus failure among those 20 makes that claim not-evaluable
   and voids the matrix. Rule 3's allowance therefore covers only the two control
   executions. Scoring 19 of 20 against a floor written for 20 is the same
   cherry-picking by another route.
5. **No run is re-executed to see whether it was a flake.**
6. **At most 3 void-and-rerun cycles.** Each cycle is individually legitimate; the
   aggregate is a slow route to overfitting the apparatus until it yields a clean
   matrix, which is the failure the floors exist to prevent one level up. After the
   third, the project ships that outcome, naming every void and its diagnosis. "The
   matrix could not be completed and here is the full record of why" is publishable
   under the same logic that makes a FAILED C1 publishable.

Every rule is a refusal, and a refusal that cannot fire is indistinguishable from a
matrix with nothing wrong, so a shape check runs before any of them: non-empty, every
status recognised, both configurations present. An unrecognised status is the quietest
way to drop a run, because it counts as neither scoreable nor as an apparatus failure
and vanishes from rule 3 and rule 4 simultaneously.

## 5. C3: what is compared, and over what

CLAIMS.md says a replay "rebuilds the sink byte-identical to the original run, verified
by checksum". Raw topic bytes cannot match: offsets, timestamps and producer epochs all
differ on a replay and are broker-assigned rather than produced by the consumer under
test, so a checksum over the partition would be a checksum over the broker's
bookkeeping.

**The checksum is over the canonical serialization of the rebuilt `SideEffectRecord`
ledger**, which is the faithful reading of "the sink" for a harness whose subject is
side effects. Ordered by `(sequence, idempotency_key)`, so arrival order cannot make two
identical logs disagree. Every occurrence kept, so a duplicated side effect stays
duplicated: collapsing duplicates would make C3 blind to precisely what C1 counts.

**Every good-configuration run that reached a scoreable status is replayed**, up to all
21. The denominator is named and every excluded run listed. An empty original ledger is
refused rather than compared, because an empty ledger checksums identically to an empty
ledger and C3 would pass on nothing.

## 6. Sequential execution

The 42 executions run one at a time, and this is not a performance decision.
`broker_stop_start` takes down the shared broker. Two concurrent executions would mean
one run's injected outage landing in another run's ordinary work, recording a fault
nobody scheduled against a run the matrix reports as something else. The evidence would
not know it had happened.

---

## 7. The five apparatus repairs, each measured against the principle

### D1 and D2 are sections 1 and 2 above.

### D3: `consumer_max_batch_records` reached no client

Frozen at 100, emitted into `docs/run_schedule.json`, mapped by ADR-0002 to "the
`num_messages` argument to `Consumer.consume()`", and described in `config.py` as "the
direct determinant of C2 loss". `process()` used `poll()`, which hands over one record
at a time, so the committed-but-not-applied window was one record rather than a hundred.
**The frozen artifact described a window the code did not have.** Moving the code to
`consume()` moves it toward what was already frozen. Validity repair. Measured
afterwards: the client really does deliver batches of 100, recorded per run.

### D4: the baseline's auto-commit never fired

`baseline_auto_commit_interval_ms` is frozen at 5000; the whole baseline execution
measures about 2 seconds. The first tick lands after the run ends and librdkafka's commit
on `close` is skipped by SIGKILL, so no offset was ever committed during the baseline
process phase and commit-before-processing, the defect C2 exists to measure, never fired.
The restarted consumer then found no committed offset and replayed from 0, contradicting
ADR-0003 section 7's "it never replays from the start of the run". Two frozen constants
that are mutually inoperative, and neither may move.

**The repair is a hold of two commit intervals at the seeded saga boundary, in the
process phase only, identical and unconditional in both configurations.**

**The argument against it, stated first.** The hold does not restore a natural race.
Without it the kill always beats the commit; with it the commit always beats the kill.
Both outcomes are determined by the harness, so the hold flips a determined result rather
than removing a bias.

**The argument for it, and why it is decisive.** The hold decides only *whether* the
mechanism acts, never *how much* is lost. Once the commit has fired the quantity is fixed
entirely by `consumer_max_batch_records`, which sets how far the offset runs ahead, and
by the frozen fault point, which sets where in that batch the kill lands. The harness
contributes no number to the measurement.

**The limitation, stated beside the ISR limit and the within-Kafka limit.** The hold
changes the workload's timing profile at one point, so the measured claim is
"commit-before-processing loses side effects when the commit has had one opportunity to
fire", not "under a realistic production cadence".

Two intervals rather than one because the timer is periodic from consumer construction,
so a tick can land microseconds before the offset store. In the process phase only
because there is no consumer and no commit timer in ingest; that is a phase property, not
a configuration property. At the saga boundary because it keeps the wait outside the
transaction-timeout budget, which is enforced rather than assumed.

Measured: the baseline committed offset 400 with applied work at 324, a 76-record window,
at the seeded fault point.

### D5: an undeclared delivery-deadline asymmetry

Verified against the pin: librdkafka caps `message.timeout.ms` at
`transaction.timeout.ms`. With the property unset the good producers ran a 60s deadline
and the baseline's ran 300s: a fivefold difference on a property that is **not**
allow-listed, produced indirectly by one that is, and invisible in `resolved_config.json`
because only explicitly-set values are recorded there. A straight INV-P3 violation. Set
explicitly and identically in both. Validity repair, and the clearest of the five.

**On the gate, honestly.** An assertion over each client's *effective* configuration
after librdkafka applies its defaults would be better, and the pinned client cannot
support it: `debug=conf` emits only explicitly-set properties, and `message.timeout.ms`
never appears even when capped. There is no effective-config dump in the Python binding.
The derived-default gate enumerates each known derivation, asserts the dependent property
is present and equal in both, and proves the derivation is real by probing the client's
own validation. **It cannot be exhaustive while the client exposes no effective
configuration, and that bound is recorded rather than implied.**

### The combined open-transaction bound

On a broker run under `good` the 25s outage sits inside an open per-saga transaction,
followed by coordinator reload, abort and replay, against a 60s timeout this repository
does not own. A transaction that timed out would present as fatal, consume recovery
budget, and land as `apparatus_failure`, which under rule 4 voids the matrix from a
single run.

`broker_outage_ms + txn_headroom_ms <= 60000`, i.e. 25000 + 20000, leaving 15s of slack.
`PINNED_TRANSACTION_TIMEOUT_MS` is **measured** by an offline probe rather than declared,
because ADR-0003 section 8 leaves the value to the pin. The hold is excluded from the
bound, and that exclusion is sound only because no hold sits inside a transaction, which
is enforced by an invariant of the process loop.

**A correction to the reasoning, recorded because it was wrong in the plan.** Including
the hold would *not* breach the bound: 25 + 20 + 10 is 55 of 60, which fits. It would cut
the slack from 15s to 5s, below the comfort floor. So moving the hold outside buys
margin, not legality.

### `session.timeout.ms`, and a repair that was not neutral

First set to 6000, the broker's floor, purely for wall clock. A broker-fault run showed
the choice was not neutral: with a 6s session and a 25s outage the consumer is evicted
from its group **while the broker is down**, every time, and `send_offsets_to_transaction`
then fails with `UNKNOWN_MEMBER_ID`. That turns the fault the schedule names into an
outage plus a group eviction, caused by an apparatus setting chosen for speed, landing on
all twelve broker executions. Raised to 45000 so the session outlasts the outage, and
gated. The ten minutes it costs is the right price for not measuring a fault nobody
scheduled.

### The `sagas_done` assertion, and why removing it bare would not have been safe

PB-T2 asserted `sagas_done == expected_sagas`. It could not survive a restart under
**either** configuration, since a resumed phase processes only the remainder, so it would
have apparatus-failed every process-phase kill run and gutted C1's coverage as well as
C2's.

**It is not simply deleted, and the reason is worth stating plainly: it was the only
guard against an apparatus bug that silently dropped sagas.** Removing it bare would let
a harness defect surface as loss and ship as a FAILED C1. The attributability invariant
is safe *only* because it covers that direction: every lost side effect must fall inside
a recorded offset gap, and under `good` the gap list is empty by construction, so any
loss at all is unattributable and the run ends as `apparatus_failure` rather than as a
C1 failure. The replacement is not safe because the original was redundant.

### The third attribution route, added before the matrix ran

The invariant as first written accepted a loss only if the record's offset fell inside a
recorded gap between a killed attempt and its restart. **Verified concretely rather than
reasoned about**, and it leaves one case with no route at all.

A baseline broker run has exactly one process-phase attempt, because nothing SIGKILLs
it, so ``offset_gaps()`` is empty by construction. It is also non-transactional, so there
is no aborted-transaction route either. A genuine loss would therefore be unattributable,
become ``apparatus_failure``, and under validity rule 4 **void the whole matrix** rather
than record the loss. That destroys the exact signal C2 measures, in the only six runs
where it can still appear, and costs a cycle each time.

So a third route is added, **before the matrix runs and while the result is unknown**:

> A lost record is attributable if its own send was recorded as permanently failed
> inside a recorded fault window.

**Record-level, not window-level, and that distinction is the whole safeguard.** "Any
loss during a fault window is attributable" would absorb a genuine apparatus break that
merely coincided with the outage, and the invariant would lose its teeth. The key must
appear in the set of records whose delivery was recorded as failed, and that set is only
written after the fault-window boundary has already accepted the failure as in-window, so
both conditions are carried by construction rather than checked twice.

The missing piece was that the failing record's identity was not recorded anywhere:
``_Sender`` kept the errors and their topics and discarded the message key, which is the
idempotency key. It is captured now and carried durably in the run state, because the
phase that records it is a different process from the one that verifies the sinks.

**This is a validity repair by the principle above.** The invariant's stated purpose is
that an *unexplained* loss is an apparatus defect, and this loss is explained. It cannot
wait until after a void, because then it would be an adjustment made after seeing a
result.

Proven red both ways: a loss with a recorded per-record failure must be attributable and
scored, and a loss without one, inside the same window and the same run, must still be
``apparatus_failure``.

---

## 8. C2 cannot reach its floor, and this is written before the matrix runs

**The ingest phase cannot lose, in either configuration, by construction.**

ADR-0003 section 7 freezes the ingest resume rule: the durable state is the input topic
read back at startup, a saga counts as complete only when all M steps are visible, and
the resume point is the first index not known to be complete with gaps never skipped.
After any number of ingest kills the visible input topic therefore contains every saga at
least once, the process phase writes all of it, and nothing can be missing. Under `good`
the aborted transaction is invisible so nothing duplicates either; under `baseline` the
partial send is durable and the re-send duplicates at most `steps_per_saga - 1` = 2
records per sink.

Confirmed empirically before this document was written, in
`tests/integration/test_resume.py` and `tests/integration/test_fault_injection.py`: after
a real SIGKILL and a real restart every saga is present, the baseline duplicates exactly
2, and the good configuration duplicates none.

`producer_sigkill_mid_send` targets the **ingest** producer. CLAIMS.md says the harness
kills "producers, consumers, and the broker", three components for three faults;
`consumer_sigkill_between_sinks` already kills the process that owns the sink producer;
and ADR-0003 section 7's ingest bullet would otherwise be dead contract.

So, as arithmetic:

| fault type | runs | can loss occur? |
| --- | --- | --- |
| `producer_sigkill_mid_send` | 7 | **no**, by the frozen resume rule |
| `consumer_sigkill_between_sinks` | 7 | yes |
| `broker_stop_start` | 6 | yes, in principle. See below |

**Maximum attainable C2 numerator: 13. Floor: 16 of 20. C2 fails as pre-registered.**

The 13 is computed by committed code from the committed schedule, not typed, and pinned
by a test. The matrix carries a `loss_structurally_possible` column so a reader can check
the arithmetic without trusting this document.

**The finding, stated precisely, because the precision is the value.** C2 did not fail
because the harness could not distinguish the configurations. It failed because the
pre-registered denominator included 7 runs in which loss is structurally impossible under
a resume contract chosen later but still blind. CLAIMS.md is not reinterpreted to rescue
it. The floor binds, C2 ships FAILED, and every result ships report-only.

### The open question about the broker runs

One broker run under the baseline has been observed to **duplicate rather than lose**.
One observation settles nothing: whether a broker outage loses depends on where the
seeded fault point falls relative to what the commit timer had already committed, and
that varies across the six runs. **`loss_structurally_possible` is therefore left
unchanged at 13, on the explicit principle that only a proof justifies moving a
pre-registered predicate, never an observation.**

What can be shown is narrower than the ingest case and of a different kind. A broker run
has exactly one process-phase attempt, so it produces no offset gaps, so under the
attributability invariant any loss it did produce would be unattributable and become
`apparatus_failure`. **That is a proof about the harness's own reporting rule, not about
the delivery configuration.** The ingest proof comes from the frozen resume contract; this
one comes from an invariant this run wrote. It does not meet the standard, so it is
recorded here as a candidate explanation the matrix will test rather than as grounds for
a change.

C2 fails either way, at 13 of 20 or 7 of 20 against a floor of 16, so nothing is gained by
moving the number and the only question is which explanation is true. The matrix reports
the **attained** figure beside the ceiling, decomposed by fault type, so the gap and its
cause are visible.

### The loss-capable-subset figure

Also pre-registered here, before the matrix runs: the baseline's loss rate across only the
runs where loss is structurally possible.

**It is not a claim. No floor applies to it. Nothing passes or fails on it.** It is never
presented as C2 passing, nor as C2 passing on a subset. Name the denominator, name the
number, state that no threshold applies, and stop there. Pre-registering it now is what
stops it being a post-hoc subgroup analysis; saying it has no threshold is what stops a
later reader seeing "11 of 13" and inferring it cleared a bar nobody set.

---

## Consequences

C2 is expected to ship FAILED, and under CLAIMS.md's ship rule every result therefore
ships report-only. That is a publishable outcome, and the pre-registration on 2026-07-28
is what makes it one.

The matrix reports, per execution: duplicated, lost, `loss_structurally_possible`,
transactions committed and aborted per phase and role, `max_open_transaction_ms`, the
recovery history, the offset gaps and the records they skip, the attempt count, and
`run_status`. The gap arithmetic is carried so a loss count can be reconciled against the
window that explains it rather than taken on the invariant's word.

---

## 9. Void record: cycle 1 of 3, 2026-07-30

**The matrix ran all 42 executions in 30.9 minutes and VOIDED under validity rule 3:
4 apparatus failures against the pre-registered limit of 2.** Log
`runs/matrix-20260730-181823.log`, matrix `runs/matrix.json`.

Recorded here whether or not a later cycle succeeds. The record of a void is part of the
result, and rule 6 caps the cycles at three precisely so that repairing until the
apparatus yields a clean matrix is bounded and visible.

Outcome distribution: 25 clean, 13 not_clean, 4 apparatus_failure.

### The four diagnoses

All four are the **same failure mode**, and all four fell inside the good
configuration's 20 kill runs. **None is a control**, so rule 4 voided the matrix
independently of the count: even one of these would have done it.

| run | configuration | fault type | phase | attempt | exception |
| --- | --- | --- | --- | --- | --- |
| 03 | good | broker_stop_start | process | 1 | recovery budget exhausted after 4 UNKNOWN_MEMBER_ID |
| 06 | good | broker_stop_start | process | 1 | as above |
| 15 | good | broker_stop_start | process | 1 | as above |
| 18 | good | broker_stop_start | process | 1 | as above |

Each recovery history is identical in shape: one `reinit_producer` for a `_MSG_TIMED_OUT`
delivery failure inside the fault window, then **four** `abort_and_replay` entries, each
carrying `UNKNOWN_MEMBER_ID` from `send_offsets_to_transaction`, after which the loop
exhausted its bound and raised.

`transactions_committed` was 349, 317, 290 and 252, which is 200 from ingest plus exactly
`saga_index` from process (149, 117, 90, 52). Each run therefore died at its own seeded
fault saga, as designed, and the failure is in the recovery rather than in the injection.

**Cause, established mechanically rather than inferred.** The broker restart invalidates
the consumer's group membership. `send_offsets_to_transaction` needs valid membership,
which it reads from `consumer_group_metadata()`. A consumer rejoins its group only when
the client is polled. **The recovery loop replays the sink write without ever polling the
consumer**, so the membership is stale on the first retry and identically stale on every
retry after it. The loop is structurally incapable of recovering from this condition: it
retries a call that cannot succeed until an action the loop never performs has happened.
Confirmed by walking the AST: `write_group_with_recovery` and `write_group` contain no
`consume` or `poll` call at all.

Runs 09 and 12, the other two good broker executions, completed clean with a single
recovery event and no `UNKNOWN_MEMBER_ID`. The difference is whether the coordinator's
reload after restart happened to preserve the membership, which is a race. That two of
six survived is what made this look like an intermittent fault rather than a structural
one until the histories were read side by side.

### The baseline broker runs are not affected

All six completed as `not_clean` with 3 duplicated and 0 lost. The baseline makes no
transactional call, so it never reaches `send_offsets_to_transaction` and never meets
this condition. The asymmetry is a property of the defect, not of the configurations.

### Classification of the repair, before it is proposed

Measured against the principle at the top of this ADR.

**Validity repair.** ADR-0003 section 6 fixes the response to an abortable error as
"abort, then replay that saga". The code classifies correctly and then performs a replay
that cannot succeed, because a saga's offset commit requires group membership and the
loop starves the client of the poll that restores it. **The contract's stated recovery
action is not actually being performed.** Letting the consumer rejoin before the replay
moves the code toward what section 6 already froze.

It changes no classification, does not widen what counts as in-window, does not widen
what counts as attributable, does not raise the recovery budget, and touches no floor and
no denominator. The set of recoverable conditions is defined by `classify`, which is
untouched.

**Rejected as tuning, and not proposed:** raising the recovery budget so four retries
become enough; reclassifying `UNKNOWN_MEMBER_ID`; widening attributability so the
incomplete run scores anyway. Each would move the declaration toward a wanted result and
each would make the failure disappear without the apparatus becoming able to do what the
contract says it does.

**The honest alternative, also not a repair:** change nothing, and report that the good
configuration's broker runs cannot be executed by this apparatus. That ships C1 as
not-evaluable at its pre-registered denominator, and it is the outcome if the repair
above does not hold.

### The ruling, and the repair as built

**Owner's ruling, 2026-07-30:** the classification is accepted. Polling is not an
addition to section 6's replay but a precondition of it: a saga replay under the good
configuration necessarily commits offsets inside the transaction, that needs group
membership, and membership is restored only by a poll. The code was attempting the
frozen action and failing on a missing mechanical prerequisite.

The honest non-repair alternative was rejected as **the wrong honesty**: reporting C1 as
not-evaluable when the cause is a recovery loop that never polls would be publishing an
apparatus bug as a limit of knowledge.

Three conditions were set and all three are met.

**The poll is unconditional in the shared recovery path.** No branch on the
configuration. The baseline never reaches the abortable-error branch at all, because it
makes no transactional call, so an unconditional poll costs it nothing while keeping
INV-P3's control-flow gate clean. That gate still passes, and a seeded `if
transactional` guard on the rejoin reds the new gate.

**What happens to the records the rejoin poll returns**, which is the part that could
have invented a number. Serving a queue returns whatever is on it. Dropping those records
would manufacture loss; handing them to the processing stream a second time would
manufacture duplication. Either would be the apparatus producing a figure out of its own
recovery rather than out of the configuration under test. They go to a pending list, and
the main loop drains that before calling `consume` again, so nothing is dropped and
nothing is seen twice. Offset order is preserved because anything the poll returned is
strictly earlier than anything a later `consume` would return, and pending records go
through the same loop body, so a partition-EOF event pulled off by the rejoin still ends
the drain.

**The diagnosis is now a gate rather than a story, and it does not rely on a broker
restart.** Runs 09 and 12 survived the same fault on a race, so a single passing run
proves nothing and a test whose setup is itself a race would inherit that. Membership is
invalidated deterministically instead: a member joins, leaves, and is replaced, and the
metadata captured before the replacement is exactly the stale membership a restart
produces. Measured: `send_offsets_to_transaction` with that metadata fails with
`UNKNOWN_MEMBER_ID`, the void's exact error, and succeeds with membership obtained after
a poll.

Proven red four ways: removing the rejoin, guarding it with `if transactional`, dropping
the pending records, and putting a `poll` back on the batch path. The last of these
tightened an existing gate rather than relaxing it: the frozen-batch-size rule now
asserts that any `poll` in the process phase lies inside `rejoin_consumer`, so the batch
still comes only from `consume`.

## Reopen trigger

Section 8's ceiling reopens only on a **proof** that the broker runs cannot lose
regardless of fault point, of the same kind as the ingest proof: derived from a frozen
contract rather than from an apparatus invariant this run introduced, and recorded before
the matrix that uses it. An observation, however many runs it covers, is not such a proof.

Section 7's D4 hold reopens if a later run shows the one-opportunity framing makes a
fault unobservable in principle rather than merely inconvenient, recorded with its date
and reason before the run that uses it, and never after seeing a result it would improve.

The derived-default gate reopens if a client version exposes its effective configuration,
at which point the honest gate replaces the reachable one.
