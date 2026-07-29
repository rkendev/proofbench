# ADR-0003: The workload, the sinks, and the two configurations

Status: accepted
Date: 2026-07-29
Supersedes: the section "Where the sink A / sink B boundary sits" in
[ADR-0002](0002-measurement-invariants.md), and nothing else in it

## Context

PB-T2 builds the apparatus and proves it reads zero. It injects no fault and
measures no claim. Everything recorded here was decided before the first control
run produced a number, and before any fault injector existed.

That ordering is the point of the whole prompt. The control exists to show the
harness does not report duplications or losses when nothing was killed. Running it
before a fault injector exists means the apparatus was proven honest before anyone
could see a kill result. Doing it the other way round would leave every later
number open to the objection that the apparatus was adjusted once its answers were
visible. The decisions below are exposed to the same objection, so they carry the
same date and the same ordering.

## Decision

### 1. Sink A and sink B are Kafka topics, not local stores

Both earlier formulations were wrong, and for the same reason: they put the
measured effect somewhere a Kafka transaction cannot reach.

**ADR-0002 as written** made sink A a non-idempotent effect store and sink B an
idempotency ledger, and predicted that a kill between them would apply the effect
a second time on redelivery. That is correct as a description of what happens, and
it makes duplication structurally **guaranteed**: a Kafka transaction cannot cover
a write to a store outside Kafka, so the same trace duplicates under the good
configuration too. C1 would ship FAILED for a reason that has nothing to do with
delivery semantics, and C2 could distinguish nothing.

**The obvious repair**, making both stores idempotent upserts on
`idempotency_key`, fails in the opposite direction. It makes duplication
structurally **impossible**: an upsert-keyed store cannot hold two records under
one key, so `LedgerDiff.duplicated` would be empty by construction. C1's first half
would then be guaranteed by the sink design rather than by the Kafka
configuration, and half the claim would be vacuous.

**The decision.** Sink A and sink B are two Kafka topics. The consumer reads the
input topic, writes sink topic A and sink topic B, and commits offsets, and under
the good configuration all of that happens inside one transaction scoped per saga,
per the frozen `transaction_boundary`. Verification reads the sink topics with
`isolation.level=read_committed`.

Both failure modes then become real and attributable to the thing under test:

| | good | baseline |
| --- | --- | --- |
| loss | the transaction covers the sink writes and the offsets together | offsets are stored before processing, so a kill in that window means no redelivery and the effect never lands |
| duplication | aborted writes never surface under read_committed | the sink is written and the kill lands before the offset commit, so redelivery writes it again, and with no idempotence a producer retry can duplicate on its own |

This also makes `consumer_sigkill_between_sinks` do what CLAIMS.md says it does.
CLAIMS.md calls it "the partial-write duplication case". Under the baseline a kill
between the two sink writes genuinely leaves A present and B absent, which is the
partial write. Under the good configuration the abort removes both, so the partial
write cannot be observed, and preventing that is the property being demonstrated.

The reasoning behind the reasoning, stated because it is the general rule: **an
apparatus in which the claim is not testable is not a neutral choice.** A design
that forces C1 to fail and a design that forces C1 to pass are equally
uninformative, and choosing either one without noticing would have produced twenty
runs of numbers that meant nothing. The question to ask of an apparatus is not
whether it is careful but whether the claim could come out either way.

"Both sinks independently durable" is therefore a consequence of this design under
the baseline, not a requirement on it, and it is not stated as one anywhere.

Nothing here touches the frozen contract. CLAIMS.md never says where the sinks
live. `docs/run_schedule.json` is unchanged and its byte-equality gate is green.

### 2. The honest limit: this measures exactly-once WITHIN Kafka

Stated as plainly as the single-node ISR limit already is, and for the same
reason: it bounds what any result can mean.

The input topic, the two sink topics, and the consumer offsets are all Kafka
resources, so one transaction can cover all of them. That is precisely the
configuration C1 names, and it is what this harness measures.

It does **not** measure exactly-once delivery to a non-transactional external
system. That is a different problem. A payment API, an email send, or a row in
someone else's database cannot join a Kafka transaction, and the answer there is
an idempotent write keyed on the effect, not a broker configuration. It is out of
scope for v1.

**A C1 pass here is not evidence that an agent's real side effects are safe under
the same configuration.** The README carries this limit beside the ISR limit, and
neither the README nor any result may imply otherwise.

### 3. Aborted-transaction counts are recorded per run

Every run's `run_summary.json` carries `transactions_committed`,
`transactions_aborted`, and the full recovery history. Under the good
configuration an abort is the mechanism that keeps a partial write unobservable,
so how often it fired is part of what a run shows rather than an implementation
detail. A clean result that had needed three producer re-initialisations to get
there says something quite different about the apparatus than one that needed
none, and the evidence has to be able to tell them apart.

### 4. Sink A is durable before sink B is attempted

Retained from ADR-0002 unchanged. What it now means is that the saga's records are
produced to topic A and flushed, then produced to topic B and flushed.

The flush between them is part of the decision, not an implementation detail.
Without it both sets of records would sit in one producer queue and reach the
broker together, so "A before B" would describe the order of two function calls
rather than the order of two writes, and a kill between them could not leave A
present and B absent at all.

The ordering is pinned by a unit test with a recording writer rather than by the
control run, because **the control run cannot see it**: with no fault injected
both sinks end up holding everything under either order, and the no-fault result
is identical. Reversing the order and re-running the control still reports clean.
A frozen decision that no test can see is a comment.

### 5. The two configurations, and INV-P3

Held in `src/proofbench/core/configs.py`, built entirely from `Settings`. No
literal restates a frozen number, and a test asserts that every integer either
configuration carries is one of the values `Settings` holds.

Shared by both, byte for byte:

| Setting | Source |
| --- | --- |
| `bootstrap.servers` | environment, through Settings (INV-1) |
| `linger.ms` | `producer_linger_ms` |
| `batch.size` | `producer_batch_size_bytes` |
| `queued.min.messages` | `consumer_queued_min_messages` |
| `auto.offset.reset` | `earliest` |
| `enable.auto.offset.store` | `True` |
| `enable.partition.eof` | `True` |

Good configuration:

| Role | Settings |
| --- | --- |
| ingest producer | shared, plus `enable.idempotence=true`, `transactional.id=proofbench.rNN.good.ingest` |
| sink producer | shared, plus `enable.idempotence=true`, `transactional.id=proofbench.rNN.good.sink` |
| consumer | shared, plus `group.id=proofbench.rNN.good`, `enable.auto.commit=false`, `isolation.level=read_committed` |
| offset commit | inside the transaction, via `send_offsets_to_transaction` |
| transaction boundary | `transaction_boundary`, frozen at `per_saga` |

Known-bad baseline:

| Role | Settings |
| --- | --- |
| ingest producer | shared, plus `enable.idempotence=false`, no `transactional.id` |
| sink producer | shared, plus `enable.idempotence=false`, no `transactional.id` |
| consumer | shared, plus `group.id=proofbench.rNN.baseline`, `enable.auto.commit=true`, `auto.commit.interval.ms` from `baseline_auto_commit_interval_ms`, `isolation.level=read_uncommitted` |
| offset commit | auto, before processing |
| transaction boundary | none |

The verifier is identical in both, differing only in its derived `group.id`:
shared settings plus `enable.auto.commit=false` and
`isolation.level=read_committed`.

**INV-P3, and why C2's validity depends on it.** The two configurations differ
only on an allow-list of Kafka client settings:
`enable.idempotence`, `transactional.id`, `isolation.level`, `enable.auto.commit`,
`auto.commit.interval.ms`. Everything else on the run path is the same code,
including the sink-writing path and the ledger writer. C2 requires the baseline to
lose at least one side effect in at least 80 percent of the 20 kill runs, and that
number is only about delivery configuration if the delivery configuration is the
only thing that differs. Hand the good configuration a larger batch or a different
sink path and the baseline might fail for that instead, at which point the 80
percent measures the rigging.

Enforced by `tests/unit/test_configs_allowlist.py`, which is the machine-checkable
authority. Every allow-listed setting must trace to a token in CLAIMS.md or
ADR-0002, and the allow-list is enumerated independently in the test, so widening
what C2 is permitted to be measuring takes two visible edits and a justification.

Three choices make INV-P3 hold more tightly than it otherwise would:

- `enable.auto.offset.store=true` in **both**. librdkafka stores the offset the
  moment `consume()` hands the message to the application, so under the baseline
  `enable.auto.commit` then commits it on the interval whether or not the
  application reached the sink writes. Commit-before-processing is therefore
  produced by a client setting rather than by a branch, and both configurations
  share one consume loop. In the good configuration it is inert, because
  auto-commit is off and offsets travel through `send_offsets_to_transaction`.
- The verifier is `read_committed` for both. `read_committed` filters only aborted
  transactional messages, so it returns the baseline's non-transactional writes in
  full, and verification stays one path with one set of settings.
- `enable.partition.eof=true` in both, so the drain ends on a deterministic event
  rather than a poll timeout. A timeout that fired early would under-read a sink
  and report loss that never happened, and a measurement harness cannot afford a
  stopping rule that can be wrong.

### 6. The transactional id is stable per run, configuration, and role

`proofbench.rNN.<configuration>.<role>`, role being `ingest` or `sink`. Never
per-instance and never random.

`transactional.id` is the zombie-fencing identity. `init_transactions` bumps the
producer epoch for that id and aborts whatever transaction the previous epoch left
open, and that is the only mechanism that cleans up after a producer SIGKILL. With
a per-instance id the killed producer's transaction is never fenced: it stays open
until `transaction.timeout.ms` expires, the Last Stable Offset does not advance
past it, and every `read_committed` consumer blocks on that partition. PB-T3's
producer-kill runs would stall, or be scored as loss the apparatus caused.

Cost accepted: one live producer per run, configuration, and role at a time, which
is true in a single-process harness. A future need for concurrency adds a role, not
a random suffix.

**A fenced producer after `broker_stop_start`.** Decided now, because PB-T3
injects that fault and discovering the behaviour then would mean choosing it with
results already visible. Three error classes, three responses:

1. `retriable()`: retry the same call. A broker restart makes `commit_transaction`
   retriable while the coordinator re-elects. Bounded by the client's own timeout,
   no custom sleep loop.
2. `txn_requires_abort()`: `abort_transaction`, then replay that saga from its
   start. The saga is the transaction boundary, so a replay is well defined, and
   its idempotency keys are unchanged, so a successful replay produces exactly the
   expected records.
3. `fatal()`, which is where fencing lands: the producer object is dead. Discard
   it, construct a new one with the **same** `transactional.id`, and call
   `init_transactions`, which bumps the epoch and fences the dead one. Never mint a
   new id, for the reason above.

An error matching none of the three is treated as fatal rather than retried. In a
measurement harness an unclassified condition must not be folded into a number.

Recovery is bounded at three producer re-initialisations per run. Exceeding it
ends the run with `run_status: apparatus_failure`.

### 7. The restart and resume contract

Frozen here, dated, before any kill result exists, because it decides the size of
the baseline's duplication and that is the quantity C2's 80 percent floor is
measured against. Left unstated until PB-T3 it would be a free knob available once
results were visible. Implementation is PB-T3; the choice is made blind.

**On restart, a phase resumes at the first saga index not known to be durably
complete, and re-processes from there. It never replays from the start of the run,
and it never skips forward past unrecorded work.** Identical code in both
configurations, per INV-P3.

- **Process phase.** The durable state is the committed consumer offset, so
  Kafka's own mechanism decides the resume point and the harness never calls
  `seek`. Good: the aborted transaction discarded the partial work and never
  committed the offsets, so the re-processed saga lands exactly once. Baseline: the
  offsets were stored before processing, so the re-processed saga duplicates
  whatever already landed.
- **Ingest phase.** The durable state is the input topic, read back at startup.

The bound this buys is the reason it is worth freezing: **at most one saga's worth
of side effects per sink per kill**, which is `steps_per_saga` records. That is the
scale CLAIMS.md's "partial-write duplication case" implies. A whole-run replay
would make the baseline duplicate hundreds of side effects and C2 would pass for a
crude reason that has nothing to do with commit placement.

**A refinement of the owner's wording, recorded rather than applied silently.** The
ruling said "resumes from the last saga index it durably recorded, and
re-processes that saga". For the process phase the two formulations coincide,
because the committed offset already points at the first unprocessed record. For
the ingest phase under the good configuration they diverge, and the literal
version is unsafe: if saga L committed and saga L+1 aborted, re-processing L
re-sends a saga that is already durable. Producer idempotence does not suppress
that, because it deduplicates retries within a producer epoch and a restart bumps
the epoch. The result would be a genuine duplicate under the good configuration,
and C1 would fail for an apparatus reason. Resuming at the first saga not known to
be complete preserves the intent, including the one-saga bound, without the
hazard. Gaps therefore are not skipped: with sagas 0, 1, and 3 durable the resume
point is 2, not 4.

**When a phase cannot resume at all**, because the resume point cannot be
determined or the re-init budget is exhausted, the run records
`run_status: apparatus_failure`, writes whatever evidence it holds, and is never
scored as a claim result. One failure mode, not two. Scoring an incomplete run
would let an apparatus problem masquerade as loss and inflate C2 for free.

### 8. `transaction.timeout.ms` is locked by the client pin

It is not restated as a frozen constant, because doing so would change
`docs/run_schedule.json`, which this run may not touch. It is **not unowned**:
`confluent-kafka==2.11.1` in `requirements.txt` fixes librdkafka's default, and
every run writes the client and librdkafka versions into `resolved_config.json`
alongside the settings it set explicitly.

**Any later change to `transaction.timeout.ms` requires its own ADR.** The same
applies to changing the client pin in a way that moves it.

### 9. The workload trace is authored from the master seed

CLAIMS.md calls the workload a "recorded LLM-agent tool-call saga" and elsewhere
says it is "modeled as a saga". `docs/agent_trace.json` is expanded from the master
seed by `scripts/write_agent_trace.py` and pinned by a byte-equality gate, exactly
like `docs/run_schedule.json`.

The wording that ships, in the README and inside the artifact itself:

> The saga shape models an LLM agent's tool-call sequence. It was authored
> deterministically from the master seed, not sampled from a live model.
> Regenerating it from that seed reproduces the committed file byte for byte.

Reasoning. The frozen schedule already fixes `steps_per_saga` at 3 and
`sagas_per_run` at 200, so a live recording could only vary payload text, which
feeds nothing but `payload_checksum`. A trace expanded from a seed is regenerable
and therefore gate-checkable, which is the proof technique this repository already
uses; a recorded one could carry only a checksum. And a provenance note naming a
model would force an exception into the house-style brand-token gate, whose stated
strength is that it carries none.

CLAIMS.md's budget line allows "pennies of model tokens at most" for a recording.
Spending zero is the low end of that range, not a breach of it. **The README must
not imply a live model was involved, because none was.**

## What this ADR does NOT supersede

ADR-0002 is superseded in its sink section only, and remains authoritative for
everything else. Stated explicitly so a reader of ADR-0003 alone knows the scope.

- **INV-P1, no live model in a harness run: unchanged.** Sinks as Kafka topics add
  no import. `confluent-kafka` is on neither denylist, because it carries its own
  transport in C, and the AST walk over `src/proofbench/` still passes.
- **INV-P2, no count taken by eye: unchanged.** `interfaces/ledger.py` is untouched.
  Every count still comes out of a `LedgerDiffer` returning a `LedgerDiff`, now
  implemented by `KeyedLedgerDiffer`. Conditions the frozen shape cannot express
  raise `LedgerIntegrityError` rather than being folded into a bucket, which is the
  invariant applied rather than relaxed.
- **The frozen experiment constants: unchanged.** `docs/run_schedule.json` is not
  modified and its byte-equality gate is green.

## Consequences

The sink topics are provisioned per run and per configuration, delete then create,
so a rerun starts from an empty topic and the two configurations can never read
each other's output. One partition per topic, giving total ordering over the saga
stream so PB-T3's seeded fault point is a single well-defined position; replication
factor 1, which is all single-node KRaft can hold. Neither number enters
`docs/run_schedule.json`: they are apparatus shape, not frozen experiment
constants.

The control run at run_id 0 is an apparatus check and never a claim result. If it
ever reports a non-zero count, that is a harness defect and it blocks the matrix.
It is not retried to see whether it was a flake.

## Reopen trigger

Section 1 reopens if a claim is ever added that requires a side effect outside
Kafka, which would mean the project had taken on the external-system problem
section 2 puts out of scope, and would need a new contract rather than a wider ADR.

Section 6's stable `transactional.id` reopens if a run ever needs two concurrent
producers in the same role, which would require a fourth component in the id
rather than a random suffix.

Section 7 reopens if PB-T3 demonstrates that the one-saga bound makes a fault
unobservable in principle rather than merely inconvenient. Such a change is
recorded here with its date and reason, before the run that uses it, and never
after seeing a result it would improve.
