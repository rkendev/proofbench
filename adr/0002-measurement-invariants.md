# ADR-0002: Measurement invariants and the frozen experiment constants

Status: accepted
Date: 2026-07-29

## Context

CLAIMS.md pre-registers three claims with a floor each, and it fixes the semantics of
both configurations under test. It does not fix everything a measurement needs. Two
gaps matter enough to close in committed code rather than in habit:

- A harness that called a live model would neither be cheap nor deterministic, and
  CLAIMS.md's near-zero spend estimate and claim C3 both rest on it not doing so.
- A count read off a screen is not evidence. CLAIMS.md says no count is ever taken by
  eye, which only means something if the code makes eye-counting structurally absent.

Beyond those, CLAIMS.md fixes the baseline's semantics but not its numeric tuning,
and it fixes neither the length of a saga stream nor where a fault lands. Those
numbers determine whether a claim can pass or fail for real reasons, so they are
recorded here with the date they were fixed.

Everything in this ADR was decided at PB-T1, before the repository booted a broker,
before the harness existed, and before any measurement of any kind.

## Decision

### INV-P1, no live model in a harness run

The agent tool-call saga is a recorded trace. No model client, and no
general-purpose HTTP client that could stand in for one, may be imported anywhere
under `src/proofbench/`. Enforced by an AST walk over every module in the package in
`tests/portability/test_no_model_client_import.py`, which is the machine-checkable
authority for the denylist; this document does not restate the package names, because
the house-style gate bans them as brand tokens in tracked files and the test
assembles them from fragments so that gate keeps zero exceptions.

Scope is the whole package rather than a designated subtree: every module under
`src/proofbench/` is on the harness run path, so scanning the package is simpler and
strictly stronger than picking a boundary and trusting it.

The network denylist is not incidental. INV-P1 has to cover "a raw HTTP client used
for a model call", and the only way to enforce that without reading intent is to ban
the general-purpose clients outright. Nothing ProofBench needs is affected: the Kafka
client is `confluent-kafka`, which carries its own transport in C.

### INV-P2, no count taken by eye

Every reported duplication or loss count comes from committed code diffing the sink
ledger against the expected saga ledger. `src/proofbench/interfaces/ledger.py` fixes
the shape: an idempotency-keyed `SideEffectRecord`, a `LedgerDiff` carrying the
duplicated and lost records themselves rather than integers, and a `LedgerDiffer`
protocol that consumes two ledgers and returns a diff. A reported number has to come
out of that shape, and there is nowhere in it for a hand-counted figure to enter.

Carrying records rather than counts is deliberate: a bare integer is a number with no
way to check it, while a record set lets any reported figure be traced to the specific
side effects behind it.

PB-T1 lands the interface only. There is no implementation and no measurement in this
run.

### Where the sink A / sink B boundary sits

The fault type `consumer_sigkill_between_sinks` has no meaning without this, so it is
fixed here: each consumed side effect is written to two sinks in a fixed order, sink A
being the effect sink (the simulated external system, where the charge lands) and sink
B being the idempotency ledger (the record marking that key as applied). The kill
point is after A is durable and before B is written.

That ordering is what makes CLAIMS.md's "partial-write duplication case" possible. The
effect happened but the record that would suppress a retry did not, so on restart the
effect is applied a second time, which is the double charge. Writing B first would
produce loss instead of duplication, so the order is part of the frozen definition
rather than an implementation detail.

### The frozen experiment constants

Held in `src/proofbench/config.py` as their single authority and emitted into
`docs/run_schedule.json`, which `tests/unit/test_schedule_frozen.py` pins byte for
byte. Changing any of them makes that gate go red.

| Constant | Value |
| --- | --- |
| Saga steps (M) | 3: create_ticket, charge_card, send_confirmation |
| Sagas per run (N) | 200, so 600 side effects per run |
| Kill runs | 20 |
| Control runs | 1, at run_id 0 |
| Fault menu | producer_sigkill_mid_send, consumer_sigkill_between_sinks, broker_stop_start |
| Fault distribution | round-robin over the kill-run ordinal: 7 / 7 / 6 |
| Master seed | 20260728 |
| Fault point | saga index in the middle 60 percent of the stream, step index 1 to M-1 |
| Producer linger.ms | 5 |
| Producer batch.size | 16384 bytes |
| Consumer batch records | 100 |
| Consumer queued.min.messages | 1000 |
| Transaction boundary | one transaction per saga |
| Baseline auto.commit.interval.ms | 5000 |

**N was fixed for statistical power before the first boot.** The deciding constraint
is C2's floor, not throughput. C2 requires the known-bad baseline to lose at least one
side effect in at least 80 percent of the 20 kill runs. An underpowered stream in
which the baseline only failed, say, 70 percent of the time would make C2 fail, and by
C2's own floor every result in the project would then ship report-only: a failure
caused by weak experimental design rather than by reality, which is the worst
available way to fail. 100 sagas leaves a thinner margin on that floor; 500 buys
little extra detection for triple the wall clock, and a matrix slow enough to
discourage re-running stops functioning as evidence. 200 keeps the full matrix at
roughly 10 to 20 minutes and gives C3's checksum a ledger of 600 effects.

Choosing N now, to give C2 adequate power, is ordinary experimental design. Changing N
after seeing C2 fail would be gaming. The date on this record is what separates the
two, and it is why the number is frozen inside a gated artifact rather than left in a
runbook.

**The batch and poll parameters are frozen for the same reason and at the same time.**
N is not what determines whether the baseline loses a side effect: what is in flight
at the instant of the kill is, and that is set by the batch and poll parameters.
CLAIMS.md fixes the baseline's semantics but not its numeric tuning, so leaving these
unpinned would leave the knobs that actually drive C2's outcome adjustable after
seeing that outcome. They sit inside the byte-equality gate for exactly that reason.

Each value: `linger.ms` 5 is the client's current default, pinned because a client
upgrade can change a default and an undeclared default is an undeclared input.
`batch.size` 16384 is not the client's default (1000000) but the familiar value from
the Java client, chosen because at roughly 200-byte records it puts about 80 records
in flight per batch, whereas a 1 MB batch would hold an entire 600-effect run and make
the seeded fault point nearly meaningless. Consumer batch records 100 bounds what the
baseline has committed but not yet applied at the kill instant, making it the direct
determinant of C2 loss. `queued.min.messages` 1000, against a default of 100000,
bounds prefetch so kill timing does not depend on how much the client happened to
have buffered. `auto.commit.interval.ms` 5000 applies to the baseline only, where
commit-before-processing placement makes it arguably the dominant loss knob; the good
configuration commits offsets inside the transaction and never auto-commits.

**Cross-client property spelling.** The client is `confluent-kafka`, backed by
librdkafka, which does not share the Java client's property names and rejects
unrecognized properties when a client is constructed. CLAIMS.md already records one
instance of this (idempotence is spelled `enable.idempotence` there). The rest of the
mapping, so a later prompt does not pin a property that silently does not exist:

| Intent | Java name | What ProofBench uses |
| --- | --- | --- |
| Records handed to the app per poll | max.poll.records | the `num_messages` argument to `Consumer.consume()`, frozen as `consumer_max_batch_records` |
| Client-side prefetch depth | no direct equivalent | `queued.min.messages` |
| Producer batching delay | linger.ms | `linger.ms` (alias `queue.buffering.max.ms`) |
| Producer batch bytes | batch.size | `batch.size` |

**The master seed is 20260728, the pre-registration date recorded in CLAIMS.md.** A
nothing-up-my-sleeve number: a reader who does not trust the author can check the seed
against the contract and see it could not have been selected after seeing a result. An
arbitrary 64-bit constant would be indistinguishable from a seed someone tried thirty
of and kept the best one. The seed only has to fix the schedule, not resist an
adversary, so auditable provenance is worth more than entropy.

Per-run seeds are derived independently as `sha256(f"{master_seed}:{run_id}")`
truncated to 64 bits, not by chained draws from one generator, so run 7 is reproducible
without replaying runs 0 through 6 and inserting or reordering a run cannot silently
shift every seed after it. No value in the schedule comes from `random`: everything is
integer arithmetic on the digest, so the artifact depends on the master seed alone and
not on a generator implementation, a Python version, or a platform.

### The no-fault control run

`docs/run_schedule.json` holds 21 entries: the 20 pre-registered kill runs plus one
control at run_id 0, carrying fault type `none` and no fault point. It runs under both
configurations. Added at PB-T1, before any measurement.

Every hygiene gate in this repository is proven by a seeded violation that drives it
red, which shows the gate fires when it should. The control is the missing half: it
shows the harness does not fire when it should not. A C1 pass without it is weaker
than it looks, because zero duplicates could mean the apparatus is correct or could
mean the apparatus cannot see anything. A unit test on the ledger diff does not close
that gap, since it proves the diff code is right rather than that the whole chain of
broker, producer, consumer, sinks, and ledger reports zero on a clean run. Only the
schedule can carry an end-to-end negative.

**If the control ever reports a non-zero count, that is a harness defect and it blocks
the matrix. It is never reported as a finding.**

This alters no claim, no floor, and no ship rule. CLAIMS.md says "at least 20 seeded
kill runs"; the control is not a kill run, so the claim set is still exactly the 20,
and `tests/unit/test_schedule_frozen.py` asserts exactly 20 entries with a fault type
other than `none`.

## Alternatives considered

**A no-fault check as a unit test instead of a schedule entry.** Cheaper, and it would
have kept the artifact at 20 entries matching CLAIMS.md's description word for word.
Rejected because a unit test cannot exercise the broker, the producer, the consumer,
the sinks, and the ledger together, which is the only place the apparatus can be shown
to read zero end to end.

**Splitting the control into its own schedule file.** Rejected: two files drift.

**Leaving the client tuning to the harness prompt.** Rejected, because it would leave
the numbers that determine C2's outcome adjustable after the outcome was known, which
is the specific failure this ADR's dating exists to prevent.

**A larger or smaller N.** Covered above.

## Consequences

The frozen constants cannot be changed without a red gate, which is intended. A change
that is genuinely warranted is a deliberate commit that updates config, regenerates
the artifact, and amends this ADR with the reason and the date, so the change is
visible in history rather than absorbed.

INV-P1 also constrains the harness's design: the recorded trace has to be read from a
committed fixture, since there is no client available to fetch one.

## Reopen trigger

INV-P1 reopens if a claim ever requires a live model in the run path, which would mean
the project had changed shape enough to need a new contract, not a looser invariant.

INV-P2 reopens if a count is ever needed that cannot be expressed as a diff of two
ledgers.

The frozen constants reopen if the harness, once built, demonstrates that a value
makes a fault unobservable in principle rather than merely inconvenient: for example,
if a batch setting turns out to make the seeded fault point unreachable. Such a change
is recorded here with its date and its reason, before the run that uses it, and never
after seeing a result it would improve.
