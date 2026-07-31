# ProofBench

[![ci](https://github.com/rkendev/proofbench/actions/workflows/ci.yml/badge.svg)](https://github.com/rkendev/proofbench/actions/workflows/ci.yml)

A kill-test harness that measures what Kafka delivery configuration actually does
under injected failure.

The workload is an LLM agent's tool-call side effects modeled as a saga:
`create_ticket`, `charge_card`, `send_confirmation`. In that framing a duplicated side
effect is a double charge and a lost one is a silently dropped step, which is what
makes the delivery-guarantee question concrete rather than theoretical. The harness is
designed to kill producers, consumers, and the broker at seeded points mid-saga and to
count, in committed code, how many side effects were duplicated or lost per
configuration.

The harness and its failure-evidence matrix are the product. This is not an ingest
pipeline, not a dashboard, and not a cloud deployment.

## Every result ships REPORT-ONLY

**C2, the pre-registered sensitivity gate, did not meet its floor, so every result in this
repository ships report-only.** `CLAIMS.md` fixed that consequence before the first broker
boot: "the harness cannot distinguish configurations, is declared insensitive, and every
result ships report-only." It applies to the clean C1 below as much as to anything else,
and it is stated here before any number because that is what the floor requires.

### C2: FAILED, and the failure was predicted in writing before the matrix ran

Loss in **7 of 20** kill runs against a floor of 16.

[ADR-0004](adr/0004-fault-injection-and-the-matrix.md) recorded, **before any execution**,
that C2 could not reach its floor. The ingest resume rule frozen in
[ADR-0003](adr/0003-workload-sinks-and-configurations.md) section 7 makes the ingest phase
incapable of losing a side effect, so the 7 `producer_sigkill_mid_send` runs cannot
contribute loss and the maximum attainable numerator is **13 against a floor of 16**. The
ceiling is computed from the committed schedule by `matrix.loss_structurally_possible`, not
typed, and the matrix carries it as a column so a reader can check the arithmetic.

The finding, stated precisely because the precision is the value: **C2 did not fail because
the harness could not distinguish the configurations.** It failed because the
pre-registered denominator included 7 runs in which loss is structurally impossible under a
resume contract chosen later but still blind. `CLAIMS.md` was not reinterpreted to rescue
it.

### C1: 0 duplicated, 0 lost across 20 kill runs, report-only

Measured across 42 executions with zero apparatus failures. **Two honest limits, named
rather than buried:**

- **It measures exactly-once WITHIN Kafka.** The input topic, both sinks and the consumer
  offsets are all Kafka resources, so one transaction covers them. ADR-0003 section 2: a C1
  pass here is **not** evidence that an agent's real side effects are safe under the same
  configuration. A payment API or an email send cannot join a Kafka transaction.
- **In these 42 executions the consumer never experienced a re-delivery.** Every execution
  recorded `redeliveries = 0`. Duplicate-delivery recovery is therefore covered by forced
  deterministic tests, not by the scored matrix.

Also limiting: single-node KRaft, so broker faults are stop and start outages and ISR
failover is out of scope; and the baseline's commit timer is given one full opportunity to
fire at the seeded fault point, so the measured claim is "commit-before-processing loses
side effects when the commit has had one opportunity to fire", not "under a realistic
production cadence".

### C3: 42 of 42 sink replays matched, report-only

Every one of the 21 good-configuration runs replayed, none excluded. Raw topic bytes cannot
match a replay, since offsets, timestamps and producer epochs are broker-assigned, so the
checksum is over the canonical serialization of the rebuilt effect ledger.

### The cleanest contrast in the project

Six `broker_stop_start` runs per configuration. **Identical code, identical recovery path**,
one `reinit_producer` on `_MSG_TIMED_OUT` in each.

| configuration | duplicated | why |
| --- | --- | --- |
| baseline | **3, in all six** | no transaction to abort, so the first attempt is already durable and the replay makes a second copy |
| good | **0, in all six** | the aborted transaction makes the first attempt invisible |

Six of six, deterministic, not a race. That is `enable.idempotence` and `transactional.id`
doing exactly what `CLAIMS.md` says they do, with everything else held identical by INV-P3.

**The full matrix is in [`docs/MATRIX.md`](docs/MATRIX.md)**, with every ledger committed
gzipped and digested so a reader who trusts neither the author nor the files can regenerate
the expected ledger from the seed and check.

### What it cost to get an honest number

Three matrix cycles were permitted and two were voided, both recorded in ADR-0004 rather
than quietly re-run. Cycle 1 voided on four apparatus failures. Cycle 2 ran clean and was
voided anyway, because the harness had manufactured a duplication and reported it as C1
FAILED: the cycle 1 repair had fixed one artifact and created another. The mechanism was
found by instrumentation after two hypothesis-driven repairs failed, and the criterion for
telling a genuine C1 failure from an apparatus artifact was written down **before** the
mechanism was located, so it could not be shaped around it.

## The claims are pre-registered

[`CLAIMS.md`](CLAIMS.md) states three claims (C1 exactly-once under kill, C2 harness
sensitivity, C3 replay determinism), each with a floor fixing what ships when the claim
fails. It is this repository's **first commit**, made before any other file existed, so
its timestamp demonstrably precedes every measurement that will follow. The repository
has been public since that commit, because a pre-registration nobody can date
independently is not a pre-registration.

The floors are not decoration. If C1 fails, a FAILED headline ships as the result. If
C2 fails, the harness is declared insensitive and everything ships report-only. If C3
fails, it ships as a documented negative. No outcome widens scope.

## The run schedule is frozen, and you can check it

[`docs/run_schedule.json`](docs/run_schedule.json) holds 21 entries: the 20
pre-registered kill runs plus one no-fault control. It is a pure function of a master
seed, and `make test` asserts that regenerating it from that seed reproduces the
committed file byte for byte. Editing a run, moving a fault point, changing the seed,
or retuning a batch parameter all make that gate go red.

That is the point. Cherry-picking runs after seeing a result, or retuning the knobs
that determine whether the known-bad baseline fails, would require a commit that visibly
breaks a gate rather than a quiet edit. The master seed is `20260728`, the
pre-registration date recorded in `CLAIMS.md`, chosen so a reader who does not trust the
author can see it was not selected after seeing a result.

Why the client tuning is frozen alongside the stream length: what is in flight at the
instant of the kill is what determines whether the baseline loses a side effect, so the
batch and poll parameters matter as much as the number of sagas.
[ADR-0002](adr/0002-measurement-invariants.md) records every frozen constant, the
reasoning, and the date.

## Two invariants the tests enforce

- **INV-P1, no live model in a harness run.** The agent tool-call saga is a recorded
  trace. No model client, and no general-purpose HTTP client that could stand in for
  one, may be imported anywhere under `src/proofbench/`. An AST walk over the package
  enforces it. This is what keeps expected spend at zero and keeps runs reproducible
  from their seeds.
- **INV-P2, no count taken by eye.** Every reported duplication or loss count has to
  come from committed code diffing the sink ledger against the expected saga ledger.
  `src/proofbench/interfaces/ledger.py` fixes that shape; it carries the records
  themselves rather than integers, so any reported figure can be traced to the specific
  side effects behind it. `src/proofbench/core/ledger_diff.py` is the implementation.
  Anything that shape cannot express, such as an observed key that was never expected
  or a payload whose checksum changed in flight, raises rather than being folded into
  a bucket: inventing a bucket would be the eye-count the invariant exists to prevent.
- **INV-P3, the two configurations differ only where the contract says they do.** They
  differ on an allow-list of five Kafka client settings and nothing else. Same sink
  code, same ledger writer, same verification path. C2 requires the known-bad baseline
  to lose a side effect in 80 percent of the kill runs, and that number is only about
  delivery configuration if the delivery configuration is the only thing that differs.
  Every allow-listed setting has to trace to a token in `CLAIMS.md` or ADR-0002, and
  the allow-list is enumerated a second time inside the test, so widening it takes two
  visible edits and a justification.

Every gate in this repository has been demonstrated failing against a seeded violation.
A gate nobody has seen fail is not a gate.

## The workload

The saga shape models an LLM agent's tool-call sequence. It was authored
deterministically from the master seed, not sampled from a live model. Regenerating it
from that seed reproduces the committed file byte for byte.

[`docs/agent_trace.json`](docs/agent_trace.json) holds the tool-call templates, and
`make trace` regenerates it. It sits inside a byte-equality gate for the same reason
the schedule does: it decides the payload behind every side effect, and therefore every
checksum claim C3 will compare. ADR-0003 records why it is authored rather than
recorded, the short version being that the schedule already fixes 3 steps and 200
sagas, so a live recording could only vary payload text, and an artifact expanded from
a seed can be checked by a reader who does not trust the author.

## Stated scope limits

Two, both of which bound what any result can mean.

**A single node cannot demonstrate ISR leader failover.** The broker runs as
single-node KRaft in Docker, with explicit replication-factor settings for the internal
offsets and transaction-state topics, so broker faults here are stop and start outages
and failover measurement is out of scope for v1. Stated in `CLAIMS.md` and repeated
here.

**This measures exactly-once WITHIN Kafka.** The input topic, both sink topics, and the
consumer offsets are all Kafka resources, so one transaction can cover all of them,
which is precisely the configuration C1 names. It does **not** measure exactly-once
delivery to a non-transactional external system. A payment API, an email send, or a row
in someone else's database cannot join a Kafka transaction, and the answer there is an
idempotent write keyed on the effect rather than a broker configuration. **A C1 pass
here would not be evidence that an agent's real side effects are safe under the same
configuration.** ADR-0003 records the decision and why the alternatives were worse: put
the measured effect outside the transaction's reach and the result measures the sink
design rather than the delivery configuration, in one direction or the other.

The client is `confluent-kafka`, where the idempotence flag is spelled
`enable.idempotence` rather than with the kafka-python spelling. ADR-0002 records the
rest of that cross-client property mapping, including the fact that the Java consumer's
`max.poll.records` has no librdkafka equivalent. Every property the harness sets was
verified against the pinned client rather than taken on trust, by a gate that needs no
broker.

## Getting started

See [`SETUP.md`](SETUP.md) for the clone-to-green checklist and its measured times.

```
make bootstrap    # venv, pinned deps, hygiene gate
make test         # the whole suite; needs no broker
make schedule     # regenerate the frozen schedule; must be a no-op
make trace        # regenerate the frozen agent trace; must be a no-op
```

The suite needs no broker. The integration tests skip with a named reason when none is
reachable, which is what keeps CI offline and booting nothing. To run them:

```
make broker-up                                        # single-node KRaft, prints the address
export PB_BROKER_BOOTSTRAP_SERVERS=127.0.0.1:29092
make broker-status                                    # ready means the broker answers
make control-run                                      # the no-fault control, both configurations
make broker-down                                      # deliberate reset, removes the log
```

See [`docs/MATRIX.md`](docs/MATRIX.md) for the failure-evidence matrix.
matrix are the next piece of work.

## Layout

| Path | What it holds |
| --- | --- |
| `CLAIMS.md` | The pre-registered contract. Frozen. |
| `docs/run_schedule.json` | The frozen 21-run schedule. |
| `docs/agent_trace.json` | The frozen agent tool-call trace. |
| `docs/MATRIX.md` | The failure-evidence matrix. The product. |
| `docs/evidence/matrix/` | All 42 executions, ledgers gzipped with digests. |
| `docs/evidence/control-run/` | The control run's evidence. An apparatus check, not a claim result. |
| `src/proofbench/config.py` | Single authority for every connection detail and frozen constant. |
| `src/proofbench/core/schedule.py` | The pure deterministic schedule generator. |
| `src/proofbench/core/trace.py`, `core/saga.py` | The workload: templates, and the expansion into 600 side effects. |
| `src/proofbench/core/configs.py` | The two configurations under test, and the INV-P3 allow-list. |
| `src/proofbench/core/ledger_diff.py` | The INV-P2 differ. |
| `src/proofbench/core/recovery.py` | The frozen recovery and resume contracts. |
| `src/proofbench/core/run.py` | The run driver: ingest, process, verify, diff. |
| `src/proofbench/core/faults.py` | The fault injector, armed once, and the sole `os.kill` site. |
| `src/proofbench/core/matrix.py` | The matrix, its validity rules, and the loss-possibility predicate. |
| `src/proofbench/core/claims.py` | C1, C2 and C3 against the frozen floors. |
| `src/proofbench/interfaces/ledger.py` | The INV-P2 diff interface. |
| `adr/` | Architecture decision records. |
| `tests/hygiene/`, `tests/portability/` | The gates, each with a seeded violation. |
| `tests/integration/` | Needs a broker; skips with a named reason without one. |
