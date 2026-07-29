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

## There are no claim results yet

No claim has been measured. No fault has been injected, none of the 20 kill runs has
been executed, and C1, C2, and C3 have not been evaluated. What exists today is the
pre-registered contract, the frozen run schedule and workload, the measurement
invariants, the two configurations under test, the harness that runs them, and the
gates that hold all of it in place.

This section gets replaced by the evidence matrix when there is one, whichever way the
claims land.

### One number exists, and it is an apparatus check

`run_id` 0 of the schedule is a no-fault control. It has been run under both
configurations with **no fault injected**, and it reports zero duplicated and zero
lost side effects across 600 expected records, in both sinks, under both
configurations. The evidence is committed in
[`docs/evidence/control-run/`](docs/evidence/control-run/).

**What it shows:** that the harness reports zero when nothing was killed. Every gate
here has been demonstrated failing against a seeded violation, which shows a gate
fires when it should; the control is the missing half, showing the apparatus does not
fire when it should not. It was run before a fault injector existed on purpose,
because doing it the other way round would leave every later number open to the
objection that the apparatus was adjusted once its answers were visible.

**What it does not show:** anything about C1, C2, or C3. Nothing was killed, so it
says nothing about behaviour under kill. It is a precondition for trusting a later
result, not a contribution to one, and it is never described as evidence for a claim.

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

`make run-matrix` exits 2. The fault injector, the 20 kill runs, and the evidence
matrix are the next piece of work.

## Layout

| Path | What it holds |
| --- | --- |
| `CLAIMS.md` | The pre-registered contract. Frozen. |
| `docs/run_schedule.json` | The frozen 21-run schedule. |
| `docs/agent_trace.json` | The frozen agent tool-call trace. |
| `docs/evidence/control-run/` | The control run's evidence. An apparatus check, not a claim result. |
| `src/proofbench/config.py` | Single authority for every connection detail and frozen constant. |
| `src/proofbench/core/schedule.py` | The pure deterministic schedule generator. |
| `src/proofbench/core/trace.py`, `core/saga.py` | The workload: templates, and the expansion into 600 side effects. |
| `src/proofbench/core/configs.py` | The two configurations under test, and the INV-P3 allow-list. |
| `src/proofbench/core/ledger_diff.py` | The INV-P2 differ. |
| `src/proofbench/core/recovery.py` | The frozen recovery and resume contracts. |
| `src/proofbench/core/run.py` | The run driver: ingest, process, verify, diff. |
| `src/proofbench/interfaces/ledger.py` | The INV-P2 diff interface. |
| `adr/` | Architecture decision records. |
| `tests/hygiene/`, `tests/portability/` | The gates, each with a seeded violation. |
| `tests/integration/` | Needs a broker; skips with a named reason without one. |
