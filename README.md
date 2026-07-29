# ProofBench

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

## There are no results yet

Nothing has been measured. No broker has been booted in this repository, the harness
is not built, and the Kafka client is not yet a dependency. What exists today is the
pre-registered contract, the frozen run schedule, the measurement invariants, and the
gates that hold them in place.

This section gets replaced by the evidence matrix when there is one, whichever way the
claims land.

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
  side effects behind it. The interface is landed; there is no implementation yet.

Every gate in this repository has been demonstrated failing against a seeded violation.
A gate nobody has seen fail is not a gate.

## Stated scope limit

The broker will run as single-node KRaft in Docker, with explicit replication-factor
settings for the internal offsets and transaction-state topics. **A single node cannot
demonstrate ISR leader failover**, so broker faults here are stop and start outages,
and failover measurement is out of scope for v1. This limit is stated in `CLAIMS.md`
and repeated here because it bounds what any result can mean.

The client will be `confluent-kafka`, where the idempotence flag is spelled
`enable.idempotence` rather than with the kafka-python spelling. ADR-0002 records the
rest of that cross-client property mapping, including the fact that the Java consumer's
`max.poll.records` has no librdkafka equivalent.

## Getting started

See [`SETUP.md`](SETUP.md) for the clone-to-green checklist and its measured times.

```
make bootstrap    # venv, pinned deps, hygiene gate
make test         # the whole suite
make schedule     # regenerate the frozen schedule; must be a no-op
```

## Layout

| Path | What it holds |
| --- | --- |
| `CLAIMS.md` | The pre-registered contract. Frozen. |
| `docs/run_schedule.json` | The frozen 21-run schedule. |
| `src/proofbench/config.py` | Single authority for every connection detail and frozen constant. |
| `src/proofbench/core/schedule.py` | The pure deterministic schedule generator. |
| `src/proofbench/interfaces/ledger.py` | The INV-P2 diff interface. |
| `adr/` | Architecture decision records. |
| `tests/hygiene/`, `tests/portability/` | The gates, each with a seeded violation. |
