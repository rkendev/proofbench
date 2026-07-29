# Control run evidence: run_id 0, both configurations

**This is an apparatus check. It is not a claim result, and it is not evidence for
C1, C2, or C3.**

## What it is

`run_id` 0 of the frozen schedule is the no-fault control. It carries fault type
`none` and no fault point. It was executed under both pre-registered
configurations with **no fault injected of any kind**, on 2026-07-29, before any
fault injector existed in the repository.

The result, identical for both configurations and both sinks:

| | good | baseline |
| --- | --- | --- |
| expected records | 600 | 600 |
| sink A visible / duplicated / lost | 600 / 0 / 0 | 600 / 0 / 0 |
| sink B visible / duplicated / lost | 600 / 0 / 0 | 600 / 0 / 0 |
| transactions committed | 200 | 0 |
| transactions aborted | 0 | 0 |
| producer re-initialisations | 0 | 0 |
| status | clean | clean |

## What it shows

That the harness reports zero when nothing was killed. Every gate in this
repository has been demonstrated failing against a seeded violation, which shows a
gate fires when it should. The control is the missing half: it shows the apparatus
does not fire when it should not.

It was run before the fault injector existed on purpose. Doing it the other way
round would leave every later number open to the objection that the apparatus was
adjusted once its answers were visible.

The baseline being clean here matters as much as the good configuration being
clean. A known-bad configuration that fails a run with no fault would mean the
harness cannot tell a bad configuration from a broken one, and every later kill
result would be uninterpretable.

That the good configuration committed 200 transactions and the baseline committed
none is the frozen `per_saga` transaction boundary and the baseline's
non-transactional nature, both observed rather than assumed.

## What it does NOT show

- **Nothing about C1.** C1 is about behaviour under kill. Nothing was killed here.
  A clean control is a precondition for trusting a later C1 result, not a
  contribution to one.
- **Nothing about C2.** The baseline is expected to be clean without a fault. Its
  defect is supposed to appear under kill, and no kill was applied.
- **Nothing about C3.** No replay was performed.
- **Nothing about sink ordering.** With no fault injected, both sinks hold
  everything under either order, so this run is byte-identical whether sink A is
  written first or second. That ordering is pinned by
  `tests/unit/test_sink_ordering.py` instead, and ADR-0003 section 4 says why.

## Reproducing it

```
make broker-up
export PB_BROKER_BOOTSTRAP_SERVERS=127.0.0.1:29092
make control-run
```

The evidence lands in the git-ignored `runs/run_00/`. The files here are the
copies committed as a record.

## What is in each directory

| File | What it holds |
| --- | --- |
| `run_summary.json` | counts, transaction and recovery totals, status |
| `diff_sink_a.json`, `diff_sink_b.json` | the ledger diffs, including the duplicated and lost records themselves |
| `resolved_config.json` | **both** configurations in full, plus the client and librdkafka versions |
| `schedule_entry.json` | the frozen schedule entry that was run |
| `ledger_checksums.json` | SHA-256 of the three full ledgers, which are too large to commit |

`resolved_config.json` carries both configurations rather than only the one that
ran, so the allow-listed difference between them is visible in one file. The
client versions are there because `transaction.timeout.ms` and every other
librdkafka default the harness does not restate is owned by the pin, and an
undeclared default is an undeclared input.

The full ledgers are 160 KB each and live under `runs/`. The expected ledger is
reproducible from the run seed and the committed trace, so anyone can rebuild it
and check its digest. The observed ledgers are not reproducible, so their digests
are what a later reader can hold this run to.

## If it is ever not clean

That is a harness defect and it blocks the matrix (ADR-0002). It is not reported
as a finding, and it is not re-run to see whether it was a flake. A
retry-until-clean loop on the apparatus check is exactly the habit this project
exists to disprove.
