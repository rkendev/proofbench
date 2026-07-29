# SETUP: clone to green

The deterministic clone-to-green checklist for ProofBench. A clean bootstrap must
reach green with no manual step. The time below is measured, not asserted.

## Prerequisites

- Python 3.12 available on PATH as `python3.12` (override with `make PYTHON=...`).
- `git` and network access. The first hygiene-gate run clones the pre-commit hook
  repositories and builds their environments; after that they are cached.

Nothing else is needed to reach green. The whole test suite runs offline: the
integration tests skip with a named reason when no broker is reachable, which is what
keeps CI booting nothing.

Docker is needed only to run the harness against a live broker. `make broker-up`
brings up single-node KRaft from `docker-compose.yml` and prints the address to
export; `make broker-status` reports ready only when the broker answers an API
request. No cloud account is involved at any point.

## Stopwatch checklist

Start the stopwatch, then run:

```
git clone https://github.com/rkendev/proofbench && cd proofbench
make bootstrap        # create .venv (Python 3.12), install pinned deps, run the hygiene gate
make lint             # ruff check + ruff format --check
make type-check       # strict mypy on src and scripts
make verify-versions  # pin consistency across the declaring files
make hygiene          # pre-commit validate-config + run --all-files
make test             # the whole suite
```

Stop the stopwatch when `make test` reports all tests passed.

`make bootstrap` is idempotent: re-running it reuses an existing `.venv`, asserts the
interpreter is Python 3.12, and re-runs the gate.

## Measured clone-to-green

Measured on the build host (Linux x86_64, Python 3.12.13), 29 tests passing in every
run, every command exiting zero:

| Scenario | Elapsed | What was cold |
| --- | --- | --- |
| Truly cold | 54 seconds | fresh clone, fresh `.venv`, empty pip wheel cache and empty pre-commit hook cache (measured under an isolated `HOME`) |
| Fresh clone, warm caches | 31 seconds | fresh clone and fresh `.venv`; pip wheel cache and pre-commit hook environments reused |
| Steady-state re-run | 13 seconds | nothing; existing `.venv` and hook cache, exercising the idempotent bootstrap |

All three figures cover the full chain above: `bootstrap`, `lint`, `type-check`,
`verify-versions`, `hygiene`, and `test`. The dominant variable cost is building the
pre-commit hook environments, which is why the truly cold figure is roughly 1.7 times
the warm one.

This is no longer a one-off measurement on one machine. The `checks` job in
`.github/workflows/ci.yml` bootstraps from a clean checkout on a hosted runner and
runs the same targets on every push, every pull request, weekly, and on demand. The
figures here stay as the recorded build-host measurement.

## Environment note

If the environment sets a global `git core.hooksPath` (as some managed hosts do),
`pre-commit` refuses to install its git-hook shim. `make bootstrap` detects this,
prints a note, and skips only the shim; the hygiene gate
(`scripts/verify-precommit.sh`) still validates the config and runs every hook, so no
hook-install failure is ever swallowed. To enable the on-commit shim locally, unset
the path (`git config --global --unset core.hooksPath`) and run `make hooks`.

Both branches of that target have been exercised: the skip branch on the build host,
which sets a global `core.hooksPath`, and the install branch under the isolated `HOME`
used for the cold measurement above, where `pre-commit` installed the shim normally.

## Verifying the frozen schedule yourself

The 21-run schedule in `docs/run_schedule.json` was committed before any broker boot.
To check that it is genuinely a function of the master seed rather than a hand-written
file:

```
make schedule                                   # regenerate from the master seed
git diff --exit-code docs/run_schedule.json      # must be empty
```

`make test` asserts the same thing byte for byte, and additionally that the artifact
holds exactly 20 kill runs plus one control, that the fault menu is distributed 7 / 7
/ 6, and that every fault point is strictly mid-saga.

## Running the harness against a live broker

Not needed to reach green, and not run by CI. Needs Docker.

```
make broker-up        # single-node KRaft; blocks until the broker answers, then prints the address
export PB_BROKER_BOOTSTRAP_SERVERS=127.0.0.1:29092
make broker-status    # ready means the broker answered an API request, not that a container is up
make control-run      # the no-fault control, under both configurations
make broker-down      # deliberate reset: removes the container and its log
```

`make control-run` takes about 17 seconds on the build host: roughly 9 for the good
configuration, which commits 200 transactions, and 2 for the baseline, which commits
none. The difference is the transactional round trips and is expected.

With the broker up and the address exported, `make test` also runs the integration
suite instead of skipping it, which takes it from about 1.5 seconds to about 23.

On a first boot against a fresh broker the client logs two warnings while acquiring a
transactional producer id, `Not coordinator` and `Coordinator load in progress`, each
followed by `retrying`. That is the transaction coordinator still starting up. The
client rides it out on its own and the run succeeds; no action is needed.

## Targets that are deliberately not implemented yet

`make run-matrix` prints a deferral note and exits non-zero. The fault injector, the
20 kill runs, and the evidence matrix are the next piece of work. It exits non-zero
rather than printing and succeeding so that a stub can never be chained into something
that then reports success.
