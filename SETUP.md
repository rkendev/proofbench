# SETUP: clone to green

The deterministic clone-to-green checklist for ProofBench. A clean bootstrap must
reach green with no manual step. The time below is measured, not asserted.

## Prerequisites

- Python 3.12 available on PATH as `python3.12` (override with `make PYTHON=...`).
- `git` and network access. The first hygiene-gate run clones the pre-commit hook
  repositories and builds their environments; after that they are cached.

Nothing else. There is no broker, no container runtime, and no cloud account in this
setup, because PB-T1 boots no broker and measures nothing.

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

## Targets that are deliberately not implemented yet

`make broker-up` and `make run-matrix` print a deferral note and exit non-zero. They
are named here because they are planned, not because they exist. They exit non-zero
rather than printing and succeeding so that a stub can never be chained into something
that then reports success.
