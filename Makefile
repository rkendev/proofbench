# Canonical operator commands. Recipe lines are TAB-indented.
# PYTHONPATH=src and a .venv-based toolchain so no global tool is required.
# Every CI step invokes one of these targets, so what CI runs and what an
# operator runs cannot drift apart.

PYTHON ?= python3.12
VENV := .venv
BIN := $(VENV)/bin
export PYTHONPATH := src

.PHONY: bootstrap hooks hygiene verify-versions lint type-check test schedule \
	broker-up run-matrix

# Idempotent: safe to re-run. Creates the venv only if absent, asserts it is
# Python 3.12, installs pinned deps, wires the pre-commit git hook where the
# environment allows it, then runs the hygiene gate. The first gate run needs
# network to clone the hook repos; after that they are cached.
bootstrap:
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	$(BIN)/python -c 'import sys; assert sys.version_info[:2] == (3, 12), "venv interpreter is not Python 3.12: %s" % sys.version'
	$(BIN)/python -m pip install --upgrade pip
	$(BIN)/pip install -r requirements.txt -r requirements-dev.txt
	$(MAKE) hooks
	$(MAKE) hygiene

# Wire the pre-commit git-hook shim. Skipped (not failed) only when the
# environment sets core.hooksPath, which makes pre-commit refuse the shim; the
# hygiene gate in bootstrap still validates and runs every hook regardless, so
# no hook-install failure is ever swallowed.
hooks:
	@if [ -n "$$(git config core.hooksPath 2>/dev/null)" ]; then \
		echo "note: git core.hooksPath is set; skipping the pre-commit git-hook shim (the hygiene gate still runs all hooks)."; \
	else \
		$(BIN)/pre-commit install; \
	fi

# The build-hygiene gate on its own: validate the pre-commit config, then run
# every hook over the whole tree. Called from bootstrap and by CI, where each
# gate is a separate named step so a failure has its own line on the run page.
hygiene:
	PATH="$(CURDIR)/$(BIN):$$PATH" bash scripts/verify-precommit.sh

# The version-consistency gate on its own (INV-5). It also runs as a pre-commit
# hook; this target is how CI reports it as a check of its own rather than as a
# line buried in another command's output.
verify-versions:
	$(BIN)/python scripts/verify_versions.py

lint:
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

# Full strict mypy on the package, plus the operator scripts. The scripts are
# checked as a directory rather than named one by one, so a new script is covered
# the moment it lands instead of when someone remembers to add it here.
type-check:
	$(BIN)/mypy src scripts/

test:
	$(BIN)/pytest

# Regenerate the committed run schedule from the master seed. This is a no-op on
# a clean tree: the schedule is frozen, and tests/unit/test_schedule_frozen.py
# fails if regenerating does not reproduce the committed file byte for byte. Run
# it to prove the artifact is reproducible, not to change it.
schedule:
	$(BIN)/python scripts/write_run_schedule.py

# Deferred to a later prompt. These exit non-zero rather than printing and
# succeeding, so a stub can never be chained into something that then reports
# success. PB-T1 deliberately boots no broker and measures nothing.
broker-up run-matrix:
	@echo "$@: not implemented until a later prompt (PB-T1 boots no broker and measures nothing)."
	@exit 2
