#!/usr/bin/env bash
# Build-hygiene gate.
#
# Validates that the pre-commit configuration parses, then runs every hook
# against all files. A broken config makes `validate-config` fail first, so the
# gate goes red before any hook install. There is deliberately no `|| true`
# anywhere: a hook-install or parse failure must not be swallowed, which is the
# exact failure mode that once hid a broken config in a sibling build.
#
# Usage: scripts/verify-precommit.sh [config-path]
#   config-path defaults to .pre-commit-config.yaml
set -euo pipefail

CONFIG="${1:-.pre-commit-config.yaml}"

echo "verify-precommit: validating ${CONFIG}"
pre-commit validate-config "${CONFIG}"

echo "verify-precommit: running all hooks against all files"
pre-commit run --all-files --config "${CONFIG}"

echo "verify-precommit: OK"
