#!/usr/bin/env python3
"""Write the frozen agent tool-call trace to its committed location.

This is the only thing in the repository that puts the trace on disk; the
generator itself (proofbench.core.trace) is pure. On a clean tree this script is a
no-op, because the trace is frozen and the committed artifact already holds
exactly these bytes. Run it to prove the artifact is reproducible:

    make trace && git diff --exit-code docs/agent_trace.json

A non-empty diff means either the artifact or the master seed was changed, and
tests/unit/test_trace_frozen.py fails for the same reason.

Deliberately the same shape as scripts/write_run_schedule.py. The trace is an
input to every measurement, so it gets the same treatment as the schedule: one
canonical serializer shared by the writer and the gate, and a reproduce-from-seed
check rather than a checksum. ADR-0003 records why the trace is authored from the
seed rather than sampled from a live model.
"""

from __future__ import annotations

import argparse
import sys

from proofbench.config import get_settings, repo_root
from proofbench.core.trace import build_trace, serialize_trace


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Do not write; exit non-zero if the committed artifact is out of date.",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    target = repo_root() / settings.trace_path
    payload = serialize_trace(build_trace(settings))

    existing = target.read_text(encoding="utf-8") if target.exists() else None
    if args.check:
        if existing == payload:
            print(f"{settings.trace_path}: up to date ({len(payload.encode())} bytes)")
            return 0
        print(
            f"{settings.trace_path}: does NOT match a regeneration from the master seed",
            file=sys.stderr,
        )
        return 1

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(payload, encoding="utf-8")
    verb = "unchanged" if existing == payload else "written"
    print(f"{settings.trace_path}: {verb} ({len(payload.encode())} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
