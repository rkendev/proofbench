#!/usr/bin/env python3
"""Write the frozen run schedule to its committed location.

This is the only thing in the repository that puts the schedule on disk; the
generator itself (proofbench.core.schedule) is pure. On a clean tree this script is
a no-op, because the schedule is frozen and the committed artifact already holds
exactly these bytes. Run it to prove the artifact is reproducible:

    make schedule && git diff --exit-code docs/run_schedule.json

A non-empty diff means either the artifact or a frozen constant was changed, and
tests/unit/test_schedule_frozen.py fails for the same reason. That is the intended
behaviour, not an inconvenience: the numbers that determine a claim's outcome must
not be movable after the outcome is known.
"""

from __future__ import annotations

import argparse
import sys

from proofbench.config import get_settings, repo_root
from proofbench.core.schedule import build_schedule, serialize_schedule


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Do not write; exit non-zero if the committed artifact is out of date.",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    target = repo_root() / settings.schedule_path
    payload = serialize_schedule(build_schedule(settings))

    existing = target.read_text(encoding="utf-8") if target.exists() else None
    if args.check:
        if existing == payload:
            print(f"{settings.schedule_path}: up to date ({len(payload.encode())} bytes)")
            return 0
        print(
            f"{settings.schedule_path}: does NOT match a regeneration from the master seed",
            file=sys.stderr,
        )
        return 1

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(payload, encoding="utf-8")
    verb = "unchanged" if existing == payload else "written"
    print(f"{settings.schedule_path}: {verb} ({len(payload.encode())} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
