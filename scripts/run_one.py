#!/usr/bin/env python3
"""Execute exactly one schedule entry under one named configuration.

Injects no fault of any kind. PB-T2 builds the apparatus and proves it reads
zero; the fault injector, the 20 kill runs, and the evidence matrix are PB-T3.

    python scripts/run_one.py --config good
    python scripts/run_one.py --run-id 0 --config baseline

Defaults to the control run, because that is the only run PB-T2 has any business
executing. Running a kill-run entry with no fault injected would produce evidence
that looked like a kill-run result and was not one, so it is refused rather than
merely discouraged: the entry carries a fault point, this script cannot inject
it, and a file on disk outlives the person who knew the difference.

Exit codes are meaningful, because this is what `make control-run` chains:

    0  the run completed and every sink diff was clean
    1  the run completed and a sink diff was not clean
    2  the run could not be completed, or was refused
"""

from __future__ import annotations

import argparse
import json
import sys

from proofbench.config import get_settings
from proofbench.core.configs import CONFIGURATION_NAMES
from proofbench.core.recovery import ApparatusFailure
from proofbench.core.run import execute_run, load_schedule_entry, write_evidence

EXIT_CLEAN = 0
EXIT_NOT_CLEAN = 1
EXIT_REFUSED = 2


def main(argv: list[str] | None = None) -> int:
    settings = get_settings()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-id",
        type=int,
        default=settings.control_run_id,
        help="Schedule entry to run. Defaults to the no-fault control run.",
    )
    parser.add_argument(
        "--config",
        required=True,
        choices=list(CONFIGURATION_NAMES),
        help="Which of the two pre-registered configurations to run under.",
    )
    args = parser.parse_args(argv)

    try:
        entry = load_schedule_entry(args.run_id, settings)
    except ApparatusFailure as exc:
        print(f"run refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    if entry["fault_point"] is not None:
        print(
            f"run refused: run_id {args.run_id} is a kill run carrying fault type "
            f"{entry['fault_type']!r}, and this harness injects no fault. Running it "
            f"now would write evidence that looked like a kill-run result and was "
            f"not one. The fault injector arrives in PB-T3.",
            file=sys.stderr,
        )
        return EXIT_REFUSED

    try:
        result = execute_run(args.run_id, args.config, settings)
    except ApparatusFailure as exc:
        print(f"apparatus failure, no result: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    directory = write_evidence(result, settings)
    print(json.dumps(result.summary(), indent=2, sort_keys=True))
    print(f"evidence written to {directory}")

    if not result.is_clean:
        print(
            "\nThe control run is NOT clean. That is an apparatus defect, not a "
            "finding, and it blocks the matrix (ADR-0002). Do not re-run it to see "
            "whether it was a flake.",
            file=sys.stderr,
        )
        return EXIT_NOT_CLEAN
    return EXIT_CLEAN


if __name__ == "__main__":
    raise SystemExit(main())
