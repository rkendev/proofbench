#!/usr/bin/env python3
"""Compute C1, C2 and C3 from the committed matrix, and print the numbers behind them.

No verdict in this project is read off a matrix by eye. The denominators are asserted
before any verdict is computed, because C1 is a universal quantifier and a universal
quantifier over an empty or mis-filtered collection returns a pass.
"""

from __future__ import annotations

import json

from proofbench.config import get_settings, repo_root
from proofbench.core.claims import evaluate
from proofbench.core.evidence import write_json
from proofbench.core.matrix import Execution, Matrix
from proofbench.core.replay import ReplayOutcome


def main() -> int:
    settings = get_settings()
    base = repo_root() / settings.run_output_dir
    payload = json.loads((base / "matrix.json").read_text())

    matrix = Matrix(
        executions=[
            Execution(
                run_id=int(e["run_id"]),
                configuration=str(e["configuration"]),
                fault_type=str(e["fault_type"]),
                is_control=bool(e["control"]),
                status=str(e["status"]),
                duplicated=int(e["duplicated"]),
                lost=int(e["lost"]),
                loss_possible=bool(e["loss_structurally_possible"]),
                transactions_committed=int(e["transactions_committed"]),
                transactions_aborted=int(e["transactions_aborted"]),
                max_open_transaction_ms=float(e["max_open_transaction_ms"]),
                recovery=dict(e["recovery"]),
                offset_gaps=list(e.get("offset_gaps", [])),
                records_in_gaps=int(e.get("records_in_gaps", 0)),
                attempts=int(e.get("attempts", 1)),
                redeliveries=int(e.get("redeliveries", 0)),
                diagnosis=str(e.get("diagnosis", "")),
            )
            for e in payload["executions"]
        ],
        cycle=int(payload.get("cycle", 1)),
    )

    replay_path = base / "replay.json"
    replays: list[ReplayOutcome] = []
    excluded: list[int] = []
    if replay_path.exists():
        replay_payload = json.loads(replay_path.read_text())
        excluded = list(replay_payload["verdict"].get("runs_excluded", []))
        replays = [
            ReplayOutcome(
                run_id=int(o["run_id"]),
                configuration=str(o["configuration"]),
                original_checksum=str(o["original_checksum"]),
                replayed_checksum=str(o["replayed_checksum"]),
                original_records=int(o["original_records"]),
                replayed_records=int(o["replayed_records"]),
                sink=str(o["sink"]),
            )
            for o in replay_payload["outcomes"]
        ]

    result = evaluate(matrix, replays, excluded)
    write_json(base / "claims.json", result)

    for verdict in result["verdicts"]:
        print(f"{verdict['claim']}: {verdict['verdict']}")
        print(f"   floor    : {verdict['floor']}")
        print(f"   observed : {verdict['observed']}")
    subset = result["loss_capable_subset"]
    print(f"\n{subset['figure']} ({subset['status']}, threshold {subset['threshold']})")
    print(f"   {subset['attained']} of {subset['denominator']}, ceiling {subset['ceiling']}")
    print(f"   by fault type: {subset['by_fault_type']}")
    print(f"\nships report-only: {result['ship_rule']['everything_ships_report_only']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
