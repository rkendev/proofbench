#!/usr/bin/env python3
"""C3: replay every scoreable good-configuration run and checksum the rebuilt ledger.

Raw topic bytes cannot match a replay, since offsets, timestamps and producer epochs are
broker-assigned rather than produced by the consumer under test. ADR-0004 section 5 fixes
the comparison as the canonical serialization of the rebuilt SideEffectRecord ledger.

The denominator is named and every excluded run listed, so nobody reverse-engineers why
21 became 19.
"""

from __future__ import annotations

import json

from proofbench.config import get_settings, repo_root
from proofbench.core.claims import evaluate_c3
from proofbench.core.configs import build_configuration
from proofbench.core.evidence import write_json
from proofbench.core.recovery import RecoveryBudget
from proofbench.core.replay import compare, replay_configuration
from proofbench.core.run import process, verify
from proofbench.core.topics import provision
from proofbench.core.txn import TransactionLedger

GOOD = "good"


def main() -> int:
    settings = get_settings()
    matrix = json.loads((repo_root() / settings.run_output_dir / "matrix.json").read_text())

    scoreable = [
        e
        for e in matrix["executions"]
        if e["configuration"] == GOOD and e["status"] in ("clean", "not_clean")
    ]
    excluded = [
        e["run_id"]
        for e in matrix["executions"]
        if e["configuration"] == GOOD and e["status"] == "apparatus_failure"
    ]
    print(f"C3: replaying {len(scoreable)} good runs, excluding {excluded or 'none'}", flush=True)

    outcomes = []
    for index, execution in enumerate(scoreable, start=1):
        run_id = int(execution["run_id"])
        original = build_configuration(GOOD, run_id, settings)
        replay = replay_configuration(original, settings)

        bootstrap = settings.broker_bootstrap_servers
        assert bootstrap
        provision(bootstrap, (replay.topics.sink_a, replay.topics.sink_b))

        process(
            replay,
            settings,
            settings.sagas_per_run,
            RecoveryBudget(),
            TransactionLedger(),
            is_control=False,
        )
        for name, original_topic, replay_topic in (
            ("sink_a", original.topics.sink_a, replay.topics.sink_a),
            ("sink_b", original.topics.sink_b, replay.topics.sink_b),
        ):
            before = tuple(verify(original, original_topic, settings.steps_per_saga))
            after = tuple(verify(replay, replay_topic, settings.steps_per_saga))
            outcomes.append(compare(before, after, run_id, GOOD, name))
        print(
            f"  [{index}/{len(scoreable)}] run {run_id:02d}: "
            f"{'match' if all(o.matched for o in outcomes[-2:]) else 'DIFFERS'}",
            flush=True,
        )

    verdict = evaluate_c3(outcomes, excluded)
    out = repo_root() / settings.run_output_dir / "replay.json"
    write_json(
        out, {"verdict": verdict.to_jsonable(), "outcomes": [o.to_jsonable() for o in outcomes]}
    )
    print(f"\nC3 {verdict.verdict}: {verdict.observed}")
    print(f"written to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
