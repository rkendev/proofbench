# The failure-evidence matrix

**Every result here ships report-only.** C2, the pre-registered sensitivity gate, did not
meet its floor, and CLAIMS.md fixes the consequence: "the harness cannot distinguish
configurations, is declared insensitive, and every result ships report-only." That applies
to the clean C1 below as much as to anything else.

42 executions, 21 schedule entries under two configurations, strictly sequential, cycle 3
of a permitted 3. Wall clock 29.0 minutes. Zero apparatus
failures.

## The three verdicts

| claim | verdict | floor | observed |
| --- | --- | --- | --- |
| C1 | **HOLDS** | zero duplicated and zero lost across all 20 kill runs under good | 0 duplicated, 0 lost across 20 kill runs |
| C2 | **FAILED** | at least 16 of 20 kill runs lose at least one | 7 of 20 kill runs exhibited loss |
| C3 | **HOLDS** | every replay rebuilds the sink ledger to the same checksum | 42 of 42 sink replays matched |

Computed by committed code in `src/proofbench/core/claims.py`, never read off this table by
eye, with the denominators asserted before any verdict was produced.

### C2 failed as predicted, and the prediction is dated

ADR-0004 section 8 recorded, **before the matrix ran**, that C2 could not reach its floor.
The ingest resume rule frozen in ADR-0003 section 7 makes the ingest phase incapable of
losing a side effect, so the 7 `producer_sigkill_mid_send` runs cannot contribute loss and
the maximum attainable numerator is 13 against a floor of 16.

The finding, precisely: **C2 did not fail because the harness could not distinguish the
configurations.** It failed because the pre-registered denominator included 7 runs in which
loss is structurally impossible under a resume contract chosen later but still blind.

### The loss-capable subset, report-only

7 of 13, ceiling 13.
**No threshold applies to this figure. It is not C2, it is not C2 on a subset, and nothing
passes or fails on it.**

By fault type: {"broker_stop_start": {"exhibited_loss": 0, "runs": 6}, "consumer_sigkill_between_sinks": {"exhibited_loss": 7, "runs": 7}}

### One derived observation, not pre-registered, satisfying no floor

Under the baseline, **13 of the 13 loss-capable runs exhibited a defect**: 7 by loss and 6
by duplication. Under the good configuration, 0 of 20. This is an observation. It is not a
claim, it did not have a floor, and it must never be read as C2 passing on a better
denominator.

## The cleanest contrast in the project

Six `broker_stop_start` runs per configuration. Identical code, identical recovery path,
one `reinit_producer` on `_MSG_TIMED_OUT` in each.

| configuration | duplicated | why |
| --- | --- | --- |
| baseline | **3, in all six** | no transaction to abort, so the first attempt is already durable and the replay makes a second copy |
| good | **0, in all six** | the aborted transaction makes the first attempt invisible |

Six of six, deterministic, not a race. This is `enable.idempotence` and `transactional.id`
doing exactly what CLAIMS.md says they do, with everything else held identical by INV-P3.

## Honest limits on C1

**It measures exactly-once within Kafka only.** The input topic, both sinks and the
consumer offsets are all Kafka resources, so one transaction covers them. ADR-0003 section
2: a C1 pass here is **not** evidence that an agent's real side effects are safe under the
same configuration.

**In these 42 executions the consumer never experienced a re-delivery.** Every execution
recorded `redeliveries = 0`. Duplicate-delivery recovery is therefore covered by forced
deterministic tests, not by this scored matrix.

**Single-node KRaft.** Broker faults are stop and start outages; ISR leader failover is out
of scope for v1.

**The D4 timing profile.** The baseline's commit timer is given one full opportunity to
fire at the seeded fault point, so the measured claim is "commit-before-processing loses
side effects when the commit has had one opportunity to fire", not "under a realistic
production cadence".

## The 42 executions

`loss?` is `loss_structurally_possible`, computed from the frozen schedule by
`matrix.loss_structurally_possible`, so the ceiling of 13 is checkable without trusting any
prose. `open ms` is the longest any transaction stayed open, against a pinned
`transaction.timeout.ms` of 60000.

| run | config | fault | saga/step | loss? | dup | lost | txn ok | txn abort | open ms | recov | redeliv | status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 00 | baseline | none | - | no | 0 | 0 | 0 | 0 | 0.0 | 0 | 0 | clean |
| 00 | good | none | - | no | 0 | 0 | 400 | 0 | 51.2 | 0 | 0 | clean |
| 01 | baseline | producer_sigkill_mid_send | 96/2 | no | 0 | 0 | 0 | 0 | 0.0 | 0 | 0 | clean |
| 01 | good | producer_sigkill_mid_send | 96/2 | no | 0 | 0 | 400 | 0 | 59.6 | 0 | 0 | clean |
| 02 | baseline | consumer_sigkill_between_sinks | 108/1 | yes | 0 | 149 | 0 | 0 | 0.0 | 0 | 0 | not_clean |
| 02 | good | consumer_sigkill_between_sinks | 108/1 | yes | 0 | 0 | 400 | 0 | 38.8 | 0 | 0 | clean |
| 03 | baseline | broker_stop_start | 149/2 | yes | 3 | 0 | 0 | 0 | 0.0 | 1 | 0 | not_clean |
| 03 | good | broker_stop_start | 149/2 | yes | 0 | 0 | 400 | 0 | 88.5 | 1 | 0 | clean |
| 04 | baseline | producer_sigkill_mid_send | 106/1 | no | 0 | 0 | 0 | 0 | 0.0 | 0 | 0 | clean |
| 04 | good | producer_sigkill_mid_send | 106/1 | no | 0 | 0 | 400 | 0 | 72.2 | 0 | 0 | clean |
| 05 | baseline | consumer_sigkill_between_sinks | 147/2 | yes | 0 | 115 | 0 | 0 | 0.0 | 0 | 0 | not_clean |
| 05 | good | consumer_sigkill_between_sinks | 147/2 | yes | 0 | 0 | 400 | 0 | 48.2 | 0 | 0 | clean |
| 06 | baseline | broker_stop_start | 117/2 | yes | 3 | 0 | 0 | 0 | 0.0 | 1 | 0 | not_clean |
| 06 | good | broker_stop_start | 117/2 | yes | 0 | 0 | 400 | 0 | 83.5 | 1 | 0 | clean |
| 07 | baseline | producer_sigkill_mid_send | 91/2 | no | 0 | 0 | 0 | 0 | 0.0 | 0 | 0 | clean |
| 07 | good | producer_sigkill_mid_send | 91/2 | no | 0 | 0 | 400 | 0 | 121.1 | 0 | 0 | clean |
| 08 | baseline | consumer_sigkill_between_sinks | 61/2 | yes | 0 | 31 | 0 | 0 | 0.0 | 0 | 0 | not_clean |
| 08 | good | consumer_sigkill_between_sinks | 61/2 | yes | 0 | 0 | 400 | 0 | 44.4 | 0 | 0 | clean |
| 09 | baseline | broker_stop_start | 133/2 | yes | 3 | 0 | 0 | 0 | 0.0 | 1 | 0 | not_clean |
| 09 | good | broker_stop_start | 133/2 | yes | 0 | 0 | 400 | 0 | 88.9 | 1 | 0 | clean |
| 10 | baseline | producer_sigkill_mid_send | 90/1 | no | 0 | 0 | 0 | 0 | 0.0 | 0 | 0 | clean |
| 10 | good | producer_sigkill_mid_send | 90/1 | no | 0 | 0 | 400 | 0 | 37.6 | 0 | 0 | clean |
| 11 | baseline | consumer_sigkill_between_sinks | 120/1 | yes | 0 | 77 | 0 | 0 | 0.0 | 0 | 0 | not_clean |
| 11 | good | consumer_sigkill_between_sinks | 120/1 | yes | 0 | 0 | 400 | 0 | 46.4 | 0 | 0 | clean |
| 12 | baseline | broker_stop_start | 61/1 | yes | 3 | 0 | 0 | 0 | 0.0 | 1 | 0 | not_clean |
| 12 | good | broker_stop_start | 61/1 | yes | 0 | 0 | 400 | 0 | 82.9 | 1 | 0 | clean |
| 13 | baseline | producer_sigkill_mid_send | 75/2 | no | 0 | 0 | 0 | 0 | 0.0 | 0 | 0 | clean |
| 13 | good | producer_sigkill_mid_send | 75/2 | no | 0 | 0 | 400 | 0 | 61.0 | 0 | 0 | clean |
| 14 | baseline | consumer_sigkill_between_sinks | 143/1 | yes | 0 | 139 | 0 | 0 | 0.0 | 0 | 0 | not_clean |
| 14 | good | consumer_sigkill_between_sinks | 143/1 | yes | 0 | 0 | 400 | 0 | 39.0 | 0 | 0 | clean |
| 15 | baseline | broker_stop_start | 90/1 | yes | 3 | 0 | 0 | 0 | 0.0 | 1 | 0 | not_clean |
| 15 | good | broker_stop_start | 90/1 | yes | 0 | 0 | 400 | 0 | 131.7 | 1 | 0 | clean |
| 16 | baseline | producer_sigkill_mid_send | 45/2 | no | 0 | 0 | 0 | 0 | 0.0 | 0 | 0 | clean |
| 16 | good | producer_sigkill_mid_send | 45/2 | no | 0 | 0 | 400 | 0 | 38.0 | 0 | 0 | clean |
| 17 | baseline | consumer_sigkill_between_sinks | 157/1 | yes | 0 | 55 | 0 | 0 | 0.0 | 0 | 0 | not_clean |
| 17 | good | consumer_sigkill_between_sinks | 157/1 | yes | 0 | 0 | 400 | 0 | 39.1 | 0 | 0 | clean |
| 18 | baseline | broker_stop_start | 52/1 | yes | 3 | 0 | 0 | 0 | 0.0 | 1 | 0 | not_clean |
| 18 | good | broker_stop_start | 52/1 | yes | 0 | 0 | 400 | 0 | 90.4 | 1 | 0 | clean |
| 19 | baseline | producer_sigkill_mid_send | 145/2 | no | 0 | 0 | 0 | 0 | 0.0 | 0 | 0 | clean |
| 19 | good | producer_sigkill_mid_send | 145/2 | no | 0 | 0 | 400 | 0 | 54.1 | 0 | 0 | clean |
| 20 | baseline | consumer_sigkill_between_sinks | 95/2 | yes | 0 | 27 | 0 | 0 | 0.0 | 0 | 0 | not_clean |
| 20 | good | consumer_sigkill_between_sinks | 95/2 | yes | 0 | 0 | 400 | 0 | 49.9 | 0 | 0 | clean |

## Reproducing it

Every ledger is committed gzipped with `mtime=0`, so identical records produce identical
bytes. Digests cover the uncompressed documents, so any gzip tool reproduces them.

```
make broker-up
export PB_BROKER_BOOTSTRAP_SERVERS=127.0.0.1:29092
make run-matrix
make replay
make evaluate-claims
```

The expected ledger is a pure function of the run seed and the committed trace, so a reader
who trusts neither the author nor these files can regenerate it and check the digests.
