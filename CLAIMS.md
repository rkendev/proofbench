# ProofBench: CLAIMS.md

Entry artifact, pre-registered 2026-07-28, BEFORE any repo, any infrastructure, or any broker boot. Concept frozen by the council of 2026-07-24; promoted to the next build slot by the recorded owner override of 2026-07-26. This document is the contract: the claims, floors, and ship rules below are fixed before the first measurement, and results ship against them unmodified. Claim sources: the three Kafka KB entries of 2026-07-24 (delivery semantics; partitioning and consumer-group mechanics; broker cost efficiency) and the system-design saga entry they cross-link.

## What ProofBench is

A kill-test harness that measures what Kafka delivery configuration actually does under injected failure, and ships the evidence. The workload is an LLM agent's tool-call side effects modeled as a saga: multi-step sequences such as create_ticket, charge_card, send_confirmation, where a duplicated side effect is a double charge and a lost one is a silently dropped step. The harness kills producers, consumers, and the broker at seeded points mid-saga and counts, in committed code, how many side effects were duplicated or lost per configuration. The harness and its failure-evidence matrix (configuration by injected crash by measured outcome) are the product.

What it is not: another ingest pipeline (the Climate Impact Index owns that shelf), a dashboard, or a cloud deployment. The KB entry that sources these claims carries confidence Medium with the note "not yet reproduced in-house"; ProofBench is that in-house reproduction, and the entry gets amended with the measured results whichever way they land.

## The three pre-registered claims

C1, exactly-once under kill. With an idempotent transactional producer, a read_committed consumer, and offset commits placed inside the transaction, the harness observes zero duplicated and zero lost side effects across at least 20 seeded kill runs. Floor: a single duplicate or loss means the headline ships FAILED, published as the result, not softened.

C2, harness sensitivity. The known-bad baseline (enable.auto.commit with commit-before-processing placement, no idempotence) exhibits at least one lost side effect in at least 80 percent of the same 20 seeded runs. Floor: if the baseline survives the kills, the harness cannot distinguish configurations, is declared insensitive, and every result ships report-only. A harness that cannot make the bad config fail proves nothing when the good config passes.

C3, replay determinism. Replaying the full event log through the committed consumer rebuilds the sink byte-identical to the original run, verified by checksum. Floor: any difference ships as a documented negative.

## What one seeded kill run is

A deterministic seed fixes the saga stream (N sagas of M steps), the fault point, and the fault type. Fault menu v1, taken directly from the KB's machine-checkable scenarios: SIGKILL the producer mid-send window; SIGKILL the consumer between sink A and sink B (the partial-write duplication case); stop and restart the broker mid-run. The 20-run schedule (fault menu crossed with seeds) is committed before the first boot. Every side effect carries an idempotency key; the sink ledger is diffed against the expected saga ledger by committed code. No count is ever taken by eye.

Honest scope limit, stated up front: the broker runs as single-node KRaft in Docker (with explicit replication-factor settings for the offsets and transaction-state internal topics, per the KB setup note). A single node cannot demonstrate ISR leader failover, so broker faults here are stop/start outages; failover measurement is out of scope for v1. Client is confluent-kafka; the idempotence flag is spelled enable.idempotence there, not the kafka-python spelling, a recorded cross-client gotcha.

## Budget, timebox, and interruptions

Hard spend cap: 30 euros. Expected spend: near zero, since everything runs in local Docker and the agent tool-call trace is recorded once (pennies of model tokens at most, no live model in any harness run). If any cloud resource is ever created, the project cost-allocation tag gets activated in billing before the first paid resource exists. Timebox: a publishable intermediate within 2 to 3 weeks. Recruiter work outranks harness polish at all times; interview preparation interrupts this project without ceremony.

## Ship rules

All three claims hold: the headline is the failure-evidence matrix. C1 fails: the FAILED headline ships. C2 fails: everything ships report-only. C3 fails: documented negative. No outcome widens scope, adds features, or reopens other projects.

## Blurb (three lines, for the Projects section when it ships)

ProofBench is a kill-test harness for Kafka delivery guarantees, run against an AI agent's tool-call side effects. It kills producers, consumers, and the broker at seeded points and counts duplicated or lost side effects per configuration, so exactly-once is a measured number instead of a config comment. Every pass/fail floor was written down before the first broker boot, and a failed claim ships as the headline.
