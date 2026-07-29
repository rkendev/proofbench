# ADR-0001: Record architecture decisions

Status: accepted
Date: 2026-07-29

## Context

ProofBench exists to produce evidence, and evidence is only worth as much as the
record of how it was produced. The claims themselves are already frozen in CLAIMS.md,
committed as this repository's first commit before any code existed. What CLAIMS.md
does not carry is the reasoning behind the design choices that sit underneath it: why
a stream is 200 sagas long, why the master seed is the number it is, where a fault
lands. Those choices determine whether a claim can pass or fail for real reasons, so
a future reader needs to know not just what was chosen but why, and what would
reopen the choice.

The portfolio this project sits in already uses architecture decision records with a
falsifiable reopen trigger on each significant decision.

## Decision

Every significant decision is recorded as a numbered ADR in this folder. Each ADR
states context, the decision, the alternatives considered, the consequences, and a
falsifiable trigger that would reopen it.

CLAIMS.md sits above the ADRs and is not one of them. An ADR may record how a claim
is measured; no ADR may modify, soften, reword, or reinterpret a claim, a floor, or a
ship rule. Where an ADR and CLAIMS.md appear to disagree, CLAIMS.md wins and the ADR
is wrong.

## Consequences

The measurement invariants have their own ADR, 0002, because they are the standing
law the harness is tested against and because they carry the dated record of which
experiment constants were fixed before the first broker boot. That dating is not
decoration: it is what distinguishes ordinary experimental design from choosing a
number after seeing which number gives a better result.

## Reopen trigger

If the project adopts a decision-tracking mechanism that supersedes flat ADR files,
migrate the existing records rather than abandoning them.
