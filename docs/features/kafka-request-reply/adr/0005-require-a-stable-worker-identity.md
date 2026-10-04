---
status: Accepted
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-04"
feature_size: "M"
ticket: "none stated (spec §8)"
---

# 0005 — Require a stable worker identity for group membership and transactions

- **Status:** Accepted
- **Date:** 2026-10-04
- **Deciders:** Ievgen Chupryna (Architect), decided with the author during the design walk

## Context

Rolling deployments must not reassign lanes across the group (US-07), and a transaction needs one identity that survives a restart so an old instance can be fenced. Deployments without stable identity are a non-goal (spec §3).

The core of this decision was fixed in the spec and interview; the options marked "excluded by spec" record why the alternative does not stand, not an open choice.

## Decision drivers

- AC-14/AC-15: restart one by one with the same identity, no reassignment; refuse to start without an explicit identity
- Spec §6: Identity window 45 s (provisional); lane reassignment 0 for returning workers
- Deployment target: stable-identity replicas

## Considered options

1. **One explicit worker identity drives static group membership and the transaction identity; Identity window 45 s** — a returning worker resumes its lanes and fences its predecessor.
2. **Dynamic membership with rebalancing** (excluded by spec §3, AC-14) — every restart reassigns lanes across the group, which US-07 forbids.
3. **Derive identity from the host name when none is configured** (excluded by AC-15) — the runtime cannot tell whether a host name is stable.

## Decision outcome

**Chosen:** Explicit identity required; absent identity refuses start (AC-15). The same identity is used for group membership and the transaction.

## Consequences

**Positive**
- Rollouts do not disturb surviving workers
- Zombie instances are fenced

**Negative**
- Lost or scaled-down workers hold their lanes until the Identity window ends (runbook and stranded-lane alert, spec §8)
- Teams without stable identity cannot adopt

**Neutral**
- Identity must be unique per worker in a group; duplicates are a startup error

## Links

- Spec: [[../spec.md]]
- SAD: [[../sad.md]] §4
- Related ADR: [[0002-commit-replies-and-positions-in-one-transaction-per-cycle]]
