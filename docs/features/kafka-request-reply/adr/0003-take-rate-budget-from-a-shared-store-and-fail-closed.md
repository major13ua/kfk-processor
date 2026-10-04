---
status: Accepted
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-04"
feature_size: "M"
ticket: "none stated (spec §8)"
---

# 0003 — Take the Rate Budget from a shared store behind a port and fail closed

- **Status:** Accepted
- **Date:** 2026-10-04
- **Deciders:** Ievgen Chupryna (Architect), decided with the author during the design walk

## Context

One Rate Budget must hold across all workers of a group, including backlog catch-up and rollouts, so downstream services are protected (US-05, AC-10). Per-pod limits cannot do this when pod count changes.

The core of this decision was fixed in the spec and interview; the options marked "excluded by spec" record why the alternative does not stand, not an open choice.

## Decision drivers

- Goal 1: accepted rate ≤ Rate Budget × 1.10 in any sliding 1 s window (spec §6, provisional)
- AC-18: pause when the store is unavailable, resume without exceeding the budget
- Existing capability: an in-memory store XME already runs (spec §8, Redis-compatible chosen for the first adapter)

## Considered options

1. **Shared counter store behind a port, Redis-compatible first adapter, allowance taken before fetching, fail closed** — one allowance for the group; other stores pluggable.
2. **Per-worker local limit of budget ÷ worker count** (excluded by spec §1: global budget) — needs the worker count; breaks during rollouts and scaling.
3. **Fail open to a local fallback limit when the store is down** (excluded by spec AC-18) — keeps serving but can exceed the budget, defeating US-09.

## Decision outcome

**Chosen:** Shared store behind a port, fail closed. Pause within 5 s of the store becoming unreachable, resume within 30 s of its return (spec §6).

## Consequences

**Positive**
- The budget holds whatever the number of workers
- The store is replaceable without touching the engine

**Negative**
- The store becomes a hard dependency of intake: its outage pauses all workers
- One store round trip per Cycle adds latency to intake

**Neutral**
- Bucket4j's distributed backends are a candidate implementation, to be confirmed in `tasks`

## Links

- Spec: [[../spec.md]]
- SAD: [[../sad.md]] §4
- Related ADR: none
