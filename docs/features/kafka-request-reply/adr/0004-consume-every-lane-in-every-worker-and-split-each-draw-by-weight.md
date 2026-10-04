---
status: Accepted
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-04"
feature_size: "M"
ticket: "none stated (spec §8)"
---

# 0004 — Consume every lane in every worker and split each worker's allowance draw by Priority Weight

- **Status:** Accepted
- **Date:** 2026-10-04
- **Deciders:** Ievgen Chupryna (Architect), decided with the author during the design walk

## Context

Lanes carry different priorities and share one Rate Budget (US-06). Weights must hold when all lanes are busy, no lane may starve, and idle lanes should not waste share (spec §8 defaults).

The core of this decision was fixed in the spec and interview; the options marked "excluded by spec" record why the alternative does not stand, not an open choice.

## Decision drivers

- AC-11: lane share within ±10 percentage points of its weight (spec §6, provisional)
- AC-12: minimum share 5% per lane (spec §8 default)
- AC-14: no lane reassignment during rollouts, so a lane must not live on a single worker

## Considered options

1. **Every worker consumes every lane; one global bucket, each worker splits its draw by weight with idle redistribution** — one store call per Cycle; accuracy rests on partitions spread evenly across workers.
2. **Per-lane buckets in the shared store** — weights hold globally, but more store calls and a second borrowing step for idle shares.
3. **Whole lane assigned to one worker** (excluded by spec §1) — a lost worker removes a whole priority class.

## Decision outcome

**Chosen:** Every worker consumes every lane with a weighted split of its own draw. A lane below the 5% minimum share is raised to it and the rest scaled; idle lanes' share goes to busy lanes.

## Consequences

**Positive**
- Simplest enforcement with the fewest store calls
- A lost worker degrades capacity evenly instead of removing a lane

**Negative**
- Weight accuracy depends on even partition spread; skewed spread can exceed the ±10 point tolerance
- Weight is per worker, not exact across the group

**Neutral**
- Per-lane buckets remain a later upgrade if accuracy is not met in the load test

## Links

- Spec: [[../spec.md]]
- SAD: [[../sad.md]] §4
- Related ADR: none
