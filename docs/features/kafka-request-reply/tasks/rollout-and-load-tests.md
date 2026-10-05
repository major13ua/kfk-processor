---
id: T17
title: "Add rollout, weight-accuracy and throughput tests"
layer: "tests"
deps: ["T15"]
blocks: []
acs: ["AC-10", "AC-11", "AC-12", "AC-14"]
files_hint: ["src/test/java/xme/common/kfkprocessor/requestreply/performance/"]
owner: "<TBD lead>"
estimate: "L"
context_budget: "M"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T17: Add rollout, weight-accuracy and throughput tests

## Place in the sequence

- **Blocked by:** T15 (Wire the Spring Boot auto-configuration and startup permission check) · **Blocks:** none · **Wave:** 6, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As an** Operator
> **I want** to restart or replace workers one by one without reassigning work across the group
> **So that** deployments do not interrupt consumption
>
> Source: `spec.md §4, US-07, verbatim` · full text: [spec.md](../spec.md)

Verifies zero lane reassignment in a rolling restart, weights and minimum share under load, and the accepted rate against the Rate Budget.

## Inlined context

> - **When:** workers are restarted one by one, each returning with the same identity within 45 s (provisional).
> - **Then:** lane reassignment = 0 for workers that return within the identity window; other workers keep consuming.
> - **How verify:** rollout test plus the group membership change counter.
>
> Source: `sad.md §10, QG-3 Rollout stability, verbatim` · full text: [sad.md](../sad.md)

> - **When:** lanes are busy, a Handler is slow, or commits take long.
> - **Then:** each lane within ±10 percentage points of its weight when all lanes are busy (provisional); Handler timeout default 30 s from dispatch, configurable (provisional); Cycle deadline ≤ 80% of the commit window, checked at startup (provisional); commit window 60 s (provisional); every lane has a weight above 0, startup refuses a weight of 0; stall indicator when work is pending and 60 s pass without a commit (provisional). Consistency Lag p95 target is TBD (spec §8) and is added here when set.
> - **How verify:** per-lane accepted-rate metric under a busy-lanes load test; startup validation tests; timeout counter and cycle-duration metric; stall-indicator metric in a failure-scenario test.
>
> Source: `sad.md §10, QG-4 Predictable Cycle behaviour, verbatim` · full text: [sad.md](../sad.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Aggregate throughput | ≥ 2,000 requests/s per worker group (provisional, "thousands" per interview) | load test in the performance environment |
> | Rate Budget accuracy | accepted rate ≤ Rate Budget × 1.10 in any sliding 1 s window (provisional) | worker "accepted per second" metric vs configured budget |
> | Priority Weight accuracy | each lane within ±10 percentage points of its effective share (weights normalised, minimum share 5%) when all lanes are busy (provisional) | per-lane accepted-rate metric |
> | Lane reassignment during rolling restart | 0 for workers that return within the identity window | rollout test + group membership change counter |
>
> Source: `spec.md §6, NFR table rows Aggregate throughput, Rate Budget accuracy, Priority Weight accuracy, Lane reassignment during rolling restart, verbatim` · full text: [spec.md](../spec.md)

> | Weight accuracy depends on partitions spread evenly across workers (ADR-0004) | Medium | Load test against ±10 percentage points; per-lane buckets as a later upgrade | Tech Lead | before first production release |
>
> Source: `sad.md §11, risk, weight accuracy, verbatim` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal: no API surface.

## Acceptance criteria

### AC-10: happy path

> **Given** several workers share one Rate Budget and a backlog of requests exists
> **When** the group catches up
> **Then** the total number of requests accepted across all workers stays within the Rate Budget in every sliding one-second window, within the tolerance in §6. A request counts as accepted when it is handed to a Handler, taking one unit of allowance at that moment
>
> Source: `spec.md §5, AC-10, verbatim` · full text: [spec.md](../spec.md)

### AC-11: happy path

> **Given** three Priority Lanes with Priority Weights and traffic waiting on all of them
> **When** the group is running at its Rate Budget
> **Then** each lane's share of accepted requests matches its Priority Weight within the tolerance in §6
>
> Source: `spec.md §5, AC-11, verbatim` · full text: [spec.md](../spec.md)

### AC-12: domain invariant

> **Given** a Priority Lane with a Priority Weight and pending requests
> **When** other lanes have heavy traffic
> **Then** that lane still receives at least its minimum share of the Rate Budget (5%, see §8) and its requests keep being served
>
> Source: `spec.md §5, AC-12, verbatim` · full text: [spec.md](../spec.md)

### AC-14: happy path

> **Given** workers are restarted one by one, each returning with the same identity within the allowed window
> **When** the rollout runs
> **Then** the other workers keep consuming without any reassignment of lanes
>
> Source: `spec.md §5, AC-14, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create tests under `src/test/java/xme/common/kfkprocessor/requestreply/performance/`: rolling restart of workers with the same identities within the identity window, group membership change counter must stay 0 for the others
- [ ] Backlog catch-up with several workers: accepted per second never above budget × 1.10 in any sliding 1 s window
- [ ] Three busy lanes: each lane within ±10 points of its effective share; a 1% lane still gets ≥ 5%
- [ ] Throughput run against the ≥ 2,000 requests/s per group target in the performance environment; report the head-of-line delay and the effect of per-key ordering; failing the provisional target is reported, not hidden
- [ ] Mark the slow load run so it is excluded from the default unit build

## Edge cases

| Case | Behaviour |
|---|---|
| Partitions not evenly spread across workers | Weight accuracy can drift (known risk); the test records the observed deviation. |
| Worker returns after the identity window | Reassignment is expected and counted; not part of the zero-reassignment assertion. |
| Provisional numbers change | Targets read from configuration, not literals. |

## Definition of Done

- [ ] Rollout test: 0 reassignments for returning workers
- [ ] Rate and weight accuracy assertions pass within the stated tolerances
- [ ] Throughput result recorded against ≥ 2,000 requests/s
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
