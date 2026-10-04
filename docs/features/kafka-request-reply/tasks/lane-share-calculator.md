---
id: T2
title: "Implement lane share calculator (weights, 5% minimum, idle redistribution)"
layer: "domain"
deps: []
blocks: ["T3", "T8"]
acs: ["AC-11", "AC-12"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/engine/LaneShares.java"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T2: Implement lane share calculator (weights, 5% minimum, idle redistribution)

## Place in the sequence

- **Blocked by:** none · **Blocks:** T3 (Add configuration properties and startup validation), T8 (Implement intake: reserve allowance, weighted fetch, return unused, fail closed) · **Wave:** 1, no dependencies, starts immediately
- **Lane:** own lane.

## Why (user story)

> **As an** Operator
> **I want** to give each Priority Lane a Priority Weight
> **So that** important traffic gets a larger share without starving the others
>
> — `spec.md §4, US-06, verbatim` · full text: [spec.md](../spec.md)

Computes the effective share of each Priority Lane and splits an allowance draw by it, which is what makes weights and the minimum share real.

## Inlined context

> **Chosen:** Every worker consumes every lane with a weighted split of its own draw. A lane below the 5% minimum share is raised to it and the rest scaled; idle lanes' share goes to busy lanes.
>
> — `adr/0004, Decision outcome, verbatim` · full text: [0004-consume-every-lane-in-every-worker-and-split-each-draw-by-weight.md](../adr/0004-consume-every-lane-in-every-worker-and-split-each-draw-by-weight.md)

> `Engine->>Engine: splits the draw by Priority Weight, 5% minimum per lane`
> `Engine->>Engine: hands its unused share to busy lanes`
>
> — `sad.md §6, flow 7 «allowance split across lanes», abridged` · full text: [sad.md](../sad.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Priority Weight accuracy | each lane within ±10 percentage points of its effective share (weights normalised, minimum share 5%) when all lanes are busy (provisional) | per-lane accepted-rate metric |
> | Priority Weight validity | every lane has a weight above 0; startup refuses a weight of 0 | startup validation |
>
> — `spec.md §6, NFR table rows Priority Weight accuracy, Priority Weight validity, verbatim` · full text: [spec.md](../spec.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal — no API surface.

## Acceptance criteria

### AC-11: happy path

> **Given** three Priority Lanes with Priority Weights and traffic waiting on all of them
> **When** the group is running at its Rate Budget
> **Then** each lane's share of accepted requests matches its Priority Weight within the tolerance in §6
>
> — `spec.md §5, AC-11, verbatim` · full text: [spec.md](../spec.md)

### AC-12: domain invariant

> **Given** a Priority Lane with a Priority Weight and pending requests
> **When** other lanes have heavy traffic
> **Then** that lane still receives at least its minimum share of the Rate Budget (5%, see §8) and its requests keep being served
>
> — `spec.md §5, AC-12, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Pure class `LaneShares` in `src/main/java/xme/common/kfkprocessor/requestreply/engine/`: input lane weights and `min-lane-share` (default 5%), output effective shares summing to 100%
- [ ] Raise any lane below the minimum to it and scale the others down proportionally
- [ ] Split a draw of N units across busy lanes by effective share; idle lanes hand their share to busy lanes (re-normalised); the sum of lane units equals N
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/engine/`: weights 5/3/2, a 1% lane raised to 5%, one idle lane, all idle, single lane, rounding

## Edge cases

| Case | Behaviour |
|---|---|
| Lane share below the 5% minimum | Raised to the minimum, other lanes scaled down (AC-02 valid-configuration clause). |
| A lane has no pending requests | Its share goes to busy lanes (spec §8 default: idle shares are redistributed). |
| Weight 0 or negative | Rejected as invalid input; startup refusal is T3, this class must not divide by zero. |
| Draw not divisible by shares | Units are rounded so the lane totals equal the draw exactly; no unit is lost or created. |

## Definition of Done

- [ ] Unit tests cover shares, minimum share, idle redistribution and exact unit sum
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
