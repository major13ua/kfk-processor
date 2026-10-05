---
id: T8
title: "Implement intake: reserve allowance, weighted fetch, return unused, fail closed"
layer: "app"
deps: ["T1", "T2", "T7"]
blocks: ["T13"]
acs: ["AC-10", "AC-10b", "AC-18"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/engine/Intake.java"]
owner: "<TBD lead>"
estimate: "L"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T8: Implement intake: reserve allowance, weighted fetch, return unused, fail closed

## Place in the sequence

- **Blocked by:** T1 (Define public API types, ports and Idempotency Key), T2 (Implement lane share calculator (weights, 5% minimum, idle redistribution)), T7 (Implement worker pause and stall state machine) · **Blocks:** T13 (Implement the Cycle loop that ties intake, execution and commit together) · **Wave:** 3, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As an** Operator
> **I want** one Rate Budget that holds across all workers of a group
> **So that** downstream services are not overloaded, including during backlog catch-up
>
> Source: `spec.md §4, US-05, verbatim` · full text: [spec.md](../spec.md)

Takes allowance before fetching, splits it across lanes, returns what was not used, answers malformed input without spending allowance and pauses when the store is down.

## Inlined context

> **Chosen:** Reserve before fetching, return unused immediately. Allowance consumed is the units reserved minus the units returned, and a request counts as accepted when it enters the Cycle (the metric counts the same event; same-key requests waiting behind a predecessor are already accepted); Error Replies for malformed or oversized input use no allowance, so their reserved units are returned.
>
> Source: `adr/0006, Decision outcome, verbatim` · full text: [0006-reserve-rate-budget-allowance-before-fetching-and-return-unused.md](../adr/0006-reserve-rate-budget-allowance-before-fetching-and-return-unused.md)

> **Chosen:** Shared store behind a port, fail closed. Pause within 5 s of the store becoming unreachable, resume within 30 s of its return (spec §6).
>
> Source: `adr/0003, Decision outcome, verbatim` · full text: [0003-take-rate-budget-from-a-shared-store-and-fail-closed.md](../adr/0003-take-rate-budget-from-a-shared-store-and-fail-closed.md)

> `Engine->>Store: reserves allowance for the next Cycle`
> `Engine->>Engine: fetches nothing, waits for the next window, requests stay unconsumed with no reply`
> `Engine->>Platform: fetches from each lane up to its share`
> `Engine->>Engine: answers with an Error Reply at once, uses no allowance`
> `Engine->>Store: returns unused allowance`
>
> Source: `sad.md §6, flow 7 «allowance split», abridged` · full text: [sad.md](../sad.md)

> `Store--xEngine: unreachable`
> `Engine->>Engine: stops accepting new requests within 5 s, keeps group membership`
> `Engine->>Store: probes for allowance`
> `Engine->>Engine: resumes within 30 s without exceeding the Rate Budget`
>
> Source: `sad.md §6, flow 3 «limiter outage and recovery», abridged` · full text: [sad.md](../sad.md)

> Decision override: allowance is counted at Cycle intake: rationale: allowance is reserved before fetching and returned when unused (ADR-0006), so "accepted" means the unit stays consumed when the request enters the Cycle; spec AC-10 words this as "handed to a Handler", which can be seconds later for same-key requests. The limiter and the "accepted per second" metric count the same event.
>
> Source: `sad.md §1, Decision override, verbatim` · full text: [sad.md](../sad.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Rate Budget accuracy | accepted rate ≤ Rate Budget × 1.10 in any sliding 1 s window (provisional) | worker "accepted per second" metric vs configured budget |
> | Pause on limiter outage | new requests stop within 5 s of the store becoming unreachable; resume within 30 s of its return (provisional) | failure-scenario test |
>
> Source: `spec.md §6, NFR table rows Rate Budget accuracy, Pause on limiter outage, verbatim` · full text: [spec.md](../spec.md)

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

### AC-10b: domain invariant

> **Given** more requests are waiting than the Rate Budget allows
> **When** the worker has no allowance left
> **Then** the remaining requests stay unconsumed, get no reply and wait for a later Cycle; requests answered at once with an Error Reply because they are malformed or oversized use no allowance
>
> Source: `spec.md §5, AC-10b, verbatim` · full text: [spec.md](../spec.md)

### AC-18: cross-context

> **Given** the shared store behind the Rate Budget becomes unavailable
> **When** a worker needs allowance to accept requests
> **Then** it stops accepting new requests, stays a member of its group, shows a paused state to the Operator, and resumes automatically when the store returns without exceeding the Rate Budget
>
> Source: `spec.md §5, AC-18, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `Intake` in `src/main/java/xme/common/kfkprocessor/requestreply/engine/` using ports `AllowanceStore`, `RequestLanes` and `LaneShares` (T2); no adapter classes imported
- [ ] Reserve before fetching; split the grant by lane share; fetch each lane up to its units; give unused units back at once
- [ ] Malformed or oversized requests become an immediate Error Reply (failure) and their units are returned
- [ ] On `AllowanceStoreUnavailableException`: set paused(limiter) in `WorkerState` (T7) within 5 s, fetch nothing, keep group membership, probe until the store answers, resume within 30 s
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/engine/` with fakes: no allowance, partial grant, idle lane, malformed input, store down and back

## Edge cases

| Case | Behaviour |
|---|---|
| No allowance left | Nothing fetched; remaining requests stay unconsumed and get no reply (AC-10b). |
| Malformed or oversized request | Error Reply at once, no allowance used, units returned. |
| Store unreachable | Intake stops, paused state shown, worker stays in its group, no request lost. |
| Store returns | Intake resumes automatically without exceeding the budget; first reserve is capped by the store. |
| Same-key request waiting behind a predecessor | Already counted as accepted at intake (§1 override); the metric counts the same event. |

## Definition of Done

- [ ] Unit tests assert fetched count ≤ granted, unused units returned, malformed input spends nothing
- [ ] Unit test: store failure pauses intake and recovery resumes it
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
