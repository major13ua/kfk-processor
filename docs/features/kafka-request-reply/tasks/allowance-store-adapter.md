---
id: T4
title: "Implement the Redis-compatible AllowanceStore adapter"
layer: "infra"
deps: ["T1"]
blocks: ["T15"]
acs: ["AC-10", "AC-18"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/adapters/allowance/", "build.gradle"]
owner: "<TBD lead>"
estimate: "L"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T4: Implement the Redis-compatible AllowanceStore adapter

## Place in the sequence

- **Blocked by:** T1 (Define public API types, ports and Idempotency Key) · **Blocks:** T15 (Wire the Spring Boot auto-configuration and startup permission check) · **Wave:** 2, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As an** Operator
> **I want** one Rate Budget that holds across all workers of a group
> **So that** downstream services are not overloaded, including during backlog catch-up
>
> Source: `spec.md §4, US-05, verbatim` · full text: [spec.md](../spec.md)

Provides the cross-worker allowance counter behind the `AllowanceStore` port so one Rate Budget holds across all workers, and signals unreachability so the worker can fail closed.

## Inlined context

> **Chosen:** Shared store behind a port, fail closed. Pause within 5 s of the store becoming unreachable, resume within 30 s of its return (spec §6).
>
> Source: `adr/0003, Decision outcome, verbatim` · full text: [0003-take-rate-budget-from-a-shared-store-and-fail-closed.md](../adr/0003-take-rate-budget-from-a-shared-store-and-fail-closed.md)

> **Chosen:** Reserve before fetching, return unused immediately. Allowance consumed is the units reserved minus the units returned, and a request counts as accepted when it enters the Cycle (the metric counts the same event; same-key requests waiting behind a predecessor are already accepted); Error Replies for malformed or oversized input use no allowance, so their reserved units are returned.
>
> Source: `adr/0006, Decision outcome, verbatim` · full text: [0006-reserve-rate-budget-allowance-before-fetching-and-return-unused.md](../adr/0006-reserve-rate-budget-allowance-before-fetching-and-return-unused.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Rate Budget accuracy | accepted rate ≤ Rate Budget × 1.10 in any sliding 1 s window (provisional) | worker "accepted per second" metric vs configured budget |
> | Pause on limiter outage | new requests stop within 5 s of the store becoming unreachable; resume within 30 s of its return (provisional) | failure-scenario test |
>
> Source: `spec.md §6, NFR table rows Rate Budget accuracy, Pause on limiter outage, verbatim` · full text: [spec.md](../spec.md)

> | Open architectural decision: shared-store client for the Rate Budget (Bucket4j candidate, ADR-0003) | Open question | Confirm library and Redis-compatible store version | Tech Lead | before `sdd:tasks` |
>
> Source: `sad.md §11, open decision, Bucket4j candidate, verbatim` · full text: [sad.md](../sad.md)

> - Rate Budget store: a shared in-memory store that XME already operates, Redis-compatible for the first adapter, behind a port (see ADR-0003).
>
> Source: `sad.md §2, Technical constraints, verbatim` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- SPI `AllowanceStore.reserve(long units)`: may grant less than asked; throws `AllowanceStoreUnavailableException` when the store is unreachable (fail closed). `giveBack(long units)` returns unused units.
- Runtime fault code (no exception to the Handler): `request_reply.rate_budget_store.unavailable` (AC-18). Property group `allowance-store.*`.

: `contracts/public-api.md §3, 4, abridged` · full text: [public-api.md](../contracts/public-api.md)

## Acceptance criteria

### AC-10: happy path

> **Given** several workers share one Rate Budget and a backlog of requests exists
> **When** the group catches up
> **Then** the total number of requests accepted across all workers stays within the Rate Budget in every sliding one-second window, within the tolerance in §6. A request counts as accepted when it is handed to a Handler, taking one unit of allowance at that moment
>
> Source: `spec.md §5, AC-10, verbatim` · full text: [spec.md](../spec.md)

### AC-18: cross-context

> **Given** the shared store behind the Rate Budget becomes unavailable
> **When** a worker needs allowance to accept requests
> **Then** it stops accepting new requests, stays a member of its group, shows a paused state to the Operator, and resumes automatically when the store returns without exceeding the Rate Budget
>
> Source: `spec.md §5, AC-18, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Confirm the shared-store client and the Redis-compatible store version with the Tech Lead (open decision, sad.md §11); do not pick silently
- [ ] Add the client dependency in `build.gradle` (this task owns it)
- [ ] Implement `AllowanceStore` in `src/main/java/xme/common/kfkprocessor/requestreply/adapters/allowance/`: atomic shared counter, sliding one-second window, `reserve` grants ≤ asked, `giveBack` returns units
- [ ] Map every connection or timeout failure to `AllowanceStoreUnavailableException`; never return a grant when the store did not answer
- [ ] Integration test under `src/test/java/xme/common/kfkprocessor/requestreply/adapters/allowance/` with a Redis-compatible Testcontainers instance: two clients share one budget, over-reserve is capped, container stop raises the exception

## Edge cases

| Case | Behaviour |
|---|---|
| Request exceeds remaining allowance | Grants the remainder, possibly 0; never more than the budget in the window. |
| Store unreachable | Throws `AllowanceStoreUnavailableException`; caller fails closed (T8). |
| Two workers reserve at the same instant | Total granted stays within the Rate Budget (× 1.10 tolerance in any sliding 1 s window). |
| `giveBack` after a failed Cycle | Units return to the shared counter; if the return fails, the unit stays consumed, which only lowers throughput and never exceeds the budget. |

## Definition of Done

- [ ] Integration test: two clients never exceed the budget in a sliding 1 s window beyond × 1.10
- [ ] Integration test: stopping the store raises `AllowanceStoreUnavailableException`
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
