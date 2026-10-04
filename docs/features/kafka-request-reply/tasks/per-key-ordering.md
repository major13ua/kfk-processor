---
id: T10
title: "Add per-Request-Key ordering inside a lane"
layer: "app"
deps: ["T9"]
blocks: ["T13"]
acs: ["AC-07c"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/engine/HandlerExecutor.java", "src/main/java/xme/common/kfkprocessor/requestreply/engine/KeyedDispatcher.java"]
owner: "<TBD lead>"
estimate: "S"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T10: Add per-Request-Key ordering inside a lane

## Place in the sequence

- **Blocked by:** T9 (Implement Handler execution: virtual threads, timeout, cancellation, Error Replies) · **Blocks:** T13 (Implement the Cycle loop that ties intake, execution and commit together) · **Wave:** 3, after its dependencies
- **Lane:** shares `src/main/java/xme/common/kfkprocessor/requestreply/engine/HandlerExecutor.java` with T9: serialized.

## Why (user story)

> **As a** Handler Owner
> **I want** to supply only a Handler and configuration
> **So that** I get a working request-reply worker without writing consumption, rate control or commit logic
>
> — `spec.md §4, US-01, verbatim` · full text: [spec.md](../spec.md)

Runs requests that share a Request Key one after another while different keys keep running in parallel.

## Inlined context

> `Engine->>Handler: dispatches A, B and C in parallel, D queued behind C`
> `Engine->>Handler: dispatches D after C finished (arrival order per key)`
>
> — `sad.md §6, flow 6 «failed, slow and ordered requests», abridged` · full text: [sad.md](../sad.md)

> | Ordering | Same-key requests run in order within a lane; Requesters must send same-key requests to one lane (see §11) | here |
>
> — `sad.md §8, Ordering, verbatim` · full text: [sad.md](../sad.md)

> | Open architectural decision: cross-lane ordering of same-key requests | Open question | Guarantee is order per key within a lane; Requesters must send same-key requests to one lane (cross-lane order is not guaranteed: a low-share lane can deliver an older request in a later Cycle); tighten spec AC-07c ("arrival order") to "within a lane" and state the rule in the starter guide | Tech Lead | before `sdd:tasks` |
>
> — `sad.md §11, open decision, cross-lane ordering, verbatim` · full text: [sad.md](../sad.md)

> | Open architectural decision: per-key ordering may cut parallelism against the 2,000 requests/s target (spec §8) | Open question | Measure with the pilot service's real key distribution | Tech Lead | before `sdd:tasks` |
>
> — `sad.md §11, open decision, parallelism vs 2,000 requests/s, verbatim` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- Same `requestKey` in one lane runs sequentially in arrival order; different keys in parallel on virtual threads. Order across lanes is not guaranteed.

— `contracts/public-api.md §1, abridged` · full text: [public-api.md](../contracts/public-api.md)

## Acceptance criteria

### AC-07c: domain invariant

> **Given** several requests with the same Request Key are waiting in a Cycle
> **When** the Handlers run
> **Then** those requests run one after another in arrival order, while requests with different Request Keys run in parallel
>
> — `spec.md §5, AC-07c, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `KeyedDispatcher` in `src/main/java/xme/common/kfkprocessor/requestreply/engine/`: per-key FIFO inside a lane, parallel across keys
- [ ] Hook it into `HandlerExecutor` (T9, same file, serialized lane)
- [ ] The per-request timeout of a queued request starts when it is dispatched, not when it arrived (AC-07); the Cycle deadline still bounds it
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/engine/`: same key ordered, different keys parallel, queued request cut by the deadline

## Edge cases

| Case | Behaviour |
|---|---|
| Several requests with one Request Key in one lane | Run strictly in arrival order, one at a time. |
| Same Request Key on two lanes | No order guaranteed across lanes (documented rule: send same-key requests to one lane; tightening AC-07c to «within a lane» is an open item for the Tech Lead). |
| Predecessor consumes the Cycle deadline | Queued request gets an Error Reply (timeout) without being dispatched. |
| Predecessor fails | Next request with the same key still runs. |

## Definition of Done

- [ ] Unit tests prove ordering per key and parallelism across keys
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
