---
id: T13
title: "Implement the Cycle loop that ties intake, execution and commit together"
layer: "app"
deps: ["T7", "T8", "T10", "T12"]
blocks: ["T15"]
acs: ["AC-01"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/engine/CycleLoop.java"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T13: Implement the Cycle loop that ties intake, execution and commit together

## Place in the sequence

- **Blocked by:** T7 (Implement worker pause and stall state machine), T8 (Implement intake: reserve allowance, weighted fetch, return unused, fail closed), T10 (Add per-Request-Key ordering inside a lane), T12 (Add commit retry, destination pause and resume) · **Blocks:** T15 (Wire the Spring Boot auto-configuration and startup permission check) · **Wave:** 4, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As a** Handler Owner
> **I want** to supply only a Handler and configuration
> **So that** I get a working request-reply worker without writing consumption, rate control or commit logic
>
> Source: `spec.md §4, US-01, verbatim` · full text: [spec.md](../spec.md)

Runs the normal request-reply path end to end: gather, run Handlers, commit, record Consistency Lag.

## Inlined context

> `Engine->>Store: reserves allowance for the next Cycle`
> `Engine->>Platform: fetches requests from all lanes by weighted share`
> `Engine->>Handler: runs each request with Idempotency Key (same-key requests of a lane in order)`
> `Engine->>Platform: commits replies and request positions together`
> `Engine->>Engine: records Consistency Lag per lane`
>
> Source: `sad.md §6, flow 1 «normal Cycle», abridged` · full text: [sad.md](../sad.md)

> **Chosen:** One transaction per Cycle. The transaction timeout equals the Commit window. Requesters must read committed replies only (documented requirement, spec §8).
>
> Source: `adr/0002, Decision outcome, verbatim` · full text: [0002-commit-replies-and-positions-in-one-transaction-per-cycle.md](../adr/0002-commit-replies-and-positions-in-one-transaction-per-cycle.md)

> Committed approach: a reusable internal starter where a team supplies only a Handler and configuration. The worker enforces one global Rate Budget before accepting requests, splits it across N Priority Lanes by Priority Weight, runs handlers in parallel under a per-request timeout and a bounded Cycle, answers every accepted request with a reply or an Error Reply, and commits replies together with the request positions so a Requester never sees a duplicate committed reply. Handlers themselves are at-least-once and receive an Idempotency Key. Every Priority Lane is consumed by every worker of the group, its partitions spread across the workers. Requests that share a Request Key run one after another in arrival order, others run in parallel. The worker never retries a failed Handler; the Requester decides whether to retry. Rationale: market research found no library combining cross-worker rate limiting at intake, weighted multi-lane priority, per-request timeouts with error replies and atomic reply commit as a handler-only starter (the closest parallel-processing library is no longer maintained; the framework's own request-reply support covers only the basic reply path), so a custom starter fills a real gap. The adversary review's sharpest failure vector is an undeliverable reply that aborts the shared commit and replays the whole Cycle on every worker, repeating handler side effects; the approach therefore treats undeliverable replies per request, never as a reason to replay. Success is one production service running it plus a second team adopting it by writing only a Handler.
>
> Source: `spec.md §1, Context, abridged` · full text: [spec.md](../spec.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal: no API surface.

## Acceptance criteria

### AC-01: happy path

> **Given** a Handler Owner has supplied a Handler and configured the Priority Lanes, the Rate Budget and the reply destination
> **When** the worker group starts and a Requester sends a request
> **Then** the Requester receives the Handler's reply carrying the same correlation identifier as the request
>
> Source: `spec.md §5, AC-01, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `CycleLoop` in `src/main/java/xme/common/kfkprocessor/requestreply/engine/`: intake (T8) → `HandlerExecutor` with keyed dispatch (T9, T10) → `CycleCommitter` (T11, T12) → `WorkerMetrics.recordLag(lane, ...)` per committed reply
- [ ] Cycle ends at the Cycle deadline at the latest; nothing pending means an idle iteration and no commit
- [ ] Skip intake while paused; update `WorkerState` open-Cycle and last-commit times
- [ ] Unit test under `src/test/java/xme/common/kfkprocessor/requestreply/engine/` with fakes: a request in produces a reply with the same correlation id and Request Key

## Edge cases

| Case | Behaviour |
|---|---|
| Nothing pending | Idle iteration, no empty commit. |
| Paused (limiter, destination, permission) | No intake; open Cycle results go through T12. |
| Shutdown mid-Cycle | Uncommitted transaction abandoned; positions not committed, so the Cycle repeats after restart (AC-05). |

## Definition of Done

- [ ] Unit test: end-to-end fake Cycle returns the Handler reply with the same correlation id
- [ ] Lag recorded once per committed reply
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
