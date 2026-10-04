---
id: T16
title: "Add the failure-scenario test suite"
layer: "tests"
deps: ["T15"]
blocks: []
acs: ["AC-03", "AC-05", "AC-08b", "AC-18", "AC-19"]
files_hint: ["src/test/java/xme/common/kfkprocessor/requestreply/failure/"]
owner: "<TBD lead>"
estimate: "L"
context_budget: "M"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T16: Add the failure-scenario test suite

## Place in the sequence

- **Blocked by:** T15 (Wire the Spring Boot auto-configuration and startup permission check) · **Blocks:** none · **Wave:** 6, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As a** Requester
> **I want** a reply that carries my correlation identifier, and never two committed replies for one request
> **So that** I can match and trust the answer
>
> — `spec.md §4, US-03, verbatim` · full text: [spec.md](../spec.md)

Proves 0 lost or duplicated committed replies under kill, failed commit, unavailable destination and limiter outage, which is the accepted success measure.

## Inlined context

> - **When:** a worker is killed mid-Cycle, a commit fails, or a reply is too large or cannot be encoded.
> - **Then:** lost or duplicated committed replies = 0; Handlers are not re-run because of a send failure.
> - **How verify:** failure-scenario test suite (kill mid-Cycle, failed commit, oversized reply, unavailable destination) reading committed replies only; production reconciliation of requests against replies.
>
> — `sad.md §10, QG-2 Reply integrity, verbatim` · full text: [sad.md](../sad.md)

> - **When:** several workers share one Rate Budget and a backlog exists, or the Rate Budget store becomes unreachable.
> - **Then:** accepted rate ≤ Rate Budget × 1.10 in any sliding 1 s window (provisional); new requests stop within 5 s of the store becoming unreachable and resume within 30 s of its return (provisional); aggregate throughput ≥ 2,000 requests/s per worker group (provisional).
> - **How verify:** load test in the performance environment against the worker "accepted per second" metric (counted at Cycle intake, the same event the limiter counts) vs the configured budget; failure-scenario test that cuts the store.
>
> — `sad.md §10, QG-1 Downstream protection, verbatim` · full text: [sad.md](../sad.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Lost or duplicated committed replies | 0 | failure-scenario test suite |
> | Pause on limiter outage | new requests stop within 5 s of the store becoming unreachable; resume within 30 s of its return (provisional) | failure-scenario test |
>
> — `spec.md §6, NFR table rows Lost or duplicated committed replies, Pause on limiter outage, verbatim` · full text: [spec.md](../spec.md)

> `Note over Engine: worker killed before commit`
> `Engine->>Handler: runs R with the same Idempotency Key K`
>
> — `sad.md §6, flow 5 «killed mid-Cycle, restart with the same identity», abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal — no API surface.

## Acceptance criteria

### AC-03: happy path

> **Given** a request is delivered to a Handler
> **When** the same request is executed again after a failure or restart
> **Then** the Handler receives the same Idempotency Key as the first time
>
> — `spec.md §5, AC-03, verbatim` · full text: [spec.md](../spec.md)

### AC-05: domain invariant

> **Given** a request attempt whose reply was already committed
> **When** a Cycle is repeated after a failure or restart
> **Then** a Requester reading committed replies sees only one reply for that request attempt (a new attempt by the Requester is a new request)
>
> — `spec.md §5, AC-05, verbatim` · full text: [spec.md](../spec.md)

### AC-08b: error

> **Given** an Error Reply itself cannot be delivered because the reply destination is unavailable
> **When** the worker tries to commit the Cycle
> **Then** the worker pauses as in AC-09, alerts the Operator, stays in its group, does not run the Handlers of that Cycle again, and resumes automatically when the destination returns
>
> — `spec.md §5, AC-08b, verbatim` · full text: [spec.md](../spec.md)

### AC-18: cross-context

> **Given** the shared store behind the Rate Budget becomes unavailable
> **When** a worker needs allowance to accept requests
> **Then** it stops accepting new requests, stays a member of its group, shows a paused state to the Operator, and resumes automatically when the store returns without exceeding the Rate Budget
>
> — `spec.md §5, AC-18, verbatim` · full text: [spec.md](../spec.md)

### AC-19: cross-context

> **Given** a downstream service is degraded and its calls are slow or failing
> **When** Handlers hit their per-request timeout
> **Then** the Requesters receive Error Replies and the accepted rate stays within the Rate Budget, so the degraded service is not hit harder by a retry storm from the worker
>
> — `spec.md §5, AC-19, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create scenarios under `src/test/java/xme/common/kfkprocessor/requestreply/failure/` using the existing `TestcontainersConfiguration` plus a Redis-compatible container; no new build dependency expected, add one in `build.gradle` only if unavoidable and say so in the PR
- [ ] Kill a worker mid-Cycle, restart with the same identity: Handler sees the same Idempotency Key, committed-only reader sees one reply
- [ ] Failed commit then success: Handler invocation count unchanged, one committed reply
- [ ] Reply destination unavailable: worker pauses, stays in group, resumes, re-commits without Handler re-run
- [ ] Rate Budget store cut: new requests stop within 5 s, resume within 30 s of return, accepted rate stays within budget
- [ ] Degraded downstream: Handlers time out, Requesters get Error Replies, accepted rate stays within budget

## Edge cases

| Case | Behaviour |
|---|---|
| Reader not set to committed-only | Test reader is configured committed-only; documented requirement (T18). |
| Timing assertions | Use the provisional numbers from config, not hard-coded literals, so a changed NFR does not silently pass. |
| Kill during commit | Either fully committed or fully invisible; never one reply of two. |

## Definition of Done

- [ ] All five scenarios pass against real Kafka and a Redis-compatible container
- [ ] Zero lost and zero duplicated committed replies asserted by reconciliation of requests against replies
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
