---
id: T9
title: "Implement Handler execution: virtual threads, timeout, cancellation, Error Replies"
layer: "app"
deps: ["T1"]
blocks: ["T10"]
acs: ["AC-06", "AC-07", "AC-07b", "AC-13", "AC-19"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/engine/HandlerExecutor.java"]
owner: "<TBD lead>"
estimate: "L"
context_budget: "M"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T9: Implement Handler execution: virtual threads, timeout, cancellation, Error Replies

## Place in the sequence

- **Blocked by:** T1 (Define public API types, ports and Idempotency Key) · **Blocks:** T10 (Add per-Request-Key ordering inside a lane) · **Wave:** 2, after its dependencies
- **Lane:** shares `src/main/java/xme/common/kfkprocessor/requestreply/engine/HandlerExecutor.java` with T10: serialized.

## Why (user story)

> **As a** Requester
> **I want** an Error Reply when my request fails or times out
> **So that** I am not left waiting and can decide to retry
>
> — `spec.md §4, US-04, verbatim` · full text: [spec.md](../spec.md)

Runs each Handler call on a virtual thread under a per-request timeout and the Cycle deadline, and turns every failure or timeout into one Error Reply without re-running the Handler.

## Inlined context

> `Engine->>Handler: dispatches A, B and C in parallel, D queued behind C`
> `Engine->>Engine: builds Error Reply for A (failure), A is never re-run`
> `Engine->>Handler: signals B to cancel (cooperative)`
> `Engine->>Engine: builds Error Reply for B (timeout), no request waited past the Cycle deadline`
>
> — `sad.md §6, flow 6 «failed, slow and ordered requests», abridged` · full text: [sad.md](../sad.md)

> `Handler-->>Engine: A times out, cancelled cooperatively`
>
> — `sad.md §6, flow 2 «failures inside a Cycle», abridged` · full text: [sad.md](../sad.md)

> 5. **Handlers on virtual threads.** Each request runs on its own virtual thread; the timeout starts at dispatch, cancellation is cooperative, requests sharing a Request Key run in arrival order within a lane, and every failure becomes an Error Reply. Ordering across lanes is an open question (§11). Execution detail inside one module (no ADR: reversible).
>
> — `sad.md §4, strategic choice 5, verbatim` · full text: [sad.md](../sad.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Per-request Handler timeout | default 30 s from dispatch, configurable (provisional); implies a commit window above 37.5 s | startup validation + timeout counter |
> | Cycle deadline | ≤ 80% of the commit window, checked at startup (provisional) | startup validation + cycle-duration metric |
>
> — `spec.md §6, NFR table rows Per-request Handler timeout, Cycle deadline, verbatim` · full text: [spec.md](../spec.md)

> - **Personal data touched:** none added by the starter. Payloads may contain personal data; the starter must not copy payload content into logs, metrics or Error Replies.
>
> — `spec.md §6.1, Personal data touched, verbatim` · full text: [spec.md](../spec.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- Handler contract: throwing any exception becomes an Error Reply (failure); the worker never re-runs a failed or timed-out Handler; cancellation is cooperative via `CancellationSignal`.

— `contracts/public-api.md §1, abridged` · full text: [public-api.md](../contracts/public-api.md)

## Acceptance criteria

### AC-06: error

> **Given** a Handler fails on one request in a Cycle
> **When** the Cycle completes
> **Then** the Requester of that request receives an Error Reply naming the failure category, and the other requests in the same Cycle get their normal replies
>
> — `spec.md §5, AC-06, verbatim` · full text: [spec.md](../spec.md)

### AC-07: error

> **Given** a Handler does not finish within the per-request timeout
> **When** the timeout elapses (the timer starts when the Handler is dispatched)
> **Then** the Requester receives an Error Reply for a timeout, the Handler is signalled to cancel (cooperative: a Handler may still finish its side effects, which the starter guide documents next to the Idempotency Key note), and the Cycle is not delayed beyond its deadline
>
> — `spec.md §5, AC-07, verbatim` · full text: [spec.md](../spec.md)

### AC-07b: error

> **Given** a Handler fails or times out
> **When** the worker handles the failure
> **Then** the Handler is not run again by the worker, and the Requester receives the Error Reply from the first failure
>
> — `spec.md §5, AC-07b, verbatim` · full text: [spec.md](../spec.md)

### AC-13: domain invariant

> **Given** a slow Handler on a low-weight lane
> **When** a high-weight lane request is in the same Cycle
> **Then** no request in the Cycle waits longer than the Cycle deadline, and unfinished requests receive Error Replies
>
> — `spec.md §5, AC-13, verbatim` · full text: [spec.md](../spec.md)

### AC-19: cross-context

> **Given** a downstream service is degraded and its calls are slow or failing
> **When** Handlers hit their per-request timeout
> **Then** the Requesters receive Error Replies and the accepted rate stays within the Rate Budget, so the degraded service is not hit harder by a retry storm from the worker
>
> — `spec.md §5, AC-19, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `HandlerExecutor` in `src/main/java/xme/common/kfkprocessor/requestreply/engine/`: one virtual thread per request, timer starts at dispatch
- [ ] On per-request timeout set the `CancellationSignal` and produce an Error Reply (timeout); on Cycle deadline do the same for every unfinished request
- [ ] Handler exception becomes an Error Reply (failure) with category only; no exception text, stack trace or payload
- [ ] A request gets exactly one result; a late Handler completion after timeout is discarded
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/engine/`: failing, slow, cancel-aware, cancel-ignoring and deadline cases; Handler invocation count stays 1

## Edge cases

| Case | Behaviour |
|---|---|
| Handler throws | Error Reply (failure) for that request; others in the Cycle get normal replies. |
| Handler exceeds the per-request timeout | Error Reply (timeout), cancellation signalled, Handler may still finish side effects (documented, T18). |
| Slow low-weight request in the same Cycle as a high-weight one | No request waits beyond the Cycle deadline; unfinished ones get Error Replies (AC-13). |
| Degraded downstream | Error Replies only; no worker-side retry, so no retry storm (AC-19). |
| Handler finishes after its timeout | Late result ignored; the Error Reply from the first outcome stands (AC-07b). |

## Definition of Done

- [ ] Unit tests assert one Handler invocation per request across failure and timeout
- [ ] Cycle never exceeds its deadline in the slow-Handler test
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
