---
id: T12
title: "Add commit retry, destination pause and resume"
layer: "app"
deps: ["T7", "T11"]
blocks: ["T13"]
acs: ["AC-08b", "AC-09"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/engine/CycleCommitter.java", "src/main/java/xme/common/kfkprocessor/requestreply/engine/CommitRetry.java"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T12: Add commit retry, destination pause and resume

## Place in the sequence

- **Blocked by:** T7 (Implement worker pause and stall state machine), T11 (Assemble the Cycle commit and turn undeliverable replies into Error Replies) · **Blocks:** T13 (Implement the Cycle loop that ties intake, execution and commit together) · **Wave:** 3, after its dependencies
- **Lane:** shares `src/main/java/xme/common/kfkprocessor/requestreply/engine/CycleCommitter.java` with T11: serialized.

## Why (user story)

> **As a** Requester
> **I want** a reply that carries my correlation identifier, and never two committed replies for one request
> **So that** I can match and trust the answer
>
> Source: `spec.md §4, US-03, verbatim` · full text: [spec.md](../spec.md)

Keeps replies safe when the commit or the destination fails: retries the same results, pauses without leaving the group, and resumes without re-running Handlers.

## Inlined context

> **Chosen:** Failures specific to one reply (too large, cannot be encoded, rejected on send) become Error Replies in the same transaction. If the commit itself fails, it is retried with the same results, 3 attempts by default (configurable). When attempts run out, or the Error Reply cannot be delivered (destination unavailable), the worker pauses, alerts and stays in its group. A restart after that re-runs Handlers, which is the at-least-once case.
>
> Source: `adr/0007, Decision outcome, verbatim` · full text: [0007-turn-send-failures-into-error-replies-and-retry-commits-without-rerunning-handlers.md](../adr/0007-turn-send-failures-into-error-replies-and-retry-commits-without-rerunning-handlers.md)

> `Engine->>Platform: retries the same results (up to 3 attempts, no Handler re-run)`
> `Engine->>Operator: pauses, shows paused state, alerts, stays in group`
> `Engine->>Platform: probes the destination`
> `Engine->>Platform: re-commits the held results in a new transaction, Handlers not re-run`
>
> Source: `sad.md §6, flow 2 «failures inside a Cycle», abridged` · full text: [sad.md](../sad.md)

> `alt permission missing, at start or when a Cycle sends replies`
> `Engine-->>Operator: reports configuration fault, accepts no requests, stays in group`
> `Engine->>Platform: probes write permission`
> `Engine->>Engine: resumes intake, a Cycle in progress is committed without re-running Handlers`
>
> Source: `sad.md §6, flow 4 «permission handling», abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- Runtime faults (state and metrics, no exception to the Handler): `request_reply.reply_destination.permission_denied` (AC-09), `request_reply.reply_destination.unavailable` (AC-08b). Property `commit-retry-attempts` (default 3).

: `contracts/public-api.md §3, abridged` · full text: [public-api.md](../contracts/public-api.md)

## Acceptance criteria

### AC-08b: error

> **Given** an Error Reply itself cannot be delivered because the reply destination is unavailable
> **When** the worker tries to commit the Cycle
> **Then** the worker pauses as in AC-09, alerts the Operator, stays in its group, does not run the Handlers of that Cycle again, and resumes automatically when the destination returns
>
> Source: `spec.md §5, AC-08b, verbatim` · full text: [spec.md](../spec.md)

### AC-09: authorization

> **Given** the worker has no permission to write to the reply destination
> **When** it starts or a Cycle tries to send replies
> **Then** the worker stops accepting requests, reports a configuration fault to the Operator, loses no request, stays in its group (no lane reassignment) and resumes automatically once the permission is restored, without running the Handlers of a Cycle in progress again
>
> Source: `spec.md §5, AC-09, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `CommitRetry` in `src/main/java/xme/common/kfkprocessor/requestreply/engine/`; extend `CycleCommitter` (T11, same file, serialized lane)
- [ ] On commit failure retry the same results up to `commit-retry-attempts`; no Handler re-run
- [ ] When attempts run out or the sink reports destination unavailable or permission denied: set paused(destination or permission) in `WorkerState` (T7), alert, stay in the group, hold results in memory
- [ ] Probe the destination; on success re-commit the held results in a new transaction
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/engine/` with a fake sink: transient failure, exhausted retries, destination returns, permission restored

## Edge cases

| Case | Behaviour |
|---|---|
| Commit fails once | Retried with the same results; replies not duplicated. |
| Attempts exhausted | Paused, alert, still a group member, results held. |
| Permission denied at start or mid-Cycle | Configuration fault reported, no requests accepted, none lost, resumes automatically when restored. |
| Worker crashes while results are held | Handlers run again after restart: at-least-once, covered by the Idempotency Key (documented, T18). |

## Definition of Done

- [ ] Unit tests: no Handler invocation during retry, pause and resume
- [ ] Paused state is visible and the worker is never removed from its group in the tests
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
