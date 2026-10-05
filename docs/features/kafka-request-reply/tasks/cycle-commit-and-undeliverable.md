---
id: T11
title: "Assemble the Cycle commit and turn undeliverable replies into Error Replies"
layer: "app"
deps: ["T1"]
blocks: ["T12"]
acs: ["AC-08"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/engine/CycleCommitter.java"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T11: Assemble the Cycle commit and turn undeliverable replies into Error Replies

## Place in the sequence

- **Blocked by:** T1 (Define public API types, ports and Idempotency Key) · **Blocks:** T12 (Add commit retry, destination pause and resume) · **Wave:** 2, after its dependencies
- **Lane:** shares `src/main/java/xme/common/kfkprocessor/requestreply/engine/CycleCommitter.java` with T12: serialized.

## Why (user story)

> **As a** Requester
> **I want** an Error Reply when my request fails or times out
> **So that** I am not left waiting and can decide to retry
>
> Source: `spec.md §4, US-04, verbatim` · full text: [spec.md](../spec.md)

Builds the single commit of replies, Error Replies and positions, and converts a reply that cannot be delivered into an Error Reply without re-running Handlers.

## Inlined context

> **Chosen:** One transaction per Cycle. The transaction timeout equals the Commit window. Requesters must read committed replies only (documented requirement, spec §8).
>
> Source: `adr/0002, Decision outcome, verbatim` · full text: [0002-commit-replies-and-positions-in-one-transaction-per-cycle.md](../adr/0002-commit-replies-and-positions-in-one-transaction-per-cycle.md)

> **Chosen:** Failures specific to one reply (too large, cannot be encoded, rejected on send) become Error Replies in the same transaction. If the commit itself fails, it is retried with the same results, 3 attempts by default (configurable). When attempts run out, or the Error Reply cannot be delivered (destination unavailable), the worker pauses, alerts and stays in its group. A restart after that re-runs Handlers, which is the at-least-once case.
>
> Source: `adr/0007, Decision outcome, verbatim` · full text: [0007-turn-send-failures-into-error-replies-and-retry-commits-without-rerunning-handlers.md](../adr/0007-turn-send-failures-into-error-replies-and-retry-commits-without-rerunning-handlers.md)

> `Handler-->>Engine: B replies with a result that cannot be encoded`
> `Engine->>Engine: builds Error Reply for A (timeout) and B (undeliverable)`
> `Engine->>Platform: commits replies and positions`
>
> Source: `sad.md §6, flow 2 «failures inside a Cycle», abridged` · full text: [sad.md](../sad.md)

> - **Personal data touched:** none added by the starter. Payloads may contain personal data; the starter must not copy payload content into logs, metrics or Error Replies.
>
> Source: `spec.md §6.1, Personal data touched, verbatim` · full text: [spec.md](../spec.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- `error_reply.v1` category `undeliverable`: reply too large, cannot be encoded or rejected on send (AC-08). No message, stack trace or payload.

: `contracts/events.md, request_reply.error_reply.v1, abridged` · full text: [events.md](../contracts/events.md)

## Acceptance criteria

### AC-08: domain invariant

> **Given** a reply cannot be delivered for any reason specific to it (too large, cannot be encoded, rejected on send)
> **When** the worker tries to commit the Cycle
> **Then** the Requester gets an Error Reply instead, the Handlers of that Cycle are not run again, and the other requests in it are unaffected
>
> Source: `spec.md §5, AC-08, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `CycleCommitter` in `src/main/java/xme/common/kfkprocessor/requestreply/engine/` using the `ReplySink` port (T1): collect results, call `commit(replies, positions)` once per Cycle
- [ ] When the sink reports a per-request failure, replace that reply with an Error Reply (undeliverable) in the same transaction; keep correlation id and Request Key
- [ ] Never call the Handler again because of a send failure
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/engine/` with a fake sink: oversized, unencodable, rejected, all fine

## Edge cases

| Case | Behaviour |
|---|---|
| One reply too large, others fine | That request gets an Error Reply (undeliverable); others commit unchanged. |
| Error Reply itself cannot be sent | Escalated to T12 (pause, hold results); not handled here. |
| Handlers of the Cycle | Not run again; results are held in memory for the retry. |

## Definition of Done

- [ ] Unit test: undeliverable reply becomes an Error Reply and Handler invocation count stays 1
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
