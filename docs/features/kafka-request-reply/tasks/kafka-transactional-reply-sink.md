---
id: T6
title: "Implement the transactional Kafka ReplySink"
layer: "infra"
deps: ["T1"]
blocks: ["T15"]
acs: ["AC-05"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/adapters/kafka/KafkaReplySink.java"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T6: Implement the transactional Kafka ReplySink

## Place in the sequence

- **Blocked by:** T1 (Define public API types, ports and Idempotency Key) · **Blocks:** T15 (Wire the Spring Boot auto-configuration and startup permission check) · **Wave:** 2, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As a** Requester
> **I want** a reply that carries my correlation identifier, and never two committed replies for one request
> **So that** I can match and trust the answer
>
> — `spec.md §4, US-03, verbatim` · full text: [spec.md](../spec.md)

Writes replies and request positions in one transaction so a repeated Cycle can never add a second committed reply.

## Inlined context

> **Chosen:** One transaction per Cycle. The transaction timeout equals the Commit window. Requesters must read committed replies only (documented requirement, spec §8).
>
> — `adr/0002, Decision outcome, verbatim` · full text: [0002-commit-replies-and-positions-in-one-transaction-per-cycle.md](../adr/0002-commit-replies-and-positions-in-one-transaction-per-cycle.md)

> **Chosen:** Explicit identity required; absent identity refuses start (AC-15). The same identity is used for group membership and the transaction.
>
> — `adr/0005, Decision outcome, verbatim` · full text: [0005-require-a-stable-worker-identity.md](../adr/0005-require-a-stable-worker-identity.md)

> **Chosen:** Failures specific to one reply (too large, cannot be encoded, rejected on send) become Error Replies in the same transaction. If the commit itself fails, it is retried with the same results, 3 attempts by default (configurable). When attempts run out, or the Error Reply cannot be delivered (destination unavailable), the worker pauses, alerts and stays in its group. A restart after that re-runs Handlers, which is the at-least-once case.
>
> — `adr/0007, Decision outcome, verbatim` · full text: [0007-turn-send-failures-into-error-replies-and-retry-commits-without-rerunning-handlers.md](../adr/0007-turn-send-failures-into-error-replies-and-retry-commits-without-rerunning-handlers.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Commit window | 60 s (provisional) | startup validation |
> | Lost or duplicated committed replies | 0 | failure-scenario test suite |
>
> — `spec.md §6, NFR table rows Commit window, Lost or duplicated committed replies, verbatim` · full text: [spec.md](../spec.md)

> `Engine->>Platform: commits reply and request position together`
> `Platform-->>Requester: exactly one committed reply for this attempt`
>
> — `sad.md §6, flow 5 «killed mid-Cycle, restart with the same identity», abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- Channel reply destination: producer worker, only inside the Cycle transaction together with the request positions. Consumers read committed records only.
- Events `request_reply.reply.v1` and `request_reply.error_reply.v1` as in T1. Faults: `request_reply.reply_destination.permission_denied`, `.unavailable`.

— `contracts/events.md, channel reply destination, abridged` · full text: [events.md](../contracts/events.md)

## Acceptance criteria

### AC-05: domain invariant

> **Given** a request attempt whose reply was already committed
> **When** a Cycle is repeated after a failure or restart
> **Then** a Requester reading committed replies sees only one reply for that request attempt (a new attempt by the Requester is a new request)
>
> — `spec.md §5, AC-05, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Implement `ReplySink` in `src/main/java/xme/common/kfkprocessor/requestreply/adapters/kafka/`: transactional producer, transactional id derived from `worker-identity`, transaction timeout = `commit-window`
- [ ] One `commit(replies, positions)` call = one transaction (replies + consumed offsets)
- [ ] Report per-reply failures (too large, cannot be encoded, rejected) as a typed per-request result, not as a transaction abort
- [ ] Report destination unavailable and permission denied as distinct typed faults for T12
- [ ] Integration test under `src/test/java/xme/common/kfkprocessor/requestreply/adapters/kafka/`: kill before commit then re-run, a committed-only reader sees exactly one reply

## Edge cases

| Case | Behaviour |
|---|---|
| Cycle repeated after a failure or restart | Committed-only reader sees one reply for the attempt; a new Requester attempt is a new request (AC-05). |
| Producer fenced by a newer instance with the same identity | Surfaced as a commit failure for T12 retry handling; no partial reply is visible. |
| One reply too large | Typed per-request failure; other replies in the Cycle unaffected. |
| Transaction exceeds the commit window | Aborts; nothing visible to committed-only readers. |

## Definition of Done

- [ ] Integration test: abandoned and re-run Cycle yields exactly one committed reply per request
- [ ] Per-request send failures do not abort the transaction
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
