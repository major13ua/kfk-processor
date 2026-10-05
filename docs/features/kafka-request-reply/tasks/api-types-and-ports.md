---
id: T1
title: "Define public API types, ports and Idempotency Key"
layer: "domain"
deps: []
blocks: ["T3", "T4", "T5", "T6", "T7", "T8", "T9", "T11", "T14"]
acs: ["AC-03", "AC-04"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/api/", "src/main/java/xme/common/kfkprocessor/requestreply/ports/"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T1: Define public API types, ports and Idempotency Key

## Place in the sequence

- **Blocked by:** none · **Blocks:** T3 (Add configuration properties and startup validation), T4 (Implement the Redis-compatible AllowanceStore adapter), T5 (Implement the Kafka RequestLanes adapter with stable worker identity), T6 (Implement the transactional Kafka ReplySink), T7 (Implement worker pause and stall state machine), T8 (Implement intake: reserve allowance, weighted fetch, return unused, fail closed), T9 (Implement Handler execution: virtual threads, timeout, cancellation, Error Replies), T11 (Assemble the Cycle commit and turn undeliverable replies into Error Replies), T14 (Implement the Micrometer WorkerMetrics adapter) · **Wave:** 1, no dependencies, starts immediately
- **Lane:** own lane.

## Why (user story)

> **As a** Handler Owner
> **I want** each Handler call to carry an Idempotency Key stable across re-executions
> **So that** I can make repeated execution of my side effects harmless
>
> Source: `spec.md §4, US-02, verbatim` · full text: [spec.md](../spec.md)

Delivers the Idempotency Key (same value on every re-execution) and the types that carry correlation and Request Key unchanged.

## Inlined context

> **Chosen:** Starter library embedded in each worker. Target surfaces are `library-sdk` (public Handler and configuration contract) and `worker` (the running engine). It matches the spec's handler-only adoption goal and keeps Handlers next to the services they call.
>
> Source: `adr/0001, Decision outcome, verbatim` · full text: [0001-ship-engine-as-spring-boot-starter.md](../adr/0001-ship-engine-as-spring-boot-starter.md)

> web and REST client dependencies are not part of the starter contract and must not be pulled into adopters.
>
> Source: `sad.md §2, Override note, abridged` · full text: [sad.md](../sad.md)

> an `engine` core that knows nothing about the platform client, the store or the metrics registry, talking through ports implemented by adapters.
>
> Source: `sad.md §5, Building block view, abridged` · full text: [sad.md](../sad.md)

> `api/          Handler interface, request/reply/Error Reply types, configuration properties (library-sdk contract)`
> `engine/       Cycle loop, intake with allowance, lane weights, Handler execution, commit, pause/stall state`
> `ports/        AllowanceStore, RequestLanes, ReplySink (transactional), WorkerMetrics`
> `adapters/     Kafka (lanes + transactional reply sink), shared-store allowance (Redis-compatible), Micrometer`
>
> Source: `sad.md §5, internal decomposition, abridged` · full text: [sad.md](../sad.md)

> | ID strategy | Idempotency Key = lane + partition + position of the request: stable across re-execution, identifies the attempt, not the business operation; correlation identifier and Request Key echoed unchanged | here |
>
> Source: `sad.md §8, ID strategy, verbatim` · full text: [sad.md](../sad.md)

> - **Personal data touched:** none added by the starter. Payloads may contain personal data; the starter must not copy payload content into logs, metrics or Error Replies.
>
> Source: `spec.md §6.1, Personal data touched, verbatim` · full text: [spec.md](../spec.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- `RequestReplyHandler<K,REQ,RES>.handle(RequestContext<K>, REQ) throws Exception`: any exception becomes an Error Reply (failure).
- `RequestContext<K>`: `requestKey`, `correlationId`, `idempotencyKey` (`lane:partition:position`), `lane`, `headers` (read only), `cancellation` (`CancellationSignal`: `isCancelled()`, `throwIfCancelled()`).
- `ErrorCategory { FAILURE, TIMEOUT, UNDELIVERABLE }` (proposal, OQ-1). An Error Reply carries category, correlation id and Request Key only.
- Public SPI `AllowanceStore { long reserve(long units); void giveBack(long units); }`; `reserve` throws `AllowanceStoreUnavailableException` (fail closed). `RequestLanes`, `ReplySink`, `WorkerMetrics` are internal ports.

: `contracts/public-api.md §1, 2, 4, abridged` · full text: [public-api.md](../contracts/public-api.md)

- `reply.v1`: `correlation_id`, `request_key`, `data`. `error_reply.v1`: `correlation_id`, `request_key`, `category` (failure | timeout | undeliverable). No message, stack trace or payload.

: `contracts/events.md, request_reply.reply.v1 / error_reply.v1, abridged` · full text: [events.md](../contracts/events.md)

## Acceptance criteria

### AC-03: happy path

> **Given** a request is delivered to a Handler
> **When** the same request is executed again after a failure or restart
> **Then** the Handler receives the same Idempotency Key as the first time
>
> Source: `spec.md §5, AC-03, verbatim` · full text: [spec.md](../spec.md)

### AC-04: happy path

> **Given** a Requester sent a request with a correlation identifier and a Request Key
> **When** the reply is committed
> **Then** the reply carries the same correlation identifier and Request Key
>
> Source: `spec.md §5, AC-04, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `RequestReplyHandler`, `RequestContext`, `CancellationSignal`, `ErrorCategory`, reply and Error Reply records, `AllowanceStoreUnavailableException` in `src/main/java/xme/common/kfkprocessor/requestreply/api/`
- [ ] Create ports `AllowanceStore`, `RequestLanes`, `ReplySink`, `WorkerMetrics` in `src/main/java/xme/common/kfkprocessor/requestreply/ports/` (interfaces only, no adapter code)
- [ ] Add `IdempotencyKey.of(lane, partition, position)` returning `lane:partition:position` in `src/main/java/xme/common/kfkprocessor/requestreply/api/`
- [ ] Add an Error Reply factory taking category, correlation id and Request Key only (no exception text)
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/api/`: key stable across calls, differs by lane, partition or position; Error Reply holds no free text

## Edge cases

| Case | Behaviour |
|---|---|
| Same lane, partition, position twice | Identical `idempotencyKey` (AC-03). |
| Different position in the same lane and partition | Different `idempotencyKey`. |
| Handler throws with a message containing payload data | Error Reply is built from category, correlation id and Request Key only; the message is never copied. |
| Request Key or correlation id present on the request | Echoed unchanged on the reply and the Error Reply (AC-04). |

## Definition of Done

- [ ] Unit tests for key stability and Error Reply content pass
- [ ] API package compiles without any web or REST client import
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
