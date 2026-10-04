---
id: T5
title: "Implement the Kafka RequestLanes adapter with stable worker identity"
layer: "infra"
deps: ["T1"]
blocks: ["T15"]
acs: ["AC-14"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/adapters/kafka/KafkaRequestLanes.java"]
owner: "<TBD lead>"
estimate: "L"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T5: Implement the Kafka RequestLanes adapter with stable worker identity

## Place in the sequence

- **Blocked by:** T1 (Define public API types, ports and Idempotency Key) · **Blocks:** T15 (Wire the Spring Boot auto-configuration and startup permission check) · **Wave:** 2, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As an** Operator
> **I want** to restart or replace workers one by one without reassigning work across the group
> **So that** deployments do not interrupt consumption
>
> — `spec.md §4, US-07, verbatim` · full text: [spec.md](../spec.md)

Consumes every lane in every worker under the configured stable identity so a restarted worker takes its lanes back and others keep consuming.

## Inlined context

> **Chosen:** Every worker consumes every lane with a weighted split of its own draw. A lane below the 5% minimum share is raised to it and the rest scaled; idle lanes' share goes to busy lanes.
>
> — `adr/0004, Decision outcome, verbatim` · full text: [0004-consume-every-lane-in-every-worker-and-split-each-draw-by-weight.md](../adr/0004-consume-every-lane-in-every-worker-and-split-each-draw-by-weight.md)

> **Chosen:** Explicit identity required; absent identity refuses start (AC-15). The same identity is used for group membership and the transaction.
>
> — `adr/0005, Decision outcome, verbatim` · full text: [0005-require-a-stable-worker-identity.md](../adr/0005-require-a-stable-worker-identity.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Identity window for a returning worker | 45 s (provisional) | rollout test |
> | Lane reassignment during rolling restart | 0 for workers that return within the identity window | rollout test + group membership change counter |
>
> — `spec.md §6, NFR table rows Identity window for a returning worker, Lane reassignment during rolling restart, verbatim` · full text: [spec.md](../spec.md)

> `Note over Platform,Engine: lanes held for the Identity window (45 s), other workers keep consuming, no reassignment`
> `Engine->>Platform: rejoins and takes its lanes back`
> `Platform-->>Engine: delivers R again (position was never committed)`
>
> — `sad.md §6, flow 5 «killed mid-Cycle, restart with the same identity», abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- Channel request lanes: producer Requester, consumer every worker, partitions spread across workers. Ordering by Request Key within a lane only.
- `request_reply.request.v1`: `correlation_id`, `request_key` required (missing → malformed), `created_at`, `data`. Open (OQ-1): record headers or body fields.

— `contracts/events.md, channel request lanes, request_reply.request.v1, abridged` · full text: [events.md](../contracts/events.md)

## Acceptance criteria

### AC-14: happy path

> **Given** workers are restarted one by one, each returning with the same identity within the allowed window
> **When** the rollout runs
> **Then** the other workers keep consuming without any reassignment of lanes
>
> — `spec.md §5, AC-14, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Implement `RequestLanes` in `src/main/java/xme/common/kfkprocessor/requestreply/adapters/kafka/`: one subscription over all configured lane sources, group membership keyed by `worker-identity` and `identity-window` (ADR-0005)
- [ ] Read committed data only; fetch per lane up to the unit count given by intake; offsets are never committed here (the Cycle transaction commits them, T6)
- [ ] Surface a record missing `correlation_id` or `request_key` as a malformed request instead of throwing
- [ ] Expose the group membership change count for `requestreply.group.membership.changes`
- [ ] Integration test under `src/test/java/xme/common/kfkprocessor/requestreply/adapters/kafka/` with Testcontainers Kafka: two consumers, restart one with the same identity within the window, no rebalance for the other

## Edge cases

| Case | Behaviour |
|---|---|
| Worker returns with the same identity within 45 s | Takes its lanes back; other workers see no reassignment. |
| Worker does not return within the Identity window | Platform reassigns its lanes to the others (stranded-lane alert is operational, see T18). |
| Record without `correlation_id` or `request_key` | Reported as malformed to intake; no exception, no stop. |
| More workers than partitions of the busiest lane | Extra workers idle; no error. |

## Definition of Done

- [ ] Integration test: rolling restart within the identity window causes 0 reassignments for the other worker
- [ ] Malformed record is surfaced, not thrown
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
