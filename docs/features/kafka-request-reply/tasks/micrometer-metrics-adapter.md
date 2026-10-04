---
id: T14
title: "Implement the Micrometer WorkerMetrics adapter"
layer: "infra"
deps: ["T1", "T7"]
blocks: ["T15"]
acs: ["AC-16"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/adapters/metrics/MicrometerWorkerMetrics.java"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T14: Implement the Micrometer WorkerMetrics adapter

## Place in the sequence

- **Blocked by:** T1 (Define public API types, ports and Idempotency Key), T7 (Implement worker pause and stall state machine) · **Blocks:** T15 (Wire the Spring Boot auto-configuration and startup permission check) · **Wave:** 3, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As an** Operator
> **I want** Consistency Lag and a clear stall indicator
> **So that** I can detect trouble even when no replies are being committed
>
> — `spec.md §4, US-08, verbatim` · full text: [spec.md](../spec.md)

Publishes Consistency Lag per lane, the accepted rate, state and error counters the Operator watches.

## Inlined context

> `Engine->>Metrics: clears stall, records Consistency Lag per lane, flags and excludes implausible samples`
>
> — `sad.md §6, flow 8 «stall detection and pause state», abridged` · full text: [sad.md](../sad.md)

> - Forged or skewed request timestamps: Consistency Lag may go negative or inflate; the metric flags and excludes implausible samples.
>
> — `spec.md §6.1, Abuse cases, verbatim` · full text: [spec.md](../spec.md)

> - **Personal data touched:** none added by the starter. Payloads may contain personal data; the starter must not copy payload content into logs, metrics or Error Replies.
>
> — `spec.md §6.1, Personal data touched, verbatim` · full text: [spec.md](../spec.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- Meters (tags `lane`, `category` only): `requestreply.accepted`, `requestreply.consistency.lag` (timer per lane), `requestreply.state` (gauge), `requestreply.errorreply` (by category), `requestreply.handler.timeout`, `requestreply.commit.attempts`, `requestreply.cycle.duration`, `requestreply.group.membership.changes`. Names are proposals.

— `contracts/public-api.md §5, abridged` · full text: [public-api.md](../contracts/public-api.md)

## Acceptance criteria

### AC-16: happy path

> **Given** replies are being committed
> **When** each Cycle commits
> **Then** Consistency Lag is recorded for every committed reply and shown to the Operator per lane
>
> — `spec.md §5, AC-16, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Implement `WorkerMetrics` in `src/main/java/xme/common/kfkprocessor/requestreply/adapters/metrics/` over `MeterRegistry`
- [ ] Lag timer per lane; negative or implausible samples are flagged (counter) and excluded from the timer
- [ ] No tag other than `lane` and `category`; no keys, no payload
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/adapters/metrics/` with `SimpleMeterRegistry`

## Edge cases

| Case | Behaviour |
|---|---|
| Negative lag (clock skew) | Flagged and excluded from the lag timer. |
| Implausibly large lag from a forged timestamp | Flagged and excluded. |
| Metric tags | Only `lane` and `category`; correlation id and Request Key never appear. |

## Definition of Done

- [ ] Unit tests: lag recorded per lane for valid samples, excluded for implausible ones
- [ ] No payload or key appears in any tag
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
