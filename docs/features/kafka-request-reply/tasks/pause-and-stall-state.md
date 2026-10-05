---
id: T7
title: "Implement worker pause and stall state machine"
layer: "app"
deps: ["T1"]
blocks: ["T8", "T12", "T13", "T14"]
acs: ["AC-17"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/engine/WorkerState.java"]
owner: "<TBD lead>"
estimate: "S"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T7: Implement worker pause and stall state machine

## Place in the sequence

- **Blocked by:** T1 (Define public API types, ports and Idempotency Key) · **Blocks:** T8 (Implement intake: reserve allowance, weighted fetch, return unused, fail closed), T12 (Add commit retry, destination pause and resume), T13 (Implement the Cycle loop that ties intake, execution and commit together), T14 (Implement the Micrometer WorkerMetrics adapter) · **Wave:** 2, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As an** Operator
> **I want** Consistency Lag and a clear stall indicator
> **So that** I can detect trouble even when no replies are being committed
>
> Source: `spec.md §4, US-08, verbatim` · full text: [spec.md](../spec.md)

Tracks running, idle, paused and stalled so a stalled worker is never mistaken for a healthy one and a pause never raises a false stall.

## Inlined context

> `Engine->>Engine: compares pending work or open Cycle with time since last commit`
> `Engine->>Metrics: shows paused state, stall indicator suppressed`
> `Engine->>Metrics: raises stall indicator`
> `Engine->>Metrics: reports idle, no stall`
> `Engine->>Metrics: clears stall, records Consistency Lag per lane, flags and excludes implausible samples`
>
> Source: `sad.md §6, flow 8 «stall detection and pause state», abridged` · full text: [sad.md](../sad.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Stall detection | stall indicator when work is pending and 60 s pass without a commit (provisional) | stall-indicator metric |
>
> Source: `spec.md §6, NFR table rows Stall detection, verbatim` · full text: [spec.md](../spec.md)

> - Alert rules (AC-17): a stall is raised only when requests are pending or a Cycle is open and nothing commits for 60 s; an idle worker raises none, and a pause is shown as its own state and suppresses the stall indicator.
>
> Source: `sad.md §7, Monitoring, verbatim` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal: no API surface.

## Acceptance criteria

### AC-17: error

> **Given** requests are pending or a Cycle is open, and the worker has committed nothing for longer than the stall threshold in §6
> **When** the Operator looks at the monitoring view
> **Then** a stall indicator is raised, so missing lag data is not mistaken for a healthy worker; an idle worker with nothing pending raises no stall, and a pause (AC-09, AC-18) is shown as its own state and suppresses the stall indicator
>
> Source: `spec.md §5, AC-17, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `WorkerState` in `src/main/java/xme/common/kfkprocessor/requestreply/engine/`: states running, idle, paused(reason: limiter, destination, permission), stalled; injectable clock
- [ ] Track last commit time, pending-work flag and open-Cycle flag; evaluate against `stall-threshold` (default 60s)
- [ ] Pause takes precedence and suppresses the stall indicator; a successful commit clears stall
- [ ] Publish state changes through the `WorkerMetrics` port
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/engine/` with a fake clock for every transition

## Edge cases

| Case | Behaviour |
|---|---|
| Nothing pending, no commit for hours | Idle, no stall raised. |
| Pending work, 60 s without a commit | Stalled indicator raised. |
| Paused because of limiter or destination | Shown as paused; stall suppressed even beyond 60 s. |
| Next commit succeeds after a stall | Stall cleared. |

## Definition of Done

- [ ] Unit tests cover idle, running, paused, stalled and recovery transitions
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
