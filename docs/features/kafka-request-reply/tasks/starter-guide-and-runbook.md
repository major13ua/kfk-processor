---
id: T18
title: "Write the starter guide and operator runbook"
layer: "docs"
deps: ["T15"]
blocks: []
acs: ["AC-07"]
files_hint: ["docs/kafka-request-reply-starter-guide.md"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T18: Write the starter guide and operator runbook

## Place in the sequence

- **Blocked by:** T15 (Wire the Spring Boot auto-configuration and startup permission check) · **Blocks:** none · **Wave:** 6, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As a** Handler Owner
> **I want** to supply only a Handler and configuration
> **So that** I get a working request-reply worker without writing consumption, rate control or commit logic
>
> — `spec.md §4, US-01, verbatim` · full text: [spec.md](../spec.md)

Documents the rules a Handler Owner, Requester and Operator must follow that the code cannot enforce.

## Inlined context

> - Business retry after a Requester timeout: the Idempotency Key identifies the attempt, not the business operation, so Handler Owners must deduplicate with their own business identifier; documented in the starter guide.
>
> — `spec.md §6.1, Abuse cases, verbatim` · full text: [spec.md](../spec.md)

> | Requesters reading uncommitted replies see abandoned commits; a stuck commit delays everyone for the Commit window (ADR-0002, spec §8 Q6) | High | Document the committed-reads-only requirement in the starter guide | Tech Lead | before `sdd:tasks` |
>
> — `sad.md §11, risk, committed reads, verbatim` · full text: [sad.md](../sad.md)

> | Lost or scaled-down workers hold their lanes until the Identity window ends (ADR-0005, spec §8 Q7) | Medium | Runbook plus stranded-lane alert | Operator lead | before `sdd:tasks` |
>
> — `sad.md §11, risk, stranded lanes, verbatim` · full text: [sad.md](../sad.md)

> | Cycle results are held in memory during commit retries and pauses; a crash re-runs Handlers (at-least-once, ADR-0007) | Low | Idempotency Key; documented in the starter guide | Handler Owner | starter guide |
>
> — `sad.md §11, risk, at-least-once on crash, verbatim` · full text: [sad.md](../sad.md)

> | Open architectural decision: cross-lane ordering of same-key requests | Open question | Guarantee is order per key within a lane; Requesters must send same-key requests to one lane (cross-lane order is not guaranteed: a low-share lane can deliver an older request in a later Cycle); tighten spec AC-07c ("arrival order") to "within a lane" and state the rule in the starter guide | Tech Lead | before `sdd:tasks` |
>
> — `sad.md §11, open decision, ordering rule, verbatim` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal — no API surface.

## Acceptance criteria

### AC-07: error

> **Given** a Handler does not finish within the per-request timeout
> **When** the timeout elapses (the timer starts when the Handler is dispatched)
> **Then** the Requester receives an Error Reply for a timeout, the Handler is signalled to cancel (cooperative: a Handler may still finish its side effects, which the starter guide documents next to the Idempotency Key note), and the Cycle is not delayed beyond its deadline
>
> — `spec.md §5, AC-07, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Write `docs/kafka-request-reply-starter-guide.md`: Handler contract, Idempotency Key (attempt, not business operation, so dedupe on a business id), cooperative cancellation and that side effects may finish after a timeout (AC-07), at-least-once
- [ ] Requester rules: read committed replies only, send same-key requests to one lane, correlation and Request Key echo, Error Reply categories
- [ ] Operator runbook: Rate Budget and Priority Weights identical in all workers and changed only by rollout, stable identity per replica, stranded-lane alert, pause and stall alerts, paused-state meaning
- [ ] Link the open points (provisional numbers, Consistency Lag target) instead of stating them as decided
- [ ] Metric names marked as proposals until spec §8 Q5 closes

## Edge cases

| Case | Behaviour |
|---|---|
| Handler finishes side effects after cancellation | Documented next to the Idempotency Key note (AC-07). |
| Requester retries the business operation | New attempt, new Idempotency Key; Handler Owner must dedupe on a business identifier. |
| Worker lost, not returning | Lanes held until the Identity window ends; runbook says what the alert means and what to do. |

## Definition of Done

- [ ] Guide covers every documented requirement listed in the checklist
- [ ] Cancellation and Idempotency Key notes sit together
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
