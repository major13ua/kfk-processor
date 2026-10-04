---
id: T3
title: "Add configuration properties and startup validation"
layer: "app"
deps: ["T1", "T2"]
blocks: ["T15"]
acs: ["AC-02", "AC-15"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/api/RequestReplyProperties.java", "src/main/java/xme/common/kfkprocessor/requestreply/autoconfig/StartupValidator.java"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T3: Add configuration properties and startup validation

## Place in the sequence

- **Blocked by:** T1 (Define public API types, ports and Idempotency Key), T2 (Implement lane share calculator (weights, 5% minimum, idle redistribution)) · **Blocks:** T15 (Wire the Spring Boot auto-configuration and startup permission check) · **Wave:** 2, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As a** Handler Owner
> **I want** to supply only a Handler and configuration
> **So that** I get a working request-reply worker without writing consumption, rate control or commit logic
>
> — `spec.md §4, US-01, verbatim` · full text: [spec.md](../spec.md)

Makes the worker refuse to start, in plain language, when configuration conflicts, a weight is not positive, there is no Handler or no worker identity.

## Inlined context

> `Engine->>Engine: validates configuration (Handler present, weights above 0, timeout fits Cycle deadline, Cycle deadline within share of Commit window, explicit worker identity)`
> `Engine-->>Operator: refuses to start, names the conflicting values in plain language`
> `Engine->>Engine: logs effective share per lane (5% minimum applied, other lanes scaled down)`
>
> — `sad.md §6, flow 4 «startup validation», abridged` · full text: [sad.md](../sad.md)

> **Chosen:** Explicit identity required; absent identity refuses start (AC-15). The same identity is used for group membership and the transaction.
>
> — `adr/0005, Decision outcome, verbatim` · full text: [0005-require-a-stable-worker-identity.md](../adr/0005-require-a-stable-worker-identity.md)

> | Aspect | Target | Measurement |
> |---|---|---|
> | Per-request Handler timeout | default 30 s from dispatch, configurable (provisional); implies a commit window above 37.5 s | startup validation + timeout counter |
> | Cycle deadline | ≤ 80% of the commit window, checked at startup (provisional) | startup validation + cycle-duration metric |
> | Commit window | 60 s (provisional) | startup validation |
> | Priority Weight validity | every lane has a weight above 0; startup refuses a weight of 0 | startup validation |
>
> — `spec.md §6, NFR table rows Per-request Handler timeout, Cycle deadline, Commit window, Priority Weight validity, verbatim` · full text: [spec.md](../spec.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- `@ConfigurationProperties("xme.request-reply")`: `worker-identity` (required, explicit), `reply-destination`, `rate-budget-per-second` (> 0), `lanes[].name|source|weight` (weight > 0), `min-lane-share` (5), `handler-timeout` (30s), `cycle-deadline` (derived, ≤ 80% of `commit-window`), `commit-window` (60s), `commit-retry-attempts` (3), `identity-window` (45s), `stall-threshold` (60s), `allowance-store.*`.
- Startup error codes: `request_reply.config.handler_missing`, `.timeout_exceeds_cycle_deadline`, `.cycle_deadline_exceeds_commit_window_share`, `.weight_not_positive`, `.identity_missing`.

— `contracts/public-api.md §3, abridged` · full text: [public-api.md](../contracts/public-api.md)

## Acceptance criteria

### AC-02: invalid configuration

> **Given** a configuration where the per-request timeout does not fit inside the Cycle deadline, or the Cycle deadline exceeds the share of the commit window allowed in §6, or a Priority Weight is not a positive number, or which has no Handler
> **When** a worker starts
> **Then** it refuses to start and tells the Operator in plain language which values conflict
> **And** (valid configuration) the worker logs the effective share of each Priority Lane; a lane whose share would fall below the 5% minimum share is raised to it and the other lanes are scaled down
>
> — `spec.md §5, AC-02, verbatim` · full text: [spec.md](../spec.md)

### AC-15: error

> **Given** a worker starts without an explicitly configured worker identity
> **When** it starts
> **Then** it refuses to start and explains that a stable identity must be configured
>
> — `spec.md §5, AC-15, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `RequestReplyProperties` (record or `@ConfigurationProperties` class) with the properties and defaults above in `src/main/java/xme/common/kfkprocessor/requestreply/api/`
- [ ] Create `StartupValidator` in `src/main/java/xme/common/kfkprocessor/requestreply/autoconfig/` checking: exactly one Handler bean, weights > 0, `handler-timeout` fits `cycle-deadline`, `cycle-deadline` ≤ 80% of `commit-window`, explicit non-blank `worker-identity`
- [ ] Failure messages are plain language and name the conflicting values and the error code
- [ ] On a valid configuration, log the effective share of each lane via `LaneShares` (T2), including any lane raised to the minimum
- [ ] Confirm with the Tech Lead how `cycle-deadline` is derived when unset: public-api.md says «derived», no rule is stated upstream. Default to the 80% maximum and record the choice in the PR
- [ ] Unit tests under `src/test/java/xme/common/kfkprocessor/requestreply/autoconfig/`: one per refusal code, valid configuration, share log

## Edge cases

| Case | Behaviour |
|---|---|
| Timeout longer than the Cycle deadline | Start refused with `timeout_exceeds_cycle_deadline` naming both values. |
| Cycle deadline above 80% of the commit window | Start refused with `cycle_deadline_exceeds_commit_window_share`. |
| Weight 0 or negative | Start refused with `weight_not_positive`. |
| No Handler bean, or more than one | Start refused with `handler_missing` (public-api.md rule: exactly one Handler per worker). |
| No explicit worker identity | Start refused with `identity_missing`, message says a stable identity must be configured. |
| Valid configuration with a lane below 5% | Start succeeds; log shows the raised lane and the scaled-down others. |

## Definition of Done

- [ ] Each refusal case in AC-02 and AC-15 has a passing test with the plain-language message asserted
- [ ] Valid configuration logs the effective share of each lane
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
