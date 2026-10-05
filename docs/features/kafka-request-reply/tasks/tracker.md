# Tracker: kafka-request-reply

> Status of every task in the epic. `implement` updates `done` as it commits each task.
> States: `todo` · `in_progress` · `blocked` · `review` · `done`.

| # | Task | Layer | Owner | Estimate | Blocked by | Status |
|---|---|---|---|---|---|---|
| T1 | Define public API types, ports and Idempotency Key | domain | <TBD lead> | M | none | done |
| T2 | Implement lane share calculator (weights, 5% minimum, idle redistribution) | domain | <TBD lead> | M | none | done |
| T3 | Add configuration properties and startup validation | app | <TBD lead> | M | T1, T2 | done |
| T4 | Implement the Redis-compatible AllowanceStore adapter | infra | <TBD lead> | L | T1 | done |
| T5 | Implement the Kafka RequestLanes adapter with stable worker identity | infra | <TBD lead> | L | T1 | done |
| T6 | Implement the transactional Kafka ReplySink | infra | <TBD lead> | M | T1 | done |
| T7 | Implement worker pause and stall state machine | app | <TBD lead> | S | T1 | done |
| T8 | Implement intake: reserve allowance, weighted fetch, return unused, fail closed | app | <TBD lead> | L | T1, T2, T7 | done |
| T9 | Implement Handler execution: virtual threads, timeout, cancellation, Error Replies | app | <TBD lead> | L | T1 | done |
| T10 | Add per-Request-Key ordering inside a lane | app | <TBD lead> | S | T9 | done |
| T11 | Assemble the Cycle commit and turn undeliverable replies into Error Replies | app | <TBD lead> | M | T1 | done |
| T12 | Add commit retry, destination pause and resume | app | <TBD lead> | M | T7, T11 | done |
| T13 | Implement the Cycle loop that ties intake, execution and commit together | app | <TBD lead> | M | T7, T8, T10, T12 | done |
| T14 | Implement the Micrometer WorkerMetrics adapter | infra | <TBD lead> | M | T1, T7 | done |
| T15 | Wire the Spring Boot auto-configuration and startup permission check | wiring | <TBD lead> | M | T3, T4, T5, T6, T13, T14 | done |
| T16 | Add the failure-scenario test suite | tests | <TBD lead> | L | T15 | done |
| T17 | Add rollout, weight-accuracy and throughput tests | tests | <TBD lead> | L | T15 | done |
| T18 | Write the starter guide and operator runbook | docs | <TBD lead> | M | T15 | done |

**Total:** 18 tasks, ~14.5 person-days (S=0.5, M=0.75, L=1 day; L means the full day, anything bigger was split).

## Review follow-ups (from `_review/review-2026-10-05.md`)

| # | Task | Status |
|---|---|---|
| F1 | Failing ITs AC-10 / AC-11 | done |
| F2 | KafkaReplySink A1, A2, B8 | done |
| F3 | Poll while paused, intake loss, buffer bound (A3, A4, A7) | todo |
| F4 | Stall gauge + metrics (A5, B12) | todo |
| F5 | Graceful stop (A6) | todo |
| F6 | Startup validation, handler types, docs (B9, B14, B13) | todo |
| F7 | Probe (B10) | todo |
| F8 | Per-lane key chains (B11) | todo |
| F9 | Test gaps (Group D) | todo |
| F10 | Spec drift + stage-2 docs (Group C) | todo |
