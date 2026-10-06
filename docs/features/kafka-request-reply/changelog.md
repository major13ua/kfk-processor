# Changelog: kafka-request-reply

## kafka-request-reply: reusable Kafka request-reply worker engine (Spring Boot starter)

**What:** An internal Spring Boot starter (package under `src/main/java`, auto-configured via `AutoConfiguration.imports`). A team supplies a Handler and configuration; the engine does the rest: N Priority Lanes split by Priority Weight, one global Rate Budget enforced across all workers through a Redis-compatible shared store (fail-closed), Handlers on virtual threads with a per-request timeout and Cycle deadline, one transaction per Cycle committing replies and consumer positions together, Error Replies for per-request failures, stable worker identity for rollouts, Micrometer metrics (Consistency Lag, stall, pause state) and Operator alerts.

**Why:** Each team wrote its own consume loop and hit the same problems (bursts flooding downstream services, slow requests blocking a batch, duplicate or lost replies). See [spec](./spec.md) §1 and §2. Key decisions: [ADR-0001](./adr/0001-ship-engine-as-spring-boot-starter.md) starter shape, [ADR-0002](./adr/0002-commit-replies-and-positions-in-one-transaction-per-cycle.md) one transaction per Cycle, [ADR-0003](./adr/0003-take-rate-budget-from-a-shared-store-and-fail-closed.md) shared fail-closed Rate Budget, [ADR-0004](./adr/0004-consume-every-lane-in-every-worker-and-split-each-draw-by-weight.md) weighted lanes, [ADR-0005](./adr/0005-require-a-stable-worker-identity.md) stable identity, [ADR-0006](./adr/0006-reserve-rate-budget-allowance-before-fetching-and-return-unused.md) allowance reservation, [ADR-0007](./adr/0007-turn-send-failures-into-error-replies-and-retry-commits-without-rerunning-handlers.md) Error Replies and commit retry.

**How to use:** Implement the Handler interface and set the lane, Rate Budget, timeout and identity properties. Contract: [public-api.md](./contracts/public-api.md), [events.md](./contracts/events.md); starter guide: `docs/kafka-request-reply-starter-guide.md`. Handlers are at-least-once: use the Idempotency Key.

**Operational notes:**
- Migration: <!-- none -->
- Config: needs a Kafka cluster with transactions, a Redis-compatible store for the Rate Budget, a stable worker identity per instance, and permission to read lanes and write the reply topic and commit records (missing permission pauses the worker with an alert). The reply topic must not be compacted (refused at start). Invalid or conflicting configuration refuses to start.
- Rollback: remove the starter dependency and configuration; no schema or data migration. Reply and lane topics are left as they are.
- Known deviations, accepted for the pilot and pending Tech Lead sign-off ([spec](./spec.md) §8): hand-back rerun of Handlers after a failed Cycle (AC-07b/AC-08), AC-08b fence and outage gaps (A1 to A5), K1, H1/J1. Handlers must be idempotent. Runbook: a reply-destination outage longer than the 60 s commit window needs a worker restart.

**Acceptance criteria delivered:** AC-01 to AC-19 (including AC-07b, AC-07c, AC-08b, AC-10b); AC-07b, AC-08, AC-08b carry the documented exceptions above.
