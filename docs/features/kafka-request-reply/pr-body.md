## Summary

Ships the `kafka-request-reply` engine: a reusable Spring Boot starter where a team writes a Handler and configuration, and gets weighted Priority Lanes, a global fail-closed Rate Budget, per-Cycle transactional replies, Error Replies, stable-identity rollouts, metrics and alerts. See [spec](docs/features/kafka-request-reply/spec.md) and [changelog](docs/features/kafka-request-reply/changelog.md).

## Acceptance criteria

AC-01 to AC-19 delivered, with ten review rounds (r1 to r10, all PASS at the end). Documented exceptions needing Tech Lead sign-off (spec §8):

- AC-07b, AC-08: Handlers re-run after a Cycle that fails after dispatch (hand-back). Handlers must be idempotent.
- AC-08b: a fenced reply producer needs a manual restart; residual gaps A1 to A5; no real-broker IT for an undeliverable Error Reply.
- Deferred K1, H1/J1.

## Design

- Spec: `docs/features/kafka-request-reply/spec.md`
- Architecture: `docs/features/kafka-request-reply/sad.md`
- Decisions: `docs/features/kafka-request-reply/adr/` (0001 to 0007)
- Contracts: `docs/features/kafka-request-reply/contracts/` (public-api, events)
- Data model / migration: none

## Tasks (SDD-Task trailers)

57 distinct `SDD-Task` references across 71 commits: `git log master..HEAD --grep SDD-Task`.

## Verification

- `./gradlew clean check`: 354 tests, 0 failures, 0 errors, 0 skipped. BUILD SUCCESSFUL.
- Integration: 19 real-broker ITs (Docker, Testcontainers) all green.
- Lint + vet: the project defines no separate lint or vet task; `check` is the only gate.
- Ran the feature: AC-01 `RequestReplyEndToEndIT` (reply carries the request's correlation id); AC-06 `ErrorPathsIT` (a failing Handler yields an Error Reply, neighbours get normal replies); AC-11/AC-12 `WeightAccuracyIT` (lane share within tolerance, 5% minimum share); AC-10 `RateBudgetAccuracyIT`; AC-09 `RequestReplyPermissionIT`; AC-18 `RateBudgetStoreOutageIT`. Not run: a standalone app against a staging cluster (the repo is a starter library with a host test app only).

## Operational notes

- Migration: none. Rollback: remove the starter and configuration.
- Config: transactional Kafka, Redis-compatible store, stable worker identity, non-compacted reply topic.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
