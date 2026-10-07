# Changelog: remote-docker-test

## remote-docker-test: run the test suite's containers on your own remote host

**What:** `./gradlew test -Premote` starts the Kafka and Redis test containers on a Developer's own remote host, reached through a build-managed ssh tunnel to its container engine. Without the switch nothing changes. Every run prints its target. Bad setup stops the run before any container starts, with no fallback to local.

**Why:** local Docker is heavy for the full integration suite ([spec](spec.md) §1). Key decisions: [ADR-0001](adr/0001-switch-remote-mode-with-a-gradle-property-that-sets-the-engine-environment.md) switch and engine environment, [ADR-0002](adr/0002-reach-the-remote-engine-through-a-build-managed-ssh-tunnel.md) ssh tunnel (access to the remote engine is host control, so only ssh addresses are accepted), [ADR-0003](adr/0003-verify-the-container-target-in-the-build-before-any-test-starts.md) engine check before tests.

**How to use:** put `remoteDocker.host=ssh://user@host` in `~/.gradle/gradle.properties` (per user, never in the project), then run `./gradlew test -Premote`. Full guide: [docs/remote-docker-test.md](../../remote-docker-test.md).

**Operational notes:**
- Migration: <!-- none -->
- Feature flag / config: `-Premote` (off by default); per-user `remoteDocker.host`, optional `remoteDocker.socket`. `loadTest` and `preReleaseTest` are refused with `-Premote`.
- Rollback: revert the PR; `gradle/remote-docker.gradle` and its `apply` line in `build.gradle` are the only runtime-affecting changes.
- Known limits: needs `ssh` on the PATH and key auth; timing-sensitive tests (`RedisAllowanceStoreIT`) fail over the network and are accepted per the spec's AC-02 exception; verified on a real remote host twice (355 tests, only the 2 timing failures, about 10 to 12 min) and an unreachable host stopped in 21 s; measured numbers are in `verification.md` (local, not committed because it holds a host address).

**Acceptance criteria delivered:** AC-01 local default unchanged, stale remote engine settings cleared; AC-02 remote run (with the timing exception); AC-03/03b no stale "up to date" between modes; AC-04 missing address message; AC-05 address kept out of shared files; AC-06/AC-08 stop within 30 s when the host or its engine is down; AC-07 target banner; AC-09 load commands refused; AC-10 only ssh addresses.
