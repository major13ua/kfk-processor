## Summary

Adds an opt-in `-Premote` switch so `./gradlew test` runs its Testcontainers on the Developer's own remote host over an ssh tunnel; default behaviour is unchanged. See [spec](docs/features/remote-docker-test/spec.md).

## Acceptance criteria

- AC-01 local default unchanged, remote engine env cleared, risky user config refused ✓
- AC-02 containers on the remote host, tests pass except network-timing ones (accepted exception) ✓ (see Verification)
- AC-03 / AC-03b no "up to date" reuse across runs and modes ✓
- AC-04 missing host setting stops the run with a pointer ✓
- AC-05 address lives only in `~/.gradle/gradle.properties` ✓
- AC-06 / AC-08 unreachable host or stopped engine stops within 30 s ✓
- AC-07 target banner on every run ✓
- AC-09 `loadTest` / `preReleaseTest` refused with `-Premote` ✓
- AC-10 only `ssh://` addresses accepted ✓

## Design

- Spec: `docs/features/remote-docker-test/spec.md`
- Architecture: `docs/features/remote-docker-test/sad.md`
- Decisions: `docs/features/remote-docker-test/adr/` (0001 to 0003)
- How-to: `docs/remote-docker-test.md`

## Tasks (SDD-Task trailers)

T1 spike (659487d), T2 switch plugin (06d9743), T3 to T7 and T9 validation, tunnel, ping, refusal, TestKit tests, how-to (ee9f3c6), review fixes (0715695, 9eef478). T8 (real-host verification) recorded in a local `verification.md`.

## Verification

- Build-logic tests (Gradle TestKit, fake ssh and engine): 17/17 pass (`./gradlew buildLogicTest`).
- Lint + vet: no separate lint/vet task in this Gradle project.
- Real run on the author's remote host: `test -Premote`, 355 tests, 353 pass; 2 failures in `RedisAllowanceStoreIT` (network timing), accepted by the AC-02 exception. Duration 12m05s.
- Re-run in the ship session: `test -Premote` against the same host, 355 tests, 2 failed (both `RedisAllowanceStoreIT`), 10m14s. Same result as the earlier run.
- AC-06: `-Premote` with an unreachable host stopped in 21 s and named it (`could not open the tunnel to ssh://u@192.0.2.1`).
- AC-04 not re-run here: the per-user host setting is configured on this machine; covered by TestKit tests only.
- Local spot-checks this session: AC-07 banner `Container target: local`; AC-09 `loadTest -Premote` refused ("local-only"); AC-10 `tcp://` refused with the secure-shell message; NFR scan of `git ls-files` finds no host address.

## Operational notes

- Migration: none.
- Config: `-Premote` off by default; per-user `remoteDocker.host`. Rollback: revert the PR.
- Reviewer note: this branch also contains earlier `kafka-request-reply` commits; the feature's own commits start at `35415f2`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
