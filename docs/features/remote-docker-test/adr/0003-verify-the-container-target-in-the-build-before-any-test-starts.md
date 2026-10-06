---
status: Accepted
owner: "I.Chupryna"
reviewers: ["Tech Lead"]
updated_at: "2026-10-06"
feature_size: "S"
ticket: "n/a"
---

# 0003 — Verify the container target in the build before any test starts

- **Status:** Accepted
- **Date:** 2026-10-06
- **Deciders:** I.Chupryna, Claude (design session)

## Context

A wrong or unreachable remote setup must stop the run within 30 seconds and say why (spec AC-04, AC-06, AC-08), and it must never fall back to local. The shared broker starts in a static initializer, so a failure inside the test JVM surfaces as class-initialization errors across many classes.

## Decision drivers

- Fail fast with one clear message (spec §6: ≤ 30 s).
- No containers on the laptop when remote setup is wrong (QG-2).
- Keep the logic in one place.

## Considered options

1. **Check in the build before the test task** — address, tunnel and an engine answer on the local socket, then print the target.
2. **Check inside the test JVM** with a JUnit session listener — closer to what the tests use, but runs in a second place and needs test-side code.
3. **Let Testcontainers fail on its own** — simplest, but slow and noisy, and the message does not name the remote setting.

## Decision outcome

**Chosen:** Option 1. All remote logic stays in one build script and every failure ends the build before a test JVM exists.

## Consequences

**Positive**
- One message, one place, no test code changes.

**Negative**
- The check cannot see problems only the test JVM would hit, for example blocked container ports (SAD §11).

**Neutral**
- A test-side listener can be added later without changing this decision's outcome.

## Links

- Spec: [[../spec.md]] AC-04, AC-06, AC-08
- SAD: [[../sad.md]] §4
- Related ADR: [[0002-reach-the-remote-engine-through-a-build-managed-ssh-tunnel]]
