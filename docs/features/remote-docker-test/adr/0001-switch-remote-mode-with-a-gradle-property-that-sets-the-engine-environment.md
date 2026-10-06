---
status: Accepted
owner: "I.Chupryna"
reviewers: ["Tech Lead"]
updated_at: "2026-10-06"
feature_size: "S"
ticket: "n/a"
---

# 0001 — Switch remote mode with a Gradle property that sets the engine environment

- **Status:** Accepted
- **Date:** 2026-10-06
- **Deciders:** I.Chupryna, Claude (design session)

## Context

Testcontainers fixes its container engine once per test JVM from environment variables or a user file, before any test code or Spring profile exists (SAD §2). The spec needs "no switch means local, switch means remote" for the command line and the IDE (spec AC-01, AC-02).

## Decision drivers

- Selection must happen before the test JVM starts (SAD §2).
- Spec §2: default behaviour identical to today, never switch automatically.
- Developer chose a Gradle flag in the interview.

## Considered options

1. **Gradle property `-Premote` sets the test JVM environment** — the build owns the translation.
2. **Developer exports environment variables** — no code, but no named mode and a forgotten export silently goes remote.
3. **Edit `~/.testcontainers.properties`** — global to every project on the machine.
4. **Spring profile** — evaluated after containers have started in static initializers, so it cannot select the engine.

## Decision outcome

**Chosen:** Option 1. It is the only option that gives a named mode, works for the command line and for the IDE through Gradle, and keeps local the default by clearing the variables that point off the machine when the flag is absent, leaving a local engine setting untouched.

## Consequences

**Positive**
- One explicit switch, default unchanged, a mode input that also fixes the stale up-to-date result.

**Negative**
- The IDE must run tests through Gradle, with `-Premote` in its run configuration.
- Ambient settings in the user file may still win over a cleared environment (SAD §11).

**Neutral**
- Switching to option 2 later only removes build logic.

## Links

- Spec: [[../spec.md]]
- SAD: [[../sad.md]] §4
- Related ADR: [[0002-reach-the-remote-engine-through-a-build-managed-ssh-tunnel]]
