---
id: T6
title: "Refuse loadTest and preReleaseTest when the remote switch is on"
layer: "wiring"
deps: ["T2"]
blocks: ["T7"]
acs: ["AC-09"]
files_hint: ["gradle/remote-docker.gradle"]
owner: "I.Chupryna"
estimate: "S"
context_budget: "S"
status: "todo"
---

# T6: Refuse loadTest and preReleaseTest when the remote switch is on

## Place in the sequence

- **Blocked by:** T2 (Add the remote switch script plugin and wire the test task environment) · **Blocks:** T7 (Test the build logic with Gradle TestKit using a fake ssh and fake engine) · **Wave:** 3, after its dependencies.
- **Lane:** shares `gradle/remote-docker.gradle` with T2, T3, T4, T5, serialized.

## Why (user story)

> **As a** Developer
> **I want** to be told that the separate load and pre-release commands are local-only
> **So that** I do not read network-distorted timings as product numbers
>
> — `spec.md §4, US-06, verbatim` · full text: [spec.md](../spec.md)

It delivers the local-only message for the separate load commands.

## Inlined context

> - Per-user setting: `remoteDocker.host=ssh://user@host` in `~/.gradle/gradle.properties`. Optional `remoteDocker.socket` (default `/var/run/docker.sock`).
> - The switch is the Gradle property `-Premote`. Logic lives in a script plugin `gradle/remote-docker.gradle` applied from `build.gradle`; the tunnel is a Gradle build service.
> - Remote mode forces the `test` task to always run, and the mode is a task input so a mode change re-runs it.
> - `loadTest` and `preReleaseTest` are refused in remote mode. The performance tests inside `test` run remotely.
> - Banner line at task start: `Container target: local` or `Container target: remote ssh://user@host`.
> - Remote mode sets `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` to `remoteDocker.socket` so Ryuk mounts the remote socket path (to be verified).
>
> — `sad.md §4, ledger of decisions made without asking, abridged` · full text: [sad.md](../sad.md)

> Java 25, Gradle, Spring Boot 4.1.1. Tests start containers through Testcontainers, defined in `TestcontainersConfiguration` (images `apache/kafka:4.2.2`, `redis:7`). The `test` task in `build.gradle` excludes tag `load`; `loadTest` and `preReleaseTest` set `outputs.upToDateWhen { false }`.
>
> — `sad.md §2 Technical + build.gradle, abridged` · full text: [sad.md](../sad.md)

> **Hard rule:** the run never falls back to local silently. Failures stop the build before any test with one message naming the setting or address (SAD §8 error handling: `[remote-docker]` prefix).
>
> — `sad.md §8, Error handling and Logging, abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal — no API surface.

## Acceptance criteria

### AC-09 — domain invariant

> **Given** a Developer who selected the remote switch
> **When** the Developer requests the separate load or pre-release test command
> **Then** that command is refused with a message that these runs are local-only
>
> — `spec.md §5, AC-09, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] In the task graph hook, if remote mode and the graph contains `loadTest` or `preReleaseTest`, fail the build with a message that these runs are local-only (`gradle/remote-docker.gradle`)
- [ ] Leave the performance tests inside `test` untouched (`gradle/remote-docker.gradle`)

## Edge cases

| Case | Behaviour |
|---|---|
| `./gradlew loadTest -Premote` | Refused with the local-only message |
| `./gradlew test preReleaseTest -Premote` | Refused before any task runs |
| `./gradlew test -Premote` | Runs, performance tests inside `test` included |

## Definition of Done

- [ ] both load commands fail fast with the local-only message when `-Premote` is set
- [ ] both still run locally without `-Premote`
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean
