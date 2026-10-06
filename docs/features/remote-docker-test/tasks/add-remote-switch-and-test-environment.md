---
id: T2
title: "Add the remote switch script plugin and wire the test task environment"
layer: "wiring"
deps: ["T1"]
blocks: ["T3", "T6"]
acs: ["AC-01", "AC-03", "AC-03b", "AC-07"]
files_hint: ["gradle/remote-docker.gradle", "build.gradle"]
owner: "I.Chupryna"
estimate: "M"
context_budget: "M"
status: "todo"
---

# T2: Add the remote switch script plugin and wire the test task environment

## Place in the sequence

- **Blocked by:** T1 (Check engine settings precedence and Ryuk socket handling, record the result) · **Blocks:** T3 (Validate the remote address from the per-user setting and refuse bad forms), T6 (Refuse loadTest and preReleaseTest when the remote switch is on) · **Wave:** 2, after its dependencies.
- **Lane:** shares `gradle/remote-docker.gradle` with T3, T4, T5, T6, serialized.

## Why (user story)

> **As a** Developer
> **I want** tests to run on my own machine when I select nothing
> **So that** existing habits and setups keep working unchanged
>
> — `spec.md §4, US-01, verbatim` · full text: [spec.md](../spec.md)

> **As a** Developer
> **I want** to run the functional test suite on the remote host with one switch
> **So that** my machine is free of container load
>
> — `spec.md §4, US-02, verbatim` · full text: [spec.md](../spec.md)

> **As a** Developer
> **I want** every run to state whether it used my machine or the remote host
> **So that** I can trust where the load went
>
> — `spec.md §4, US-05, verbatim` · full text: [spec.md](../spec.md)

It delivers the switch itself, the default local behaviour, the banner and the re-run rule.

## Inlined context

> Java 25, Gradle, Spring Boot 4.1.1. Tests start containers through Testcontainers, defined in `TestcontainersConfiguration` (images `apache/kafka:4.2.2`, `redis:7`). The `test` task in `build.gradle` excludes tag `load`; `loadTest` and `preReleaseTest` set `outputs.upToDateWhen { false }`.
>
> — `sad.md §2 Technical + build.gradle, abridged` · full text: [sad.md](../sad.md)

> **Committed approach (ADR-0001):** the Gradle flag `-Premote` makes the build set the engine environment variables for the test JVM, because Testcontainers fixes its engine before any test code runs. Without the flag the build clears the variables that point off the machine (a remote engine address or a host override) and leaves a local engine setting, such as a Colima or Podman socket, untouched, so a no-switch run is local.
>
> — `sad.md §4, strategic choice 1, abridged` · full text: [sad.md](../sad.md)

> **Chosen:** a Gradle property `-Premote` sets the test JVM environment. The only option that gives a named mode, works for the command line and for the IDE through Gradle, and keeps local the default.
>
> — `adr/0001, Decision outcome, abridged` · full text: [adr/](../adr/)

> - Per-user setting: `remoteDocker.host=ssh://user@host` in `~/.gradle/gradle.properties`. Optional `remoteDocker.socket` (default `/var/run/docker.sock`).
> - The switch is the Gradle property `-Premote`. Logic lives in a script plugin `gradle/remote-docker.gradle` applied from `build.gradle`; the tunnel is a Gradle build service.
> - Remote mode forces the `test` task to always run, and the mode is a task input so a mode change re-runs it.
> - `loadTest` and `preReleaseTest` are refused in remote mode. The performance tests inside `test` run remotely.
> - Banner line at task start: `Container target: local` or `Container target: remote ssh://user@host`.
> - Remote mode sets `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` to `remoteDocker.socket` so Ryuk mounts the remote socket path (to be verified).
>
> — `sad.md §4, ledger of decisions made without asking, abridged` · full text: [sad.md](../sad.md)

> **Hard rule:** the run never falls back to local silently. Failures stop the build before any test with one message naming the setting or address (SAD §8 error handling: `[remote-docker]` prefix).
>
> — `sad.md §8, Error handling and Logging, abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal — no API surface.

## Acceptance criteria

### AC-01 — happy path

> **Given** a Developer who selected no switch, whatever container-host setting exists in the Developer's environment or user-level tool configuration
> **When** the Developer runs the test suite
> **Then** all containers start on the Developer's own machine and tests behave as before
>
> — `spec.md §5, AC-01, verbatim` · full text: [spec.md](../spec.md)

### AC-03 — domain invariant violation

> **Given** a Developer whose earlier run, in either mode, finished successfully and nothing has changed
> **When** the Developer runs the test suite with the remote switch
> **Then** the tests run again and the run is never reported as already done
>
> — `spec.md §5, AC-03, verbatim` · full text: [spec.md](../spec.md)

### AC-03b — domain invariant violation

> **Given** a Developer whose earlier remote run finished successfully and nothing has changed
> **When** the Developer runs the test suite without the switch
> **Then** the tests run again on the Developer's own machine and the earlier remote result is not reused
>
> — `spec.md §5, AC-03b, verbatim` · full text: [spec.md](../spec.md)

### AC-07 — happy path

> **Given** a Developer who starts a test run in either mode
> **When** the run begins
> **Then** it states whether it uses the Developer's own machine or which remote host
>
> — `spec.md §5, AC-07, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `gradle/remote-docker.gradle` and apply it from `build.gradle`; read `-Premote` and expose `remoteMode` (`gradle/remote-docker.gradle`, `build.gradle`)
- [ ] Local mode: remove the engine variables that point off the machine (remote engine address, host override, socket override) from the `test` task environment and leave a local socket setting alone, applying the T1 finding (`gradle/remote-docker.gradle`)
- [ ] Remote mode: set the engine environment for the `test` task (final values come from T4 and T5; here only the plumbing and a placeholder property) (`gradle/remote-docker.gradle`)
- [ ] Add the mode as a `test` task input and set `outputs.upToDateWhen { false }` when remote (`gradle/remote-docker.gradle`)
- [ ] Print the banner `Container target: local` or `Container target: remote <address>` with prefix `[remote-docker]` at the start of the `test` task (`gradle/remote-docker.gradle`)

## Edge cases

| Case | Behaviour |
|---|---|
| No switch, user environment has a local socket `DOCKER_HOST` | Left untouched, containers start locally |
| No switch, environment names an off-machine host | Cleared for the test JVM, containers start locally |
| Local run right after a remote run, nothing changed | Test task re-runs because the mode input changed |
| Second remote run, nothing changed | Test task re-runs, never reported up to date |

## Definition of Done

- [ ] a no-switch run prints `Container target: local` and starts containers on the local engine
- [ ] a remote run directly after a local run, and the reverse, both execute the tests again
- [ ] `./gradlew test -PskipDockerTests` still works as before
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean
