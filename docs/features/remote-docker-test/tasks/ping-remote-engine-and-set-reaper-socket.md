---
id: T5
title: "Ping the remote engine through the tunnel and set the reaper socket override"
layer: "infra"
deps: ["T4", "T1"]
blocks: ["T7", "T9"]
acs: ["AC-08"]
files_hint: ["gradle/remote-docker.gradle"]
owner: "I.Chupryna"
estimate: "M"
context_budget: "M"
status: "todo"
---

# T5: Ping the remote engine through the tunnel and set the reaper socket override

## Place in the sequence

- **Blocked by:** T4 (Open and close the ssh tunnel as a build service within the time budget), T1 (Check engine settings precedence and Ryuk socket handling, record the result) · **Blocks:** T7 (Test the build logic with Gradle TestKit using a fake ssh and fake engine), T9 (Write the developer how-to for remote mode) · **Wave:** 5, after its dependencies.
- **Lane:** shares `gradle/remote-docker.gradle` with T2, T3, T4, T6, serialized.

## Why (user story)

> **As a** Developer
> **I want** a clear stop when the remote host cannot be reached
> **So that** I know why tests did not run and my machine is not loaded unexpectedly
>
> — `spec.md §4, US-04, verbatim` · full text: [spec.md](../spec.md)

It delivers the clear stop when the container service is not running and keeps cleanup working.

## Inlined context

> **Check (ADR-0003):** the build checks the address, the tunnel and the engine answer, then prints the target, all before the first test JVM. One 30 second budget, measured from the start of the run, covers validation, tunnel start and engine ping together.
>
> — `sad.md §4 choice 3 and ledger, abridged` · full text: [sad.md](../sad.md)

> **Chosen:** check in the build before the test task. All remote logic stays in one build script and every failure ends the build before a test JVM exists.
>
> — `adr/0003, Decision outcome, abridged` · full text: [adr/](../adr/)

> alt address missing or not ssh: stop, name the missing or refused setting and say it goes in the per-user Gradle properties file.
> alt tunnel does not open within 30 seconds: stop and name the address tried.
> alt engine does not answer within 30 seconds: stop and say the container service on the remote host is not running.
> Happy path: check address, open tunnel, ping engine, print `Container target: remote`, start tests with engine environment set, close tunnel after.
>
> — `sad.md §6, «remote run» flow, abridged` · full text: [sad.md](../sad.md)

> **Hard rule:** unreachable-host detection is at most 30 s, measured by a timed run against a stopped host.
>
> — `spec.md §6, NFR row «Unreachable-host detection», verbatim` · full text: [spec.md](../spec.md)

> - Per-user setting: `remoteDocker.host=ssh://user@host` in `~/.gradle/gradle.properties`. Optional `remoteDocker.socket` (default `/var/run/docker.sock`).
> - The switch is the Gradle property `-Premote`. Logic lives in a script plugin `gradle/remote-docker.gradle` applied from `build.gradle`; the tunnel is a Gradle build service.
> - Remote mode forces the `test` task to always run, and the mode is a task input so a mode change re-runs it.
> - `loadTest` and `preReleaseTest` are refused in remote mode. The performance tests inside `test` run remotely.
> - Banner line at task start: `Container target: local` or `Container target: remote ssh://user@host`.
> - Remote mode sets `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` to `remoteDocker.socket` so Ryuk mounts the remote socket path (to be verified).
>
> — `sad.md §4, ledger of decisions made without asking, abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal — no API surface.

## Acceptance criteria

### AC-08 — cross-context

> **Given** a Developer who selected the remote switch while the remote host accepts the Developer's secure-shell connection but its container service is not running
> **When** the run starts
> **Then** within 30 seconds the run stops, tells the Developer that the container service on the remote host is not running, and starts no container on the Developer's machine
>
> — `spec.md §5, AC-08, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] After the tunnel is up, send an engine ping over the local unix socket from the build and require an answer within the remaining 30 s budget (`gradle/remote-docker.gradle`)
- [ ] No answer: stop and say the container service on the remote host is not running, naming the address (`gradle/remote-docker.gradle`)
- [ ] Set `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` to `remoteDocker.socket` (default `/var/run/docker.sock`) per the T1 finding (`gradle/remote-docker.gradle`)

## Edge cases

| Case | Behaviour |
|---|---|
| ssh works but the engine is stopped | Stop within 30 s, message says the container service is not running |
| Engine answers slowly | Waits only within the shared 30 s budget |
| Custom socket path on the remote host | `remoteDocker.socket` is used for the forward and the reaper override |

## Definition of Done

- [ ] with the engine stopped on the remote host the run stops within 30 s with the container-service message
- [ ] with the engine running the ping succeeds and the target banner shows the remote address
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean
