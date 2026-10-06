---
id: T3
title: "Validate the remote address from the per-user setting and refuse bad forms"
layer: "wiring"
deps: ["T2"]
blocks: ["T4", "T7"]
acs: ["AC-04", "AC-05", "AC-10"]
files_hint: ["gradle/remote-docker.gradle"]
owner: "I.Chupryna"
estimate: "M"
context_budget: "M"
status: "todo"
---

# T3: Validate the remote address from the per-user setting and refuse bad forms

## Place in the sequence

- **Blocked by:** T2 (Add the remote switch script plugin and wire the test task environment) · **Blocks:** T4 (Open and close the ssh tunnel as a build service within the time budget), T7 (Test the build logic with Gradle TestKit using a fake ssh and fake engine) · **Wave:** 3, after its dependencies.
- **Lane:** shares `gradle/remote-docker.gradle` with T2, T4, T5, T6, serialized.

## Why (user story)

> **As a** Developer
> **I want** to set my remote host address once per machine, in a per-user setting outside the project
> **So that** I do not share or commit it and need not retype it
>
> — `spec.md §4, US-03, verbatim` · full text: [spec.md](../spec.md)

> **As a** Developer
> **I want** no run to ever reach my remote host over an unauthenticated connection
> **So that** my remote machine cannot be taken over through the private network
>
> — `spec.md §4, US-07, verbatim` · full text: [spec.md](../spec.md)

It delivers the private, validated address and the refusal of unauthenticated forms.

## Inlined context

> - Per-user setting: `remoteDocker.host=ssh://user@host` in `~/.gradle/gradle.properties`. Optional `remoteDocker.socket` (default `/var/run/docker.sock`).
> - The switch is the Gradle property `-Premote`. Logic lives in a script plugin `gradle/remote-docker.gradle` applied from `build.gradle`; the tunnel is a Gradle build service.
> - Remote mode forces the `test` task to always run, and the mode is a task input so a mode change re-runs it.
> - `loadTest` and `preReleaseTest` are refused in remote mode. The performance tests inside `test` run remotely.
> - Banner line at task start: `Container target: local` or `Container target: remote ssh://user@host`.
> - Remote mode sets `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` to `remoteDocker.socket` so Ryuk mounts the remote socket path (to be verified).
>
> — `sad.md §4, ledger of decisions made without asking, abridged` · full text: [sad.md](../sad.md)

> **Chosen:** a build-managed ssh tunnel to a local socket. Reuses ssh keys, adds no dependency, authentication and encryption come from ssh. Negative: needs the `ssh` command on the laptop, a dropped VPN kills the tunnel, container ports are reached directly over the network.
>
> — `adr/0002, Decision outcome and Consequences, abridged` · full text: [adr/](../adr/)

> **Hard rule:** 0 remote host addresses in the project (scan of tracked files and the working tree); the address lives only in the per-user Gradle properties file.
>
> — `spec.md §6, NFR row «Remote host addresses in the project», abridged` · full text: [spec.md](../spec.md)

> **Hard rule:** the run never falls back to local silently. Failures stop the build before any test with one message naming the setting or address (SAD §8 error handling: `[remote-docker]` prefix).
>
> — `sad.md §8, Error handling and Logging, abridged` · full text: [sad.md](../sad.md)

> alt address missing or not ssh: stop, name the missing or refused setting and say it goes in the per-user Gradle properties file.
> alt tunnel does not open within 30 seconds: stop and name the address tried.
> alt engine does not answer within 30 seconds: stop and say the container service on the remote host is not running.
> Happy path: check address, open tunnel, ping engine, print `Container target: remote`, start tests with engine environment set, close tunnel after.
>
> — `sad.md §6, «remote run» flow, abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal — no API surface.

## Acceptance criteria

### AC-04 — error

> **Given** a Developer who selected the remote switch but has no host address configured
> **When** the Developer starts the run
> **Then** the run stops before any container starts and tells the Developer which setting is missing and where to set it
>
> — `spec.md §5, AC-04, verbatim` · full text: [spec.md](../spec.md)

### AC-05 — domain invariant

> **Given** a Developer who has configured a remote host address once in a per-user setting outside the project
> **When** the Developer shares or commits project changes
> **Then** the address is not part of the shared project files, and the same setting serves every copy of the project on that machine
>
> — `spec.md §5, AC-05, verbatim` · full text: [spec.md](../spec.md)

### AC-10 — authorization

> **Given** a remote host address that is not a secure-shell address
> **When** the Developer selects remote mode
> **Then** the run is refused and the Developer is told that a secure-shell address is required
>
> — `spec.md §5, AC-10, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Read `remoteDocker.host` from the Gradle properties (per-user `~/.gradle/gradle.properties`), never from project files (`gradle/remote-docker.gradle`)
- [ ] If missing in remote mode: stop the build before any container starts with a message naming `remoteDocker.host` and `~/.gradle/gradle.properties` (`gradle/remote-docker.gradle`)
- [ ] If the value is not an `ssh://user@host` address (any other scheme, empty host): stop with a message that a secure-shell address is required (`gradle/remote-docker.gradle`)
- [ ] Add a check that no tracked file or working tree file contains a `remoteDocker.host=` value (`gradle/remote-docker.gradle`, or a verification task)

## Edge cases

| Case | Behaviour |
|---|---|
| Property missing | Stop before any container, message names the setting and the file |
| `tcp://host:2375` or `http://host` | Refused, message says a secure-shell address is required |
| `ssh://` without a host | Refused as not a valid secure-shell address |
| Address placed in a project `gradle.properties` | Repository scan check fails |

## Definition of Done

- [ ] missing property stops the run with the setting and file named, before any container
- [ ] a non-ssh address is refused with the ssh message
- [ ] repository scan finds 0 addresses in tracked files and the working tree
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean
