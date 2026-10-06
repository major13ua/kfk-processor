---
id: T9
title: "Write the developer how-to for remote mode"
layer: "docs"
deps: ["T5"]
blocks: []
acs: ["AC-04", "AC-10"]
files_hint: ["docs/remote-docker-test.md"]
owner: "I.Chupryna"
estimate: "S"
context_budget: "M"
status: "todo"
---

# T9: Write the developer how-to for remote mode

## Place in the sequence

- **Blocked by:** T5 (Ping the remote engine through the tunnel and set the reaper socket override) · **Blocks:** none · **Wave:** 6, after its dependencies.
- **Lane:** own lane.

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

It lets a Developer set up and use remote mode without asking anyone.

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

> **Risk:** container ports on the remote host may not be reachable from the laptop (VPN or firewall rules); the failure suite picks a free port on the laptop and pins it on the remote host; timing-sensitive tests may flake over the VPN.
>
> — `sad.md §11, risk rows 1, 3, 4, abridged` · full text: [sad.md](../sad.md)

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

### AC-10 — authorization

> **Given** a remote host address that is not a secure-shell address
> **When** the Developer selects remote mode
> **Then** the run is refused and the Developer is told that a secure-shell address is required
>
> — `spec.md §5, AC-10, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Document one-time setup: ssh key access to the remote host, engine running there, `remoteDocker.host` (and optional `remoteDocker.socket`) in `~/.gradle/gradle.properties` (`docs/remote-docker-test.md`)
- [ ] Document usage: `./gradlew test -Premote`, the IDE Gradle run configuration with `-Premote`, the banner, the load commands staying local (`docs/remote-docker-test.md`)
- [ ] Document limits and the network rules the remote host needs for container ports, and how to prune leftover containers by hand (`docs/remote-docker-test.md`)

## Edge cases

| Case | Behaviour |
|---|---|
| Developer has no ssh key set up | How-to names the one-time step first |
| IDE runs tests directly, not through Gradle | How-to says the switch has no effect there and how to switch the IDE to Gradle |

## Definition of Done

- [ ] a new Developer can follow the how-to to a first remote run (setup time recorded against the 15 minute KPI in T8)
- [ ] the how-to names the setting, the file and the secure-shell address form
- [ ] every Hard Rule inlined above still holds
