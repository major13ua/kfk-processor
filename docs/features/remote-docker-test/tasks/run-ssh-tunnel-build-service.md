---
id: T4
title: "Open and close the ssh tunnel as a build service within the time budget"
layer: "infra"
deps: ["T3"]
blocks: ["T5"]
acs: ["AC-02", "AC-06"]
files_hint: ["gradle/remote-docker.gradle"]
owner: "I.Chupryna"
estimate: "M"
context_budget: "M"
status: "todo"
---

# T4: Open and close the ssh tunnel as a build service within the time budget

## Place in the sequence

- **Blocked by:** T3 (Validate the remote address from the per-user setting and refuse bad forms) · **Blocks:** T5 (Ping the remote engine through the tunnel and set the reaper socket override) · **Wave:** 4, after its dependencies.
- **Lane:** shares `gradle/remote-docker.gradle` with T2, T3, T5, T6, serialized.

## Why (user story)

> **As a** Developer
> **I want** to run the functional test suite on the remote host with one switch
> **So that** my machine is free of container load
>
> — `spec.md §4, US-02, verbatim` · full text: [spec.md](../spec.md)

> **As a** Developer
> **I want** a clear stop when the remote host cannot be reached
> **So that** I know why tests did not run and my machine is not loaded unexpectedly
>
> — `spec.md §4, US-04, verbatim` · full text: [spec.md](../spec.md)

It delivers the channel to the remote host and the clear stop when the host is down.

## Inlined context

> **Channel (ADR-0002):** the build starts `ssh` forwarding the remote engine socket to a local socket and closes it after the run; Testcontainers then talks to a local socket and the host override points container ports at the remote host. Testcontainers has no native `ssh://` support and bundles its own Docker client, so an `ssh://` host cannot be passed to it directly.
>
> — `sad.md §4 choice 2 and §2 Technical, abridged` · full text: [sad.md](../sad.md)

> **Chosen:** a build-managed ssh tunnel to a local socket. Reuses ssh keys, adds no dependency, authentication and encryption come from ssh. Negative: needs the `ssh` command on the laptop, a dropped VPN kills the tunnel, container ports are reached directly over the network.
>
> — `adr/0002, Decision outcome and Consequences, abridged` · full text: [adr/](../adr/)

> **Hard rule:** unreachable-host detection is at most 30 s, measured by a timed run against a stopped host.
>
> — `spec.md §6, NFR row «Unreachable-host detection», verbatim` · full text: [spec.md](../spec.md)

> alt address missing or not ssh: stop, name the missing or refused setting and say it goes in the per-user Gradle properties file.
> alt tunnel does not open within 30 seconds: stop and name the address tried.
> alt engine does not answer within 30 seconds: stop and say the container service on the remote host is not running.
> Happy path: check address, open tunnel, ping engine, print `Container target: remote`, start tests with engine environment set, close tunnel after.
>
> — `sad.md §6, «remote run» flow, abridged` · full text: [sad.md](../sad.md)

> **Risk:** container ports on the remote host may not be reachable from the laptop (VPN or firewall rules); the failure suite picks a free port on the laptop and pins it on the remote host; timing-sensitive tests may flake over the VPN.
>
> — `sad.md §11, risk rows 1, 3, 4, abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

Internal — no API surface.

## Acceptance criteria

### AC-02 — happy path

> **Given** a Developer with a configured, reachable remote host
> **When** the Developer runs the test suite with the remote switch
> **Then** every container starts on the remote host and none on the Developer's machine, the tests reach those containers at the remote host's address, and they pass as they do locally
>
> — `spec.md §5, AC-02, verbatim` · full text: [spec.md](../spec.md)

### AC-06 — error

> **Given** a Developer who selected the remote switch while the remote host is unreachable
> **When** the run starts
> **Then** the run stops within 30 seconds, names the address it tried, and starts no container on the Developer's machine; "unreachable" means the container service does not answer at the configured address, for any connection failure including the host being down
>
> — `spec.md §5, AC-06, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Add a Gradle build service that starts `ssh -N -L <local sock>:<remote socket> user@host` with batch mode and a connect timeout, local socket under `build/remote-docker/` (`gradle/remote-docker.gradle`)
- [ ] Wait for the local socket to appear, within the shared 30 s budget measured from run start; on failure stop and name the address tried (`gradle/remote-docker.gradle`)
- [ ] Close the service after the test task even when tests fail (`gradle/remote-docker.gradle`)
- [ ] Set the test task environment: `DOCKER_HOST` to the local socket, `TESTCONTAINERS_HOST_OVERRIDE` to the remote host name (`gradle/remote-docker.gradle`)

## Edge cases

| Case | Behaviour |
|---|---|
| Host down or VPN off | Stop within 30 s, message names the address, no container on the laptop |
| ssh key rejected | ssh error is shown with the address, run stops |
| `ssh` command missing on the laptop | Stop with a message naming the missing command |
| Tests fail mid-run | Tunnel is still closed |

## Definition of Done

- [ ] a run against a stopped host stops within 30 s naming the address and starts no container locally
- [ ] after a successful or failed run no `ssh` tunnel process remains
- [ ] with a reachable host the tests start containers on the remote host (manual check recorded in T8)
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean
