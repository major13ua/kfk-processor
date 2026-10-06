---
id: T8
title: "Verify remote mode against the real remote host and record findings"
layer: "tests"
deps: ["T7"]
blocks: []
acs: ["AC-02", "AC-06", "AC-08"]
files_hint: ["docs/features/remote-docker-test/verification.md"]
owner: "I.Chupryna"
estimate: "M"
context_budget: "M"
status: "todo"
---

# T8: Verify remote mode against the real remote host and record findings

## Place in the sequence

- **Blocked by:** T7 (Test the build logic with Gradle TestKit using a fake ssh and fake engine) · **Blocks:** none · **Wave:** 7, after its dependencies.
- **Lane:** own lane.

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

It proves the real remote run and records what only a real host can show.

## Inlined context

> **Hard rule:** containers on the Developer's machine in remote mode: 0. Remote suite duration vs local: at most 150% (proposal, unmeasured).
>
> — `spec.md §6, NFR rows, abridged` · full text: [spec.md](../spec.md)

> **Hard rule:** unreachable-host detection is at most 30 s, measured by a timed run against a stopped host.
>
> — `spec.md §6, NFR row «Unreachable-host detection», verbatim` · full text: [spec.md](../spec.md)

> **Risk:** container ports on the remote host may not be reachable from the laptop (VPN or firewall rules); the failure suite picks a free port on the laptop and pins it on the remote host; timing-sensitive tests may flake over the VPN.
>
> — `sad.md §11, risk rows 1, 3, 4, abridged` · full text: [sad.md](../sad.md)

> **Chosen:** a build-managed ssh tunnel to a local socket. Reuses ssh keys, adds no dependency, authentication and encryption come from ssh. Negative: needs the `ssh` command on the laptop, a dropped VPN kills the tunnel, container ports are reached directly over the network.
>
> — `adr/0002, Decision outcome and Consequences, abridged` · full text: [adr/](../adr/)

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

### AC-08 — cross-context

> **Given** a Developer who selected the remote switch while the remote host accepts the Developer's secure-shell connection but its container service is not running
> **When** the run starts
> **Then** within 30 seconds the run stops, tells the Developer that the container service on the remote host is not running, and starts no container on the Developer's machine
>
> — `spec.md §5, AC-08, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Run `./gradlew test -Premote` against the real remote host and record: containers on the laptop (expected 0), pass or fail per test class, total duration vs a local run (`docs/features/remote-docker-test/verification.md`)
- [ ] Time a run with the host stopped and with the engine stopped (30 s limit) (`docs/features/remote-docker-test/verification.md`)
- [ ] Record whether container ports are reachable and which failure-suite and timing tests fail only remotely (`docs/features/remote-docker-test/verification.md`)
- [ ] Update the open questions in `spec.md §8` with the measured duration and the remote-only failures (`docs/features/remote-docker-test/spec.md`)

## Edge cases

| Case | Behaviour |
|---|---|
| Container ports blocked by the network | Recorded as a finding with the required rules |
| Failure suite fixes a host port chosen on the laptop | Recorded, follow-up task or open question created |
| Remote run slower than 150% | Recorded against the proposal in `spec.md §8` |

## Definition of Done

- [ ] `verification.md` shows 0 containers on the laptop, the timings for both failure modes, and the remote vs local duration
- [ ] remote-only failures are listed with a decision (fix, per-mode threshold, or skip) in `spec.md §8`
- [ ] every Hard Rule inlined above still holds
