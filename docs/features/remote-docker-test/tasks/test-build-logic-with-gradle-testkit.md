---
id: T7
title: "Test the build logic with Gradle TestKit using a fake ssh and fake engine"
layer: "tests"
deps: ["T3", "T5", "T6"]
blocks: ["T8"]
acs: ["AC-01", "AC-03", "AC-03b", "AC-04", "AC-05", "AC-06", "AC-07", "AC-08", "AC-09", "AC-10"]
files_hint: ["src/test/java/xme/common/kfkprocessor/build/", "build.gradle"]
owner: "I.Chupryna"
estimate: "M"
context_budget: "M"
status: "todo"
---

# T7: Test the build logic with Gradle TestKit using a fake ssh and fake engine

## Place in the sequence

- **Blocked by:** T3 (Validate the remote address from the per-user setting and refuse bad forms), T5 (Ping the remote engine through the tunnel and set the reaper socket override), T6 (Refuse loadTest and preReleaseTest when the remote switch is on) · **Blocks:** T8 (Verify remote mode against the real remote host and record findings) · **Wave:** 6, after its dependencies.
- **Lane:** own lane.

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
> **I want** to set my remote host address once per machine, in a per-user setting outside the project
> **So that** I do not share or commit it and need not retype it
>
> — `spec.md §4, US-03, verbatim` · full text: [spec.md](../spec.md)

> **As a** Developer
> **I want** a clear stop when the remote host cannot be reached
> **So that** I know why tests did not run and my machine is not loaded unexpectedly
>
> — `spec.md §4, US-04, verbatim` · full text: [spec.md](../spec.md)

> **As a** Developer
> **I want** every run to state whether it used my machine or the remote host
> **So that** I can trust where the load went
>
> — `spec.md §4, US-05, verbatim` · full text: [spec.md](../spec.md)

> **As a** Developer
> **I want** to be told that the separate load and pre-release commands are local-only
> **So that** I do not read network-distorted timings as product numbers
>
> — `spec.md §4, US-06, verbatim` · full text: [spec.md](../spec.md)

> **As a** Developer
> **I want** no run to ever reach my remote host over an unauthenticated connection
> **So that** my remote machine cannot be taken over through the private network
>
> — `spec.md §4, US-07, verbatim` · full text: [spec.md](../spec.md)

It proves the build behaviour for every criterion that does not need a real remote host.

## Inlined context

> Java 25, Gradle, Spring Boot 4.1.1. Tests start containers through Testcontainers, defined in `TestcontainersConfiguration` (images `apache/kafka:4.2.2`, `redis:7`). The `test` task in `build.gradle` excludes tag `load`; `loadTest` and `preReleaseTest` set `outputs.upToDateWhen { false }`.
>
> — `sad.md §2 Technical + build.gradle, abridged` · full text: [sad.md](../sad.md)

> **Check (ADR-0003):** the build checks the address, the tunnel and the engine answer, then prints the target, all before the first test JVM. One 30 second budget, measured from the start of the run, covers validation, tunnel start and engine ping together.
>
> — `sad.md §4 choice 3 and ledger, abridged` · full text: [sad.md](../sad.md)

> **Hard rule:** unreachable-host detection is at most 30 s, measured by a timed run against a stopped host.
>
> — `spec.md §6, NFR row «Unreachable-host detection», verbatim` · full text: [spec.md](../spec.md)

> **Hard rule:** 0 remote host addresses in the project (scan of tracked files and the working tree); the address lives only in the per-user Gradle properties file.
>
> — `spec.md §6, NFR row «Remote host addresses in the project», abridged` · full text: [spec.md](../spec.md)

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

### AC-06 — error

> **Given** a Developer who selected the remote switch while the remote host is unreachable
> **When** the run starts
> **Then** the run stops within 30 seconds, names the address it tried, and starts no container on the Developer's machine; "unreachable" means the container service does not answer at the configured address, for any connection failure including the host being down
>
> — `spec.md §5, AC-06, verbatim` · full text: [spec.md](../spec.md)

### AC-07 — happy path

> **Given** a Developer who starts a test run in either mode
> **When** the run begins
> **Then** it states whether it uses the Developer's own machine or which remote host
>
> — `spec.md §5, AC-07, verbatim` · full text: [spec.md](../spec.md)

### AC-08 — cross-context

> **Given** a Developer who selected the remote switch while the remote host accepts the Developer's secure-shell connection but its container service is not running
> **When** the run starts
> **Then** within 30 seconds the run stops, tells the Developer that the container service on the remote host is not running, and starts no container on the Developer's machine
>
> — `spec.md §5, AC-08, verbatim` · full text: [spec.md](../spec.md)

### AC-09 — domain invariant

> **Given** a Developer who selected the remote switch
> **When** the Developer requests the separate load or pre-release test command
> **Then** that command is refused with a message that these runs are local-only
>
> — `spec.md §5, AC-09, verbatim` · full text: [spec.md](../spec.md)

### AC-10 — authorization

> **Given** a remote host address that is not a secure-shell address
> **When** the Developer selects remote mode
> **Then** the run is refused and the Developer is told that a secure-shell address is required
>
> — `spec.md §5, AC-10, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Add a Gradle TestKit dependency and a small test project using `gradle/remote-docker.gradle` (`build.gradle`, `src/test/java/xme/common/kfkprocessor/build/`)
- [ ] Use a fake `ssh` executable on the path and a fake local socket server to simulate: tunnel ok, tunnel never opens, engine silent (`src/test/java/xme/common/kfkprocessor/build/`)
- [ ] Assert: banner text in both modes, missing address message, non-ssh refusal, load commands refused, up-to-date behaviour across mode changes, 30 s limit (use a shortened limit property) (`src/test/java/xme/common/kfkprocessor/build/`)
- [ ] Assert the repository scan finds no address (`src/test/java/xme/common/kfkprocessor/build/`)

## Edge cases

| Case | Behaviour |
|---|---|
| Fake ssh exits non-zero | Build fails with the address named |
| Fake socket accepts but never answers | Stop with the container-service message |
| No `-Premote` | No tunnel is started |

## Definition of Done

- [ ] TestKit tests cover AC-01, AC-03, AC-03b, AC-04 to AC-10 and pass
- [ ] the suite runs in local mode without real containers or a real remote host
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean
