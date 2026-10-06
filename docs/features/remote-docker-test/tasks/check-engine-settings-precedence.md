---
id: T1
title: "Check engine settings precedence and Ryuk socket handling, record the result"
layer: "docs"
deps: []
blocks: ["T2", "T5"]
acs: ["AC-01"]
files_hint: ["docs/features/remote-docker-test/spike-findings.md"]
owner: "I.Chupryna"
estimate: "S"
context_budget: "S"
status: "todo"
---

# T1: Check engine settings precedence and Ryuk socket handling, record the result

## Place in the sequence

- **Blocked by:** none · **Blocks:** T2 (Add the remote switch script plugin and wire the test task environment), T5 (Ping the remote engine through the tunnel and set the reaper socket override) · **Wave:** 1, after its dependencies.
- **Lane:** own lane.

## Why (user story)

> **As a** Developer
> **I want** tests to run on my own machine when I select nothing
> **So that** existing habits and setups keep working unchanged
>
> — `spec.md §4, US-01, verbatim` · full text: [spec.md](../spec.md)

It removes the unknown that decides whether a no-switch run can be guaranteed local.

## Inlined context

> **Risk (open, due before `sdd:tasks`):** ambient settings in `~/.testcontainers.properties` may send a no-switch run remote even with a cleared environment. Resolve by checking Testcontainers' configuration precedence: either force local, or refuse with a message naming the file (which needs spec AC-01 amended).
>
> — `sad.md §11, risk row 2 and open architectural decision, abridged` · full text: [sad.md](../sad.md)

> **Committed approach (ADR-0001):** the Gradle flag `-Premote` makes the build set the engine environment variables for the test JVM, because Testcontainers fixes its engine before any test code runs. Without the flag the build clears the variables that point off the machine (a remote engine address or a host override) and leaves a local engine setting, such as a Colima or Podman socket, untouched, so a no-switch run is local.
>
> — `sad.md §4, strategic choice 1, abridged` · full text: [sad.md](../sad.md)

> **Channel (ADR-0002):** the build starts `ssh` forwarding the remote engine socket to a local socket and closes it after the run; Testcontainers then talks to a local socket and the host override points container ports at the remote host. Testcontainers has no native `ssh://` support and bundles its own Docker client, so an `ssh://` host cannot be passed to it directly.
>
> — `sad.md §4 choice 2 and §2 Technical, abridged` · full text: [sad.md](../sad.md)

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

## Checklist

- [ ] Read Testcontainers configuration docs and, with a throwaway test or `testcontainers.properties` experiment, find whether `docker.host` or `tc.host` in `~/.testcontainers.properties` override `DOCKER_HOST` and `TESTCONTAINERS_HOST_OVERRIDE` set in the environment (`docs/features/remote-docker-test/spike-findings.md`)
- [ ] Find whether Ryuk needs `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` when `DOCKER_HOST` is a forwarded local unix socket (`docs/features/remote-docker-test/spike-findings.md`)
- [ ] Write the outcome as one of: force local is possible (how) / must refuse with a message naming the file (spec AC-01 then needs an amendment) (`docs/features/remote-docker-test/spike-findings.md`)
- [ ] Close the matching open decision row in `sad.md §11` and, if needed, add an ADR or spec amendment note (`docs/features/remote-docker-test/sad.md`)

## Edge cases

| Case | Behaviour |
|---|---|
| The user file sets a host and the environment does not | Findings state which wins, with the exact property names tried |
| Ryuk fails to start with a forwarded local socket | Findings state the working socket override value |

## Definition of Done

- [ ] `spike-findings.md` names the precedence result and the Ryuk socket value, each with the command or test used
- [ ] `sad.md §11` open architectural decision row is closed or re-pointed to a spec amendment
- [ ] every Hard Rule inlined above still holds
