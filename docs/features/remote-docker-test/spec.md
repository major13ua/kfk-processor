---
status: Draft
owner: "I.Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-06"
feature_size: "S"
---

# Spec — remote-docker-test

> **Glossary:** [CONTEXT](./CONTEXT.md)
> **Reference module / docs / channels used:** `docs/idea-brief.md` and the existing test setup in the repository.

## 1. Context

Developers of this repository run integration tests that start throwaway broker and cache containers on their own Mac. This consumes its memory and processor capacity and slows the machine down while tests run. A stronger remote machine is available to each Developer but there is no way to point the tests at it.

There is no external trigger. The motivation is relieving the developer machine, so this is a convenience rather than a deadline-driven need.

The committed approach is a named remote mode selected by one build switch. Without the switch, behaviour is unchanged and everything runs on the Developer's machine. In remote mode all containers run on a remote host, each Developer's own remote machine, whose address the Developer keeps privately, reached only over an authenticated channel. If the host is unreachable the run stops and never falls back to local. This builds on the remote-host configuration the existing container test tooling already provides. Market research found no ready option combining a local/remote switch with a self-hosted host, only paid cloud services. The sharpest failure vector found is a remote engine opened without authentication, which gives root-level control to anyone on the private network.

¶4 Traceability: sources are `docs/idea-brief.md` and the test setup already in the repository.

## 2. Goals

- A Developer can run the normal test command (the functional suite, which includes the performance tests that already run inside it) with zero containers on their own machine, using one switch.
- Default behaviour with no switch is identical to today.
- It is always clear which machine ran the tests, and there is no silent fallback.

## 3. Non-goals

- CI setup: CI keeps what it uses today, and this feature covers developer machines only.
- Provisioning or administering the remote host: each Developer sets up their own remote machine.
- Multiple remote hosts, shared hosts or load balancing: each Developer uses one remote machine of their own, to keep the change small.
- Remote runs of the separate load and pre-release test commands: they stay local-only, because network latency distorts timings. Performance tests inside the normal test command do run remotely (see §8).
- Automatic cleanup of leftover containers on the remote host: undecided, tracked in §8.

## 4. User stories

### US-01: Run tests locally by default

**As a** Developer
**I want** tests to run on my own machine when I select nothing
**So that** existing habits and setups keep working unchanged

### US-02: Run tests on remote host

**As a** Developer
**I want** to run the functional test suite on the remote host with one switch
**So that** my machine is free of container load

### US-03: Keep host address private

**As a** Developer
**I want** to set my remote host address once per machine, in a per-user setting outside the project
**So that** I do not share or commit it and need not retype it

### US-04: Stop clearly when host unreachable

**As a** Developer
**I want** a clear stop when the remote host cannot be reached
**So that** I know why tests did not run and my machine is not loaded unexpectedly

### US-05: See which machine ran the tests

**As a** Developer
**I want** every run to state whether it used my machine or the remote host
**So that** I can trust where the load went

### US-06: Know load runs are local-only

**As a** Developer
**I want** to be told that the separate load and pre-release commands are local-only
**So that** I do not read network-distorted timings as product numbers

### US-07: Use only authenticated connections

**As a** Developer
**I want** no run to ever reach my remote host over an unauthenticated connection
**So that** my remote machine cannot be taken over through the private network

## 5. Acceptance criteria

### AC-01 (US-01) — happy path

**Given** a Developer who selected no switch
**When** the Developer runs the test suite
**Then** all containers start on the Developer's own machine and tests behave as before; engine settings in the Developer's environment are cleared for the run, and a setting in the user-level tool configuration file that points to another machine stops the run with a message naming the file and the setting (no silent remote run)

### AC-02 (US-02) — happy path

**Given** a Developer with a configured, reachable remote host
**When** the Developer runs the test suite with the remote switch
**Then** every container starts on the remote host and none on the Developer's machine, the tests reach those containers at the remote host's address, and they pass as they do locally

### AC-03 (US-02) — domain invariant violation

**Given** a Developer whose earlier run, in either mode, finished successfully and nothing has changed
**When** the Developer runs the test suite with the remote switch
**Then** the tests run again and the run is never reported as already done

### AC-03b (US-02) — domain invariant violation

**Given** a Developer whose earlier remote run finished successfully and nothing has changed
**When** the Developer runs the test suite without the switch
**Then** the tests run again on the Developer's own machine and the earlier remote result is not reused

### AC-04 (US-03) — error

**Given** a Developer who selected the remote switch but has no host address configured
**When** the Developer starts the run
**Then** the run stops before any container starts and tells the Developer which setting is missing and where to set it

### AC-05 (US-03) — domain invariant

**Given** a Developer who has configured a remote host address once in a per-user setting outside the project
**When** the Developer shares or commits project changes
**Then** the address is not part of the shared project files, and the same setting serves every copy of the project on that machine

### AC-06 (US-04) — error

**Given** a Developer who selected the remote switch while the remote host is unreachable
**When** the run starts
**Then** the run stops within 30 seconds, names the address it tried, and starts no container on the Developer's machine; "unreachable" means the container service does not answer at the configured address, for any connection failure including the host being down

### AC-07 (US-05) — happy path

**Given** a Developer who starts a test run in either mode
**When** the run begins
**Then** it states whether it uses the Developer's own machine or which remote host

### AC-08 (US-04) — cross-context

**Given** a Developer who selected the remote switch while the remote host accepts the Developer's secure-shell connection but its container service is not running
**When** the run starts
**Then** within 30 seconds the run stops, tells the Developer that the container service on the remote host is not running, and starts no container on the Developer's machine

### AC-09 (US-06) — domain invariant

**Given** a Developer who selected the remote switch
**When** the Developer requests the separate load or pre-release test command
**Then** that command is refused with a message that these runs are local-only

### AC-10 (US-07) — authorization

**Given** a remote host address that is not a secure-shell address
**When** the Developer selects remote mode
**Then** the run is refused and the Developer is told that a secure-shell address is required

## 6. Non-functional requirements

| Aspect | Target | Measurement |
|---|---|---|
| Unreachable-host detection | ≤ 30 s | timed run against a stopped host |
| Containers on the Developer's machine in remote mode | 0 | container list on the machine after a run |
| Remote suite duration vs local | ≤ 150% (proposal, unmeasured; see §8) | same suite started from the same Developer machine, local mode vs remote mode |
| Remote host addresses in the project | 0 | scan of tracked files and the working tree |

## 6.1 Security / privacy

- **Data classification:** internal, because test data is synthetic but the remote host is an internal asset.
- **Personal data touched:** none.
- **AuthZ/AuthN impact:** new trust boundary, since access to the remote host's container engine is equivalent to full control of that machine; the project only ever connects over a secure-shell channel (AC-10). Securing the remote machine itself is the Developer's precondition.
- **Abuse cases:**
  - Open engine port: anyone on the private network takes over the host; the project refuses plain connections (AC-10).
  - Address committed by mistake: hidden from shared files (AC-05).
- **Security review:** Required, because it adds a new authorization boundary to a remote machine.

## 7. Metrics / KPIs

- **Containers on the Developer's machine during a test run:** baseline: all, target: 0 in remote mode, within 30 days of release.
- **Runs that used the wrong machine:** baseline: 0, target: 0.
- **Setup time for a new Developer:** baseline: n/a, target: ≤ 15 minutes from reading the guide to a first remote run.

## 8. Open questions

- [X] Is cleanup of leftover containers on the remote host needed, or handled by hand? Default now: manual. — owner: I.Chupryna, due: before `sdd:design`
  Answer: Test Container kills all leftover containers by itself. 
- [X] Performance tests inside the normal test command run remotely and may fail on network delay: keep the same thresholds, set per-mode thresholds, or skip them in remote mode? Default now: same thresholds. — owner: I.Chupryna, due: after the first measured remote run
  Answer: No visible impact on performance. Not an issue.
- [X] Is a remote suite duration of at most 150% of local the right target? Default now: 150% as a proposal. — owner: I.Chupryna, due: after the first measured remote run
  Answer: No visible impact on performance. Not an issue.
