---
status: Accepted
owner: "I.Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-06"
feature_size: "S"
ticket: "n/a"
---

# 0002 — Reach the remote engine through a build-managed ssh tunnel

- **Status:** Accepted
- **Date:** 2026-10-06
- **Deciders:** I.Chupryna, Claude (design session)

## Context

Access to a container engine is equivalent to root on its machine, so the remote engine must be reached over an authenticated channel (spec §6.1, AC-10). The Developer wants ssh. Testcontainers has no native `ssh://` support: the upstream request was closed as not planned, and it bundles its own Docker client.

## Decision drivers

- Authenticated channel only (spec AC-10, QG-1).
- No new unproven dependency for a size S change.
- Use the ssh keys the Developer already has.

## Considered options

1. **Build starts an ssh tunnel to a local socket** — Testcontainers sees a local socket, with the host override naming the remote machine for container ports.
2. **Community ssh transport add-on on the test classpath** — keeps `ssh://` end to end, but unproven with the bundled client and needs the host override anyway.
3. **TLS client certificates** — native and proven, but needs daemon and certificate setup on the remote machine.

## Decision outcome

**Chosen:** Option 1. It reuses ssh keys, adds no dependency, and is a widely used pattern with Testcontainers. The Developer chose ssh over TLS, and option 2 was rejected after finding it unproven.

## Consequences

**Positive**
- Authentication and encryption come from ssh. No daemon change on the remote machine.

**Negative**
- Needs the `ssh` command on the laptop. A dropped VPN kills the tunnel and the run.
- Container ports are reached directly over the network, not through the tunnel, so network rules must allow them (SAD §11).

**Neutral**
- A certificate-secured option remains possible as a later feature (SAD §11 accepted debt).

## Links

- Spec: [[../spec.md]] AC-10
- SAD: [[../sad.md]] §4
- Related ADR: [[0001-switch-remote-mode-with-a-gradle-property-that-sets-the-engine-environment]]
