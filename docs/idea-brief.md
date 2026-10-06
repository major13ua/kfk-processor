---
status: Draft
owner: "I.Chupryna"
updated_at: "2026-10-06"
depth: "easy"
---

# Idea brief — remote-docker-test

## 1. Raw idea

Now testcontainers is running in on current pc.
I would like to be able to run test containers on remote docker pc or current pc, depending from test profile.
If no profile selected, tests are running on local pc, if profile = remote, remote docker is used.

## 2. Problem

Integration tests start their supporting containers on the developer's own machine, which consumes its memory and processor capacity and slows it down while tests run. A stronger remote machine is available but there is no way to point the tests at it.

## 3. Users

Developers of this repository running the integration test suite from their workstation or IDE.

## 4. Why now

No external trigger named; the motivation is relieving the developer machine. Treat as a convenience, not a deadline-driven need.

## 5. Out of scope

- CI pipeline setup: only developer machines are covered; CI keeps what it uses today.

## 6. Risks

- Assumes the remote host sits on a private network or VPN; false if it is reachable only over the public internet, which adds credential and encryption work.
- Assumes network latency between workstation and remote host is small; false on a slow link, where the suite may run slower than locally and defeat the goal.
- Assumes tests can reach started containers at the remote host's address; false if the tests hardcode a local address.
- Remote host becomes a single dependency: if it is down or full of leftover containers, the remote profile fails.

## 7. Recommendation

Add a selectable test profile named "remote". With no profile, tests use the local machine as today; with the profile, tests use the remote host whose address each developer sets once in a per-developer, uncommitted setting. No silent fallback to local, so it is always clear which machine ran the tests.

## 8. Open questions

- Whether leftover-container cleanup on the remote host is needed or handled by hand (owner: I.Chupryna).
- Whether provisioning and access to the remote host stays manual (owner: I.Chupryna).
- Whether multiple remote hosts are ever needed (owner: I.Chupryna).
