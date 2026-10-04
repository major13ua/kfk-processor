---
status: Accepted
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-04"
feature_size: "M"
ticket: "none stated (spec §8)"
---

# 0002 — Commit replies and request positions in one transaction per Cycle

- **Status:** Accepted
- **Date:** 2026-10-04
- **Deciders:** Ievgen Chupryna (Architect), decided with the author during the design walk

## Context

A worker must never leave a Requester with a duplicate committed reply or a missing one, while handlers call services that cannot join any commit. The spec fixes a Commit window of 60 s and says committed replies and request positions move together.

The core of this decision was fixed in the spec and interview; the options marked "excluded by spec" record why the alternative does not stand, not an open choice.

## Decision drivers

- Goal 2: 0 lost or duplicated committed replies (spec §6)
- Handlers are at-least-once: external effects cannot join the commit (spec §3)
- AC-05: a repeated Cycle yields one committed reply per request attempt

## Considered options

1. **One platform transaction per Cycle holding all replies and the request positions** — atomic; Requesters read committed replies only.
2. **Idempotent reply writes, then commit positions separately** (excluded by spec §1) — a crash between the two repeats replies, so Requesters see duplicates.
3. **Commit positions first, then write replies** (excluded by spec §1) — a crash between the two loses replies.

## Decision outcome

**Chosen:** One transaction per Cycle. The transaction timeout equals the Commit window. Requesters must read committed replies only (documented requirement, spec §8).

## Consequences

**Positive**
- Duplicate committed replies are structurally impossible
- Failure of one Cycle never leaves half-committed positions

**Negative**
- A stuck commit delays replies for the whole worker for up to the Commit window
- Requesters reading uncommitted data can see replies from abandoned commits
- A reply send failure can abort the whole transaction, so per-request failures must be caught before commit (ADR-0007)

**Neutral**
- Needs a stable transaction identity per worker (ADR-0005)

## Links

- Spec: [[../spec.md]]
- SAD: [[../sad.md]] §4
- Related ADR: [[0005-require-a-stable-worker-identity]], [[0007-turn-send-failures-into-error-replies-and-retry-commits-without-rerunning-handlers]]
