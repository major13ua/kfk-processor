---
status: Accepted
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-04"
feature_size: "M"
ticket: "none stated (spec §8)"
---

# 0007 — Turn send failures into Error Replies and retry commits without re-running Handlers

- **Status:** Accepted
- **Date:** 2026-10-04
- **Deciders:** Ievgen Chupryna (Architect), decided with the author during the design walk

## Context

A reply that cannot be delivered must not abort the shared commit and replay the whole Cycle on every worker, repeating Handler side effects. This is the adversary review's sharpest failure vector (spec §1). The user chose that every failure specific to one reply becomes an Error Reply.

The core of this decision was fixed in the spec and interview; the options marked "excluded by spec" record why the alternative does not stand, not an open choice.

## Decision drivers

- AC-08/AC-08b: undeliverable reply becomes an Error Reply; Handlers are not run again
- AC-09: missing permission stops intake without losing requests
- Handlers are at-least-once, but repeated side effects should be rare, not routine

## Considered options

1. **Per-reply Error Reply for reply-specific failures; bounded retry of the commit with the same in-memory results; pause and alert when exhausted** — Handlers never re-run inside the worker.
2. **Abort the Cycle on any send failure and replay it** (excluded by spec §1, AC-08) — repeats every Handler's side effects on every worker.
3. **Dead-letter the failed request and say nothing to the Requester** (excluded by US-04) — violates US-04: silence instead of an Error Reply.

## Decision outcome

**Chosen:** Failures specific to one reply (too large, cannot be encoded, rejected on send) become Error Replies in the same transaction. If the commit itself fails, it is retried with the same results, 3 attempts by default (configurable). When attempts run out, or the Error Reply cannot be delivered (destination unavailable), the worker pauses, alerts and stays in its group. A restart after that re-runs Handlers, which is the at-least-once case.

## Consequences

**Positive**
- No Handler is re-run because of a send problem
- The Requester always gets either a reply or an Error Reply, except in a destination outage

**Negative**
- Results of a Cycle are held in memory while retrying; a crash loses them and Handlers re-run on restart
- A transient broker failure during the commit may cost up to 3 commit attempts inside the Commit window

**Amendment (review r5 R1, decision 2026-10-05, pending Tech Lead sign-off):** Handlers never re-run inside the worker, EXCEPT on the hand-back path: when a Cycle fails after its Handlers ran and before the commit takes ownership (membership-change commit attempts exhausted, keep-alive failure in that retry, any unexpected failure after dispatch), every request of the Cycle is handed back and run again, including retained partitions and requests whose Handler failed or timed out; `request_reply.cycle.failed` is alerted once per failure streak. The decision otherwise stands. See spec AC-07b, AC-08 and §8.

**Neutral**
- Requires splitting reply-specific errors (sync, caught at send) from commit-level errors (async, abort the transaction)

## Links

- Spec: [[../spec.md]]
- SAD: [[../sad.md]] §6
- Related ADR: [[0002-commit-replies-and-positions-in-one-transaction-per-cycle]]
