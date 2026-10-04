---
status: Accepted
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-04"
feature_size: "M"
ticket: "none stated (spec §8)"
---

# 0006 — Reserve Rate Budget allowance before fetching requests and return the unused part

- **Status:** Accepted
- **Date:** 2026-10-04
- **Deciders:** Ievgen Chupryna (Architect), decided with the author during the design walk

## Context

The Rate Budget must hold even during backlog catch-up (AC-10), and requests over the budget must stay unconsumed with no reply (AC-10b). The moment allowance is taken decides whether over-budget records are ever fetched.

## Decision drivers

- AC-10b: throttled requests stay unconsumed; no reply
- Spec §6: accepted rate ≤ Rate Budget × 1.05 in any sliding 1 s window
- Memory and bandwidth: fetching what cannot be handled is waste

## Considered options

1. **Reserve for the maximum batch before fetching, then return the unused part** — nothing over budget is fetched; an underfilled lane costs one extra store call.
2. **Fetch first, then hand over only what allowance covers and rewind the rest** — fewer store calls, but over-budget records are fetched and re-read repeatedly.

## Decision outcome

**Chosen:** Reserve before fetching, return unused immediately. Allowance consumed is the units reserved minus the units returned, and a request counts as accepted when it enters the Cycle (the metric counts the same event; same-key requests waiting behind a predecessor are already accepted); Error Replies for malformed or oversized input use no allowance, so their reserved units are returned.

## Consequences

**Positive**
- The budget is never exceeded by fetched-but-unhandled batches
- Backlog catch-up is smooth: allowance paces the fetch

**Negative**
- Extra store round trip when a lane returns fewer records than reserved
- Reservation size is a tuning value (maximum batch per lane)

**Neutral**
- Returned allowance must be idempotent against store failure: unreturned units only make the worker more conservative, never exceed the budget

## Links

- Spec: [[../spec.md]]
- SAD: [[../sad.md]] §5
- Related ADR: [[0003-take-rate-budget-from-a-shared-store-and-fail-closed]], [[0004-consume-every-lane-in-every-worker-and-split-each-draw-by-weight]]
