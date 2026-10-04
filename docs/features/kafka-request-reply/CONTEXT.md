---
status: Living
updated_at: "2026-10-04"
---

# Domain Context: kafka-request-reply

## Glossary

- Consistency Lag: time from a request's creation to the moment its reply is durably committed. NOT processing time (it includes queue wait before the request was picked up).
- Cycle: one pass of the worker that gathers records from all priority lanes, runs their handlers and commits replies plus offsets together. NOT a single poll of one lane.
- Error Reply: a reply that tells the Requester its request failed (handler failure, timeout or an undeliverable reply), sent in place of a normal reply. NOT a dead-letter record (which the requester never sees).
- Handler: the team-supplied business logic that turns one request into one reply. NOT the worker engine.
- Handler Owner: the team that writes and maintains a Handler and adopts the starter. NOT the Operator.
- Operator: the person who runs a worker group, sets its Rate Budget and Priority Weights and watches its metrics. NOT the Requester.
- Idempotency Key: stable identifier of one request attempt handed to the handler so repeated execution can be deduplicated. NOT the correlation id (which only matches a reply to its request).
- Priority Lane: one request source with its own priority weight, sharing the rate budget with the other lanes. NOT a separate worker.
- Priority Weight: a lane's share of the global rate budget. NOT strict precedence (low lanes are throttled, never starved).
- Rate Budget: the global cap on requests accepted per second across all workers of one worker group. NOT per-pod capacity.
- Request Key: the identifier a Requester sets on a request, echoed unchanged on its reply. NOT the Idempotency Key (set by the worker, identifies one request attempt).
- Requester: the system that sends a request and waits for its reply. NOT the worker.
- Commit window: the maximum time one Cycle's replies and request positions may take to become durably committed before the commit is considered failed. NOT the Cycle duration.
- Cycle deadline: the latest time within a Cycle by which every request must have a reply or an Error Reply; always shorter than the Commit window. NOT the per-request timeout.
- Stall threshold: how long the worker may go without a commit while work is pending before a stall indicator is raised. NOT the Commit window.
- Identity window: how long a worker's lanes are held for it after it disappears, so a returning worker with the same identity resumes them without reassignment (the "session window" in spec §8). NOT the Commit window.
