---
status: Draft
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead"]
updated_at: "2026-10-04"
feature_size: M
---

# Events: kafka-request-reply

Wire contract between Requesters and the worker, derived from `sad.md` §6 flows 1, 2, 5, 6. Field names, header names and categories follow the spec §8 Q5 defaults and stay **proposals** until the Tech Lead closes it.

## Channel: request lanes (N Priority Lanes)

- **Producer:** Requester. **Consumer:** every worker of the group, partitions spread across workers (ADR-0004).
- **Delivery:** at-least-once to the Handler; replies exactly-once as seen by committed-only readers (ADR-0002).
- **Ordering:** by Request Key within a lane only. Requesters must send same-key requests to one lane (SAD §11).

### Event: `request_reply.request.v1`

```json
{
  "correlation_id": "<opaque, set by Requester>",
  "request_key": "<set by Requester>",
  "created_at": "<iso8601>",
  "data": "<payload owned by the Handler Owner>"
}
```

- **Required:** `correlation_id`, `request_key`. A request missing them is malformed: answered at once with an Error Reply, uses no allowance (AC-10b).
- **`created_at`:** source of Consistency Lag (platform record timestamp may stand in); skewed values are excluded from metrics (spec §6.1).
- **Origin:** flow 1 "sends request with correlation and Request Key".
- **Open:** whether these are record headers or body fields (OQ-1).

## Channel: reply destination (one fixed destination)

- **Producer:** worker, only inside the Cycle transaction together with the request positions (ADR-0002). **Consumer:** Requesters, filtering by `correlation_id`.
- **Delivery:** Requesters must read committed records only; otherwise they may see replies of abandoned commits (SAD §11, spec §8 Q6).
- **Ordering:** none guaranteed across requests.

### Event: `request_reply.reply.v1`

```json
{ "correlation_id": "<echoed unchanged>", "request_key": "<echoed unchanged>", "data": "<Handler result>" }
```

Origin: flow 1 and flow 5 "committed reply with same correlation and Request Key" (AC-01, AC-04, AC-05).

### Event: `request_reply.error_reply.v1`

```json
{ "correlation_id": "<echoed unchanged>", "request_key": "<echoed unchanged>", "category": "failure | timeout | undeliverable" }
```

- No message, stack trace or payload (spec §6.1).
- `failure`: Handler threw (AC-06). `timeout`: per-request timeout or Cycle deadline (AC-07, AC-13). `undeliverable`: reply too large, cannot be encoded or rejected on send (AC-08). Malformed or oversized request: `failure` (open, OQ-1).
- Origin: flows 2 and 6.
- **Backwards-compat:** additive only; Requesters ignore unknown fields; removing or renaming is `v2`.

## Idempotency and retry

- **Idempotency key:** `lane:partition:position` of the request, handed to the Handler, never put on the reply. Same on every re-execution (AC-03, flow 5).
- **Duplicate prevention:** the reply and request position commit in one transaction; a repeated Cycle after failure cannot add a second committed reply (AC-05).
- **Retry:** a failed commit retries the same results, up to 3 attempts, Handlers not re-run (flow 2, ADR-0007). After that the worker pauses, probes the destination and re-commits in a new transaction.
- **Dead-letter:** none, by design. Requesters get an Error Reply instead of silence; the Requester decides about retry (spec §1). The async template's DLQ rule does not apply.

## Schema registry

Not decided: no registry or validator found in the repo. Wire format (JSON vs other, record headers vs body) is part of OQ-1.
