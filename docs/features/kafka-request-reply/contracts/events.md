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
- **Open:** whether these are record headers or body fields (OQ-1). The worker reads `correlation_id` and `request_key` from record headers; replies use headers too (see Wire format).

## Channel: reply destination (one fixed destination)

- **Producer:** worker, only inside the Cycle transaction together with the request positions (ADR-0002). **Consumer:** Requesters, filtering by `correlation_id`.
- **Delivery:** Requesters must read committed records only; otherwise they may see replies of abandoned commits (SAD §11, spec §8 Q6).
- **Ordering:** none guaranteed across requests.

### Wire format (decided in F2, review B8)

The code and every test use **record headers**, not a JSON envelope; this document is aligned to that (spec §8 Q5 / OQ-1 left the choice open, no ADR requires an envelope). On both reply events:

| Header | Content |
|---|---|
| `correlation_id` | the request's correlation id, **raw bytes echoed byte for byte** (not re-encoded, so non-UTF-8 ids survive) |
| `request_key` | the request's Request Key, raw bytes echoed byte for byte |
| `type` | `reply` or `error_reply`: tells the two events apart without parsing the value |

Requesters filter by `correlation_id` and branch on `type`. A reply replaced by its `undeliverable` Error Reply (AC-08) carries `type = error_reply`. The Kafka record key is not set on replies.

### Event: `request_reply.reply.v1`

- Headers as above, `type = reply`.
- Value: the Handler result (`data`) as UTF-8 text, nothing else. No envelope.

Origin: flow 1 and flow 5 "committed reply with same correlation and Request Key" (AC-01, AC-04, AC-05).

### Event: `request_reply.error_reply.v1`

- Headers as above, `type = error_reply`.
- Value (JSON, UTF-8; string values JSON-escaped; the headers stay authoritative for the raw ids):

```json
{ "correlation_id": "<echoed>", "request_key": "<echoed>", "category": "failure | timeout | undeliverable" }
```

- No message, stack trace or payload (spec §6.1).
- `failure`: Handler threw (AC-06). `timeout`: per-request timeout or Cycle deadline (AC-07, AC-13). `undeliverable`: reply too large, cannot be encoded or rejected on send, by the client or by the broker (AC-08). Malformed or oversized request: `failure` (open, OQ-1).
- Origin: flows 2 and 6.
- **Backwards-compat:** additive only; Requesters ignore unknown fields and headers; removing or renaming is `v2`.

## Idempotency and retry

- **Idempotency key:** `lane:partition:position` of the request, handed to the Handler, never put on the reply. Same on every re-execution (AC-03, flow 5).
- **Duplicate prevention:** the reply and request position commit in one transaction; a repeated Cycle after failure cannot add a second committed reply (AC-05).
- **Retry:** a failed commit retries the same results, up to 3 attempts, Handlers not re-run (flow 2, ADR-0007). A commit that times out has an unknown outcome: the sink resolves it by committing again on the same transaction and never re-sends the replies. After that the worker pauses, probes the destination and re-commits in a new transaction.
- **Dead-letter:** none, by design. Requesters get an Error Reply instead of silence; the Requester decides about retry (spec §1). The async template's DLQ rule does not apply.

## Schema registry

Not decided: no registry or validator found in the repo. Reply wire format is fixed above (headers); the request side stays part of OQ-1.
