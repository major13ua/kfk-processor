# Kafka request-reply starter: guide and operator runbook

For Handler Owners, Requesters and Operators. Source of truth: the code in `src/main/java/xme/common/kfkprocessor/requestreply`, then [public-api.md](features/kafka-request-reply/contracts/public-api.md), [events.md](features/kafka-request-reply/contracts/events.md), [spec.md](features/kafka-request-reply/spec.md) and [sad.md](features/kafka-request-reply/sad.md).

Status of names: wire names (headers, categories), metric names and some property defaults are proposals until the Tech Lead closes spec §8 Q5 ("OQ-1" in the contracts). Numbers marked provisional are not decided, see [Open points](#open-points).

## 1. What the starter does

You supply one Handler bean and `xme.request-reply.*` configuration. The starter consumes the request lanes, enforces one global Rate Budget before accepting requests, splits it across Priority Lanes by Priority Weight, runs Handlers in parallel under a per-request timeout and a bounded Cycle, and commits replies together with the request positions in one Kafka transaction. Every accepted request ends in one committed reply or Error Reply. The starter never re-runs a failed or timed-out Handler.

Build and test locally:

```
./gradlew build       # compile, lint, unit and integration tests (Docker needed: Testcontainers tests FAIL without it)
./gradlew test -PskipDockerTests   # local opt-out: unit tests only, never in CI
./gradlew loadTest    # slow throughput run, tagged "load", excluded from the default test task
```

`loadTest` asserts the provisional throughput target for the unique-key phase and only reports the fast-Handler and hot-key phases (see [Known behaviours](#6-known-behaviours)). Options: `-Dload.target`, `-Dload.seconds`, `-Dload.workers`, `-Dload.handler-ms`, `-Dload.hot-percent`, `-Dload.phases`.

## 2. Handler contract

```java
@Component
class MyHandler implements RequestReplyHandler<String, String, String> {
    public String handle(RequestContext<String> ctx, String request) throws Exception { ... }
}
```

- Exactly one `RequestReplyHandler` bean per worker. None or several: startup refuses (`request_reply.config.handler_missing`).
- In the current auto-configuration the request payload is decoded as a UTF-8 `String` and the reply body is `String.valueOf(result)` encoded as UTF-8. Handle serialization of structured data yourself. Declare all three generics as `String`: a Handler bean resolving to any other type is refused at startup (`request_reply.config.handler_types_unsupported`); there is no codec SPI.
- `RequestContext` gives: `requestKey`, `correlationId`, `idempotencyKey`, `lane`, read-only `headers` (includes a synthetic `record_timestamp`, epoch millis), `cancellation`.
- Throwing any exception produces an Error Reply with category `failure`. The exception text is never sent to the Requester, and payload content is never logged by the starter.
- Same Request Key in one lane: Handlers run one after another in arrival order. Different keys run in parallel on virtual threads. Order across lanes is not guaranteed.
- Handlers should be I/O-bound. CPU-heavy work is out of scope.

### Idempotency Key, cancellation and at-least-once (read together)

**At-least-once.** Handler side effects cannot join the reply transaction. If a worker crashes after a Handler ran but before the Cycle commits, the request is delivered again and the Handler runs again. Replies themselves are committed once.

**Idempotency Key.** `idempotencyKey` is `lane:partition:position` of the request. It is identical on every re-execution of the same request, so use it to make repeated execution harmless. It identifies the attempt, not the business operation. If a Requester times out and sends the business operation again, that is a new request with a new Idempotency Key. Deduplicate business retries on your own business identifier (for example an order or payment id), not on the Idempotency Key.

**Cooperative cancellation (AC-07).** The timeout starts when the Handler is dispatched (default 30 s, provisional) and is also bounded by the Cycle deadline. When it elapses the Requester gets an Error Reply with category `timeout`, `cancellation.isCancelled()` becomes true, and the Cycle is not delayed. The starter does not interrupt your code: a Handler that ignores the signal may still finish its side effects after the Requester already received the timeout. Poll `isCancelled()` or call `throwIfCancelled()` between steps, and make side effects safe to complete late (use the Idempotency Key and a business identifier). A late result is discarded.

**No automatic retry.** The starter never re-runs a failed or timed-out Handler. The Requester decides whether to retry. There is no dead-letter topic by design.

## 3. Requester rules

1. **Read committed replies only.** Set the consumer `isolation.level=read_committed`. Replies are written inside the Cycle transaction, so a reader that sees uncommitted data may see replies of abandoned commits. A stuck commit delays replies for everyone up to the commit window (60 s, provisional).
2. **Send same-key requests to one lane.** Ordering is guaranteed per Request Key within a lane only. A low-share lane can deliver an older request in a later Cycle than a newer one on another lane.
   **Set the Kafka record key to the `request_key` value.** Kafka keeps order only inside one partition and picks the partition from the record key, not from the `request_key` header. If the record key differs, same-key requests can sit on different partitions and run on different workers in any order. The worker does not check this.
3. **Send the required fields.** Currently as Kafka record headers (open, OQ-1, may move to the body):
   - `correlation_id` (required, opaque, set by the Requester)
   - `request_key` (required)
   - `created_at` (optional, ISO-8601, source of Consistency Lag; if absent or unparsable, the Kafka record timestamp is used)
   - record value: the payload for the Handler.
   A request without `correlation_id` or `request_key` (or without a payload) is answered at once with an Error Reply `failure` and uses no Rate Budget allowance. A payload over `max-payload-bytes` (default 1 MiB) is answered the same way.
4. **Read the reply destination and filter by `correlation_id`.** There is one fixed reply destination. Replies and Error Replies carry `correlation_id` and `request_key` echoed unchanged as record headers; the reply record has no key. Do not rely on order across requests.
5. **Handle Error Replies.** Tell them from replies by the record header `type` (`reply` or `error_reply`). The body of an Error Reply is JSON: `{"correlation_id":"...","request_key":"...","category":"failure|timeout|undeliverable"}`. No message, stack trace or payload.

| Category | Meaning |
|---|---|
| `failure` | The Handler threw, or the request was malformed or oversized |
| `timeout` | Per-request timeout or Cycle deadline elapsed |
| `undeliverable` | The reply was too large, could not be encoded, or was rejected on send |

6. **Retry is yours.** A timeout does not mean the work did not happen (see cooperative cancellation). Retry with your own business identifier so the Handler Owner can deduplicate.

## 4. Configuration

All under `xme.request-reply`. Brokers come from `spring.kafka.bootstrap-servers` (default `localhost:9092` if unset in the starter's binder); a `KafkaConnectionDetails` bean overrides `spring.kafka.bootstrap-servers`. Defaults below are from `RequestReplyProperties`; items marked provisional are not decided.

| Property | Default | Notes |
|---|---|---|
| `enabled` | `true` | `false` registers nothing, no Handler required |
| `auto-start` | `true` | start the Cycle loop with the application context |
| `worker-identity` | none | required, explicit, unique and stable per replica |
| `reply-destination` | none | required, one fixed destination |
| `group-id` | `request-reply-<reply-destination>` | consumer group of the worker group |
| `rate-budget-per-second` | none | required, > 0 |
| `lanes[].name`, `lanes[].source`, `lanes[].weight` | none | weight must be > 0 |
| `min-lane-share` | `5` (percent) | lanes below it are raised, others scaled down; effective shares are logged at start |
| `draw-per-round` | `100` | allowance units drawn per intake round |
| `max-payload-bytes` | `1048576` | larger requests get an Error Reply at once |
| `handler-timeout` | `30s` | provisional |
| `cycle-deadline` | 80% of `commit-window` | must be at most 80% of `commit-window`; `handler-timeout` must fit inside it |
| `commit-window` | `60s` | provisional, transaction timeout |
| `commit-retry-attempts` | `3` | retries the same results, Handlers are not re-run |
| `identity-window` | `45s` | provisional, used as the consumer session timeout |
| `stall-threshold` | `60s` | provisional; false stalls if `commit-window` is 75 s or more |
| `probe-interval` | `5s` | probe cadence for the reply destination and allowance store while paused |
| `allowance-store.redis-uri` | none | required unless you provide your own `AllowanceStore` bean |

Replaceable beans: `RequestLanes`, `ReplySink`, `AllowanceStore`, `DestinationProbe`, `WorkerMetrics`, and a `Consumer<CommitRetry.Alert>` bean named `requestReplyAlertListener` (default: logs at ERROR). The Rate Budget counter is stored in Redis under `xme:request-reply:<group-id>:budget`.

Startup refusals (message names the conflicting values, code in brackets; full list in [public-api.md](features/kafka-request-reply/contracts/public-api.md) section 3): `request_reply.config.handler_missing`, `request_reply.config.handler_types_unsupported`, `request_reply.config.identity_missing`, `request_reply.config.reply_destination_missing`, `request_reply.config.reply_destination_invalid`, `request_reply.config.reply_destination_compacted`, `request_reply.config.rate_budget_not_positive`, `request_reply.config.draw_per_round_not_positive`, `request_reply.config.lanes_missing`, `request_reply.config.lane_incomplete`, `request_reply.config.lane_name_duplicate`, `request_reply.config.lane_source_invalid`, `request_reply.config.lane_source_duplicate`, `request_reply.config.weight_not_positive`, `request_reply.config.timeout_exceeds_cycle_deadline`, `request_reply.config.cycle_deadline_exceeds_commit_window_share`, `request_reply.config.ssl_bundles_missing` (an SSL bundle named by the Kafka connection details is not defined), `request_reply.config.allowance_store_missing` (no Redis URI and no `AllowanceStore` bean).

## 5. Operator runbook

### Deployment rules

- **Rate Budget and Priority Weights must be identical in every worker of a group.** The budget is shared through the allowance store, and each worker computes lane shares locally from its own weights. Change them only by a rollout of all workers, never on a subset. The starter cannot detect a mismatch.
- **Stable identity per replica.** `worker-identity` is the Kafka static group member id. It must be unique per replica and the same after a restart (for example the StatefulSet pod name). A restart that returns within `identity-window` (45 s, provisional) keeps its lanes and causes no reassignment.
- **Rolling deploy:** restart one worker at a time, each returning with its own identity.
- **Delivery requirements:** Requesters read committed replies only (section 3).
- **Upgrading a group from an earlier build.** The starter uses the `CooperativeStickyAssignor` (fixed, not configurable). Builds before this change set no assignor and ran the kafka-clients 4.2.1 default `[Range, CooperativeSticky]`, so a plain rolling restart works. Only a group that forced range-only needs a stop-all and restart of every worker (requests wait on the platform, nothing is lost) or a two-step bounce (first `[cooperative-sticky, range]`, then `cooperative-sticky`). Open point: spec §8.

### States and alerts

Metric `requestreply.state` is a gauge with ordinal values `0 = running` (includes idle), `1 = paused`, `2 = stalled`. Pause takes precedence and suppresses stall.

| State | Meaning | Operator action |
|---|---|---|
| running | Intake and Cycles are working, or nothing is pending (idle raises no stall) | none |
| paused (limiter) | Allowance store unreachable. Intake failed closed, worker stays in its group, resumes by itself when the store returns, without exceeding the budget. Alert fault id `request_reply.rate_budget_store.unavailable`, raised once per outage, also shown in `requestreply.state` | restore the Redis-compatible store |
| paused (destination) | Commit failed after `commit-retry-attempts`, or the reply destination is unavailable. Results are held in memory, the worker probes every `probe-interval` and re-commits in a new transaction. Alert fault id `request_reply.reply_destination.unavailable`. Alert fault id `request_reply.reply_destination.fenced` when the producer is fenced: a newer instance took the transactional id (same group and `worker-identity`), or a commit was held past the commit window (`transaction.timeout.ms`) and the broker aborted the transaction. The producer is never re-created and nothing is re-sent; the worker stays paused | fix the broker or destination; for `fenced`, make sure no other instance runs with the same identity, then restart this worker (the held Cycle is re-run after restart). Paused with `unavailable` for longer than the commit window: restart the worker (Handlers re-run). `fenced` is alerted on any commit attempt made in the Cycle loop; after a commit timeout the Cycle is held with alert `unavailable`, and a fence found later by the probe raises no new alert (spec §8 A1) |
| paused (permission) | No write permission on the reply destination or its transactional id, or no read permission on a lane, found by the start probe, or no write permission on a later commit. At start the worker keeps its group membership with every lane paused and starts by itself once the probe succeeds. Alert fault id `request_reply.reply_destination.permission_denied`. The transactional id is `request-reply-<len>:<group>:<identity>` and needs WRITE (and DESCRIBE) | grant the ACL (reply topic WRITE, lane topics READ, group READ, transactional id WRITE), or, if TLS/SASL authentication failed, check certificates and credentials |
| paused (lane permission) | A poll after start is denied READ on a lane or the group. The worker pauses, raises the alert once and retries every `probe-interval`, then resumes by itself. Alert fault id `request_reply.request_lane.permission_denied` | grant read permission (ACL), or, if TLS/SASL authentication failed, check certificates and credentials |
| running (reply undeliverable) | A reply was too large, not encodable or rejected. Metric `errorReply` with category `undeliverable` counts it, whether it was replaced by an `undeliverable` Error Reply or, when even that could not be delivered, dropped (position commits, no pause), except when every reply of the round is rejected as an invalid record and one of them is on its last stage (a reply without a fallback, a single no-fallback Error Reply included): then nothing is dropped and the worker pauses as a destination fault (paused (destination), alert `request_reply.reply_destination.unavailable`), see the invalid reply topic note below; likely causes are a compacted reply topic or topic-level timestamp or size bounds. Dropped replies are logged at ERROR without payload and raise alert fault id `request_reply.reply.undeliverable` once per commit; no consistency-lag sample for them | find the cause from the log and the requester's missing reply; resend the request if needed |
| running (cycle failed) | An unexpected error occurred between intake and commit. The accepted requests are handed back in poll order and served again, no pause. Alert fault id `request_reply.cycle.failed`, raised once per consecutive failure streak | find the cause in the log; the Handlers of those requests run again |
| stalled | Work pending or a Cycle open and no commit for longer than `stall-threshold` (60 s, provisional) | check Handlers and commit, see below |

Invalid reply topic: a startup value that is not a valid topic name is refused (`request_reply.config.reply_destination_invalid`); one rejected at runtime is a destination fault (paused (destination), alert `request_reply.reply_destination.unavailable`). A commit rejected because the consumer group rebalanced is a membership change, not a destination outage: the worker keeps its group alive, drops results of revoked partitions, retries, and raises no destination alert; if it keeps failing the requests are handed back (`request_reply.cycle.failed`).

Alert wiring: the limiter, commit and permission pauses, and the non-pause faults `request_reply.reply.undeliverable` and `request_reply.cycle.failed` (no pause reason), call the `requestReplyAlertListener` bean with the pause reason and fault id. Register your own bean to route them to paging. A stall is visible through `requestreply.state` only.

If the worker pauses while results are held in memory and then crashes, those Handlers run again after restart (at-least-once).

### Stall

Likely causes: slow Handlers holding a Cycle up to the Cycle deadline, a slow commit, or a commit window of 75 s or more against the 60 s threshold (false stall). Check `requestreply.cycle.duration`, `requestreply.handler.timeout` and `requestreply.commit.attempts`.

### Stranded lane

A worker that is lost or scaled down and does not return keeps its partitions until the identity window ends (45 s, provisional); those lanes are not consumed during that time. After the window the broker reassigns them. If a worker stays gone: confirm the replica is really removed, wait for the window, and watch `requestreply.group.membership.changes` and Consistency Lag for the affected lanes. Scaling down permanently should be done by removing replicas one at a time. There is no worker-side signal and no dedicated alert for a stranded lane (planned in the SAD, not implemented; see [Open points](#open-points)). What an Operator can observe: a rise of Consistency Lag on the affected lanes, a lane's `requestreply.accepted` rate falling while its backlog grows (broker-side consumer lag), and `requestreply.group.membership.changes` on the remaining workers after the window ends. Alert on broker-side consumer lag per lane.

### Metrics (proposals until spec §8 Q5 closes)

Tags are limited to `lane` and `category`.

| Meter | Meaning |
|---|---|
| `requestreply.accepted` (tag `lane`) | requests taken at intake, matches the limiter |
| `requestreply.consistency.lag` (tag `lane`) | timer, request creation to reply commit |
| `requestreply.consistency.lag.implausible` (tag `lane`) | counter of excluded samples (negative or over one day) |
| `requestreply.state` | gauge, see above |
| `requestreply.errorreply` (tag `category`) | counter of Error Replies |
| `requestreply.handler.timeout` | counter |
| `requestreply.commit.attempts` | counter |
| `requestreply.cycle.duration` | timer |
| `requestreply.group.membership.changes` | counter of partition assign and revoke events |

## 6. Known behaviours

- **Rate Budget burst cap is budget/20.** The shared counter's burst capacity is `max(1, budget / 20)` (5%). This keeps any sliding second within budget x 1.10. A consequence: with slow Cycles, throughput can fall below the configured budget, because unused allowance cannot accumulate beyond the cap. Budgets under 20 keep a minimum capacity of 1.
- **Rate Budget accuracy NFR is x1.10.** Accepted rate must stay at or under budget x 1.10 in any sliding 1 s window. The Tech Lead widened it from x1.05 to x1.10 and confirmed it on 2026-10-05 (spec §1). Allowance is counted at intake. Measured peaks went up to about x1.075. The value is still labelled provisional in spec §6.
- **Hot-key ordering throttles one partition.** Requests with the same Request Key run one after another, so a hot key limits parallelism. The `loadTest` hot-key phase measured 863 requests/s against the provisional 2,000 requests/s target.
- **Throughput target is asserted for unique keys only.** `loadTest` fails when the unique-key phase is below the target; the fast-Handler and hot-key phases print a MISSED line and do not fail.
- **A Rate Budget change does not reach a running group.** The shared counter keeps its first capacity; change the budget by a full rollout and reset the counter key (spec §8).
- **The allowance store must be up at start.** If Redis is down while the worker starts, the context fails to start instead of starting paused (spec §8).
- **Cross-lane ordering is not guaranteed** (section 3, rule 2).
- **Head-of-line delay.** One slow request can hold its Cycle for up to the Cycle deadline, including requests of high-weight lanes.
- **A commit held past the commit window needs a restart.** The commit window is the transaction timeout; when a commit is held longer (broker or network outage), the broker aborts the transaction and the Kafka client reports the producer as fenced (it cannot be told from a newer instance). The worker pauses and does not re-send, so no reply is duplicated. The alert is `request_reply.reply_destination.fenced` when the fence shows on any commit attempt made in the Cycle loop; after a commit timeout the Cycle is held with alert `request_reply.reply_destination.unavailable`, and a fence found later by the probe raises no new alert (spec §8 A1), so a worker paused with `unavailable` for longer than the commit window needs a restart too (Handlers re-run). Compaction is unsupported for reply topics: reply records are keyless, so a compacted reply topic would reject every reply; it is refused at start with `request_reply.config.reply_destination_compacted` (best effort: if the topic configs cannot be described, the start is not refused).
- **Priority is a share of the Rate Budget**, not strict precedence. Idle lanes hand their share to busy lanes.

## Open points

Linked, not decided:

- Wire format and names: record headers versus body for `correlation_id`, `request_key`, `created_at`, error categories, meter names: spec §8 Q5, OQ-1 in [events.md](features/kafka-request-reply/contracts/events.md). Schema registry not decided.
- Provisional numbers (2,000 requests/s, accuracy tolerance, 30 s timeout, 80% Cycle deadline, 60 s commit window, 45 s identity window, 60 s stall threshold): spec §8 Q2, [spec.md](features/kafka-request-reply/spec.md) §6.
- Committed-reads-only requirement and commit window: spec §8 Q6.
- Stranded-lane alert and runbook sufficiency: spec §8 Q7.
- Per-key ordering versus throughput: spec §8, [sad.md](features/kafka-request-reply/sad.md) §11.
- Consistency Lag p95 target and baseline: TBD, spec §8 (last item). The metric exists; no target is set.
- Spec AC-07c is tightened to "arrival order within a lane" (2026-10-05, review r2); cross-lane order stays unguaranteed (section 3, rule 2).
