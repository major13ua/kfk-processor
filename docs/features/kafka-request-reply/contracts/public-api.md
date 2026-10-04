---
status: Draft
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead"]
updated_at: "2026-10-04"
feature_size: M
interface_kind: library-sdk
---

# Public API: kafka-request-reply (starter)

Contract a Handler Owner codes against (`sad.md` §5 `api/` package, surface `library-sdk`). Derived from spec §4/§5, `sad.md` §6 flows 1-8, ADR-0001..0007 and the source design notes (`docs/kafka-request-reply-architecture.md`, handler example). No data-model: no schema change (legal skip). Names marked `proposal` are open until the Tech Lead closes spec §8 Q5. Internal tests may use `@SpringBootTest`; web dependencies are not part of this contract (SAD §2).

Package root: `xme.common.kfkprocessor.requestreply.api`.

## 1. Handler (US-01, US-02, US-04)

```java
@FunctionalInterface
public interface RequestReplyHandler<K, REQ, RES> {
    /** One request in, one reply out. Throwing any exception becomes an Error Reply (failure). */
    RES handle(RequestContext<K> context, REQ request) throws Exception;
}

public record RequestContext<K>(
    K requestKey,                    // Request Key, echoed unchanged on the reply (AC-04)
    String correlationId,            // echoed unchanged (AC-01, AC-04); name is a proposal (OQ-1)
    String idempotencyKey,           // lane:partition:position, same on every re-execution (AC-03)
    String lane,                     // Priority Lane name
    Map<String, byte[]> headers,     // request headers, read only
    CancellationSignal cancellation  // cooperative cancel on timeout (AC-07)
) {}

public interface CancellationSignal {
    boolean isCancelled();           // true once the per-request timeout elapsed
    void throwIfCancelled() throws CancellationException;
}
```

Rules:
- Exactly one `RequestReplyHandler` bean per worker; none or several: startup refuses (`request_reply.config.handler_missing`, AC-02).
- Handlers are at-least-once. Dedupe on `idempotencyKey` for repeat executions; it identifies the attempt, not the business operation (spec §6.1).
- The worker never re-runs a failed or timed-out Handler (AC-07b, AC-08).
- Same `requestKey` in one lane runs sequentially in arrival order; different keys in parallel on virtual threads (AC-07c). Order across lanes is not guaranteed.
- Cancellation is cooperative: a Handler may still finish side effects after the signal (AC-07).
- Payload content is never logged by the starter (spec §6.1).

## 2. Reply and Error Reply (US-03, US-04)

Serialization of `RES` is the starter's codec; a reply that cannot be encoded or sent becomes an Error Reply (AC-08).

```java
public enum ErrorCategory { FAILURE, TIMEOUT, UNDELIVERABLE }   // proposal (OQ-1)
```

An Error Reply carries category, correlation id and Request Key only. No exception text, no stack trace, no payload (spec §6.1). Wire shape: `events.md`.

## 3. Configuration (`@ConfigurationProperties("xme.request-reply")`)

| Property | Type | Default | Rule | AC / source |
|---|---|---|---|---|
| `worker-identity` | string | none | required, explicit, unique per replica; missing refuses start | AC-15, ADR-0005 |
| `reply-destination` | string | none | required, one fixed destination | AC-01, spec §3 |
| `rate-budget-per-second` | long | none | required, > 0, identical in all workers of a group | AC-10, ADR-0003 |
| `lanes[].name` | string | none | unique | AC-11 |
| `lanes[].source` | string | none | request source of the lane | AC-01 |
| `lanes[].weight` | decimal | none | > 0; 0 or negative refuses start | AC-02, §6 |
| `min-lane-share` | percent | 5 | lanes below it are raised, others scaled down; logged at start | AC-02, AC-12 |
| `handler-timeout` | duration | 30s | provisional; must fit inside `cycle-deadline` | AC-07, AC-02 |
| `cycle-deadline` | duration | derived | ≤ 80% of `commit-window` | AC-02, AC-13 |
| `commit-window` | duration | 60s | provisional; transaction timeout | ADR-0002 |
| `commit-retry-attempts` | int | 3 | retries same results, no Handler re-run | ADR-0007 |
| `identity-window` | duration | 45s | provisional | AC-14, ADR-0005 |
| `stall-threshold` | duration | 60s | provisional; false stalls if commit window ≥ 75s | AC-17 |
| `allowance-store.*` | adapter | none | first adapter Redis-compatible, behind the `AllowanceStore` port; `allowance-store.redis-uri` for the default adapter | ADR-0003 |
| `enabled` | boolean | true | `false` registers nothing (no Handler required) | AC-01 |
| `auto-start` | boolean | true | start the Cycle loop with the application context | AC-01 |
| `group-id` | string | `request-reply-<reply-destination>` | consumer group of the worker group | ADR-0005 |
| `max-payload-bytes` | long | 1048576 | larger requests are answered at once with an Error Reply | AC-10b |
| `draw-per-round` | int | 100 | allowance units drawn per intake round | ADR-0003 |
| `probe-interval` | duration | 5s | interval of reply-destination and allowance-store probes while paused | AC-08b, AC-09, AC-18 |

Kafka brokers come from `spring.kafka.bootstrap-servers`. Beans `RequestLanes`, `ReplySink`, `AllowanceStore` and `DestinationProbe` replace the defaults; a `Consumer<CommitRetry.Alert>` bean named `requestReplyAlertListener` replaces the default error log of configuration faults.

Startup validation (flow 4) fails with a plain-language message naming the conflicting values. Error codes, neutral `module.error_name`:

| Code | Trigger |
|---|---|
| `request_reply.config.handler_missing` | no Handler bean |
| `request_reply.config.timeout_exceeds_cycle_deadline` | timeout does not fit the Cycle deadline |
| `request_reply.config.cycle_deadline_exceeds_commit_window_share` | deadline above 80% of the commit window |
| `request_reply.config.weight_not_positive` | lane weight ≤ 0 |
| `request_reply.config.identity_missing` | no explicit worker identity |

Runtime faults (no exception to the Handler; state and metrics instead): `request_reply.reply_destination.permission_denied` (AC-09), `request_reply.reply_destination.unavailable` (AC-08b), `request_reply.rate_budget_store.unavailable` (AC-18).

## 4. Extension ports (SPI, pluggable adapters)

```java
public interface AllowanceStore {            // ADR-0003, ADR-0006
    long reserve(long units);                // may grant less; throws AllowanceStoreUnavailableException (fail closed)
    void giveBack(long units);
}
```

`RequestLanes`, `ReplySink` (transactional) and `WorkerMetrics` are internal ports; only `AllowanceStore` is public in v1.

## 5. Observability contract (US-08, AC-16, AC-17, AC-18)

Micrometer meters, tags limited to `lane` and `category` (no payload, no keys):

| Meter | Meaning |
|---|---|
| `requestreply.accepted` | requests taken at Cycle intake (counter, matches the limiter) |
| `requestreply.consistency.lag` | timer per lane; implausible samples flagged and excluded |
| `requestreply.state` | gauge: running, paused (limiter or destination), stalled |
| `requestreply.errorreply` | counter by `category` |
| `requestreply.handler.timeout` | counter |
| `requestreply.commit.attempts` | counter |
| `requestreply.cycle.duration` | timer |
| `requestreply.group.membership.changes` | counter (AC-14) |

Pause suppresses the stall state (AC-17). Meter names are proposals.

## 6. Compatibility

Internal starter, no external API-stability promise (spec §3). Handler and configuration property names change only with a CHANGELOG line.
