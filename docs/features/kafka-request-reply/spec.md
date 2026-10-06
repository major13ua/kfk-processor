---
status: Draft
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-05"
feature_size: "M"
---

# Spec: kafka-request-reply

> **Glossary:** [CONTEXT](./CONTEXT.md)
> **Reference module / docs / channels used:** `docs/kafka-request-reply-architecture.md`, `docs/kafka-request-reply-claude-prompt.md` (the source design notes), plus the interview. No other channels.

## 1. Context

XME teams build workers that take requests from a streaming platform, enrich them by calling other services or databases, and send replies back. Each team writes its own consume loop and meets the same problems: bursts of requests flood the downstream services, one slow request delays everything gathered with it, rolling deployments disrupt consumption, and failures produce duplicate or missing replies. The users are Handler Owners (teams that write the business logic) and the Operators who run and watch those workers.

The trigger for doing this now is not yet confirmed: the design notes describe the pain but name no incident, contract or deadline (see §8). What is known: several services need the same worker shape, handlers are I/O-bound calls lasting from about 100 ms to a few seconds, and the target load is thousands of requests per second.

Committed approach: a reusable internal starter where a team supplies only a Handler and configuration. The worker enforces one global Rate Budget before accepting requests, splits it across N Priority Lanes by Priority Weight, runs handlers in parallel under a per-request timeout and a bounded Cycle, answers every accepted request with a reply or an Error Reply, and commits replies together with the request positions so a Requester never sees a duplicate committed reply. Handlers themselves are at-least-once and receive an Idempotency Key. Every Priority Lane is consumed by every worker of the group, its partitions spread across the workers. Requests that share a Request Key run one after another in arrival order, others run in parallel. The worker never retries a failed Handler; the Requester decides whether to retry. Rationale: market research found no library combining cross-worker rate limiting at intake, weighted multi-lane priority, per-request timeouts with error replies and atomic reply commit as a handler-only starter (the closest parallel-processing library is no longer maintained; the framework's own request-reply support covers only the basic reply path), so a custom starter fills a real gap. The adversary review's sharpest failure vector is an undeliverable reply that aborts the shared commit and replays the whole Cycle on every worker, repeating handler side effects; the approach therefore treats undeliverable replies per request, never as a reason to replay. Success is one production service running it plus a second team adopting it by writing only a Handler.

Traceability: decisions fixed in the interview (2026-10-03/04): N configurable lanes with one consumer per lane; weighted-share priority; per-request error replies with batch commit; at-least-once handlers with an Idempotency Key; per-request timeout plus Cycle deadline validated at startup; global Rate Budget with a pluggable shared store (first adapter assumes an already-operated in-memory store), fail closed when it is unavailable; stable-identity deployment only; fixed reply destination with correlation echoed; full custom engine (the thin-layer alternative was declined); acceptance by a failure-scenario test suite.

Decision override: 30 s Handler timeout with a fixed 60 s stall threshold, kept as chosen by the author at critic review (the critic flagged false stall alerts for commit windows of 75 s or more, and a head-of-line delay of up to the Cycle deadline for high-weight lanes). The Tech Lead confirms this knowingly via the open question on provisional numbers in §8.

Decision override: Rate Budget accuracy tolerance widened from x1.05 to x1.10 (introduced in commit 23a544b; confirmed by the Tech Lead on 2026-10-05). The allowance is counted at intake, when the Handler hand-off is prepared, as implemented; the burst cap of the shared counter stays budget/20. Small test budgets (below 20) have no margin and must not be used for the accuracy assertion.

## 2. Goals

- Accepted requests stay within the configured Rate Budget in total across all workers (within the §6 tolerance), including backlog catch-up and rollouts, so downstream services are protected.
- A second team can stand up a worker by writing only a Handler and configuration.
- Every accepted request ends in exactly one committed outcome (reply or Error Reply), and the Operator can observe Consistency Lag and stalls.
- Workers stay available through rollouts, slow lanes and dependency incidents without group-wide disruption.

## 3. Non-goals

- Exactly-once effects inside Handlers: external services cannot join the reply commit, so Handlers are at-least-once.
- Per-Requester reply routing in v1: replies go to one fixed destination and Requesters filter by correlation; keeps the worker simple.
- Support for deployments without stable worker identity: a stable worker identity is required for uninterrupted rollouts; such teams are out of scope.
- A latency guarantee for high-priority lanes beyond the Cycle bound: priority is a share of the Rate Budget, not strict precedence.
- A public or open-source library: internal starter only, so no external API-stability promise.
- CPU-heavy Handlers: the design targets I/O-bound work.

## 4. User stories

### US-01: Run a worker from a Handler
**As a** Handler Owner
**I want** to supply only a Handler and configuration
**So that** I get a working request-reply worker without writing consumption, rate control or commit logic

### US-02: Receive an idempotency key
**As a** Handler Owner
**I want** each Handler call to carry an Idempotency Key stable across re-executions
**So that** I can make repeated execution of my side effects harmless

### US-03: Get a reply with my correlation
**As a** Requester
**I want** a reply that carries my correlation identifier, and never two committed replies for one request
**So that** I can match and trust the answer

### US-04: Get an error reply instead of silence
**As a** Requester
**I want** an Error Reply when my request fails or times out
**So that** I am not left waiting and can decide to retry

### US-05: Cap total request rate
**As an** Operator
**I want** one Rate Budget that holds across all workers of a group
**So that** downstream services are not overloaded, including during backlog catch-up

### US-06: Weight the priority lanes
**As an** Operator
**I want** to give each Priority Lane a Priority Weight
**So that** important traffic gets a larger share without starving the others

### US-07: Roll out without disruption
**As an** Operator
**I want** to restart or replace workers one by one without reassigning work across the group
**So that** deployments do not interrupt consumption

### US-08: See lag and stalls
**As an** Operator
**I want** Consistency Lag and a clear stall indicator
**So that** I can detect trouble even when no replies are being committed

### US-09: Pause safely when limiting fails
**As an** Operator
**I want** workers to stop accepting requests when the Rate Budget cannot be enforced
**So that** an outage of the limiter never turns into an overload of downstream services

## 5. Acceptance criteria

### AC-01 (US-01): happy path
**Given** a Handler Owner has supplied a Handler and configured the Priority Lanes, the Rate Budget and the reply destination
**When** the worker group starts and a Requester sends a request
**Then** the Requester receives the Handler's reply carrying the same correlation identifier as the request

### AC-02 (US-01): invalid configuration
**Given** a configuration where the per-request timeout does not fit inside the Cycle deadline, or the Cycle deadline exceeds the share of the commit window allowed in §6, or a Priority Weight is not a positive number, or which has no Handler
**When** a worker starts
**Then** it refuses to start and tells the Operator in plain language which values conflict

**And** (valid configuration) the worker logs the effective share of each Priority Lane; a lane whose share would fall below the 5% minimum share is raised to it and the other lanes are scaled down

### AC-03 (US-02): happy path
**Given** a request is delivered to a Handler
**When** the same request is executed again after a failure or restart
**Then** the Handler receives the same Idempotency Key as the first time

### AC-04 (US-03): happy path
**Given** a Requester sent a request with a correlation identifier and a Request Key
**When** the reply is committed
**Then** the reply carries the same correlation identifier and Request Key

### AC-05 (US-03): domain invariant
**Given** a request attempt whose reply was already committed
**When** a Cycle is repeated after a failure or restart
**Then** a Requester reading committed replies sees only one reply for that request attempt (a new attempt by the Requester is a new request)

### AC-06 (US-04): error
**Given** a Handler fails on one request in a Cycle
**When** the Cycle completes
**Then** the Requester of that request receives an Error Reply naming the failure category, and the other requests in the same Cycle get their normal replies

### AC-07 (US-04): error
**Given** a Handler does not finish within the per-request timeout
**When** the timeout elapses (the timer starts when the Handler is dispatched)
**Then** the Requester receives an Error Reply for a timeout, the Handler is signalled to cancel (cooperative: a Handler may still finish its side effects, which the starter guide documents next to the Idempotency Key note), and the Cycle is not delayed beyond its deadline

### AC-07b (US-04): error
**Given** a Handler fails or times out
**When** the worker handles the failure
**Then** the Handler is not run again by the worker, and the Requester receives the Error Reply from the first failure

**Exception (AC deviation, needs Tech Lead sign-off; review r5 R1, decision 2026-10-05, code unchanged):** when a Cycle fails after its Handlers ran and before the commit takes ownership of the results (membership-change commit attempts exhausted, keep-alive failure in that retry, any unexpected failure after dispatch), the worker hands every request of the Cycle back and runs them again, including requests on partitions it keeps and requests whose Handler already failed or timed out. `request_reply.cycle.failed` is alerted once per consecutive failure streak (the streak ends on a committed iteration). The first run's allowance units stay spent and the rerun takes new units. Pending Tech Lead sign-off (§8).

### AC-07c (US-01): domain invariant
**Given** several requests with the same Request Key are waiting in a Cycle in the same Priority Lane
**When** the Handlers run
**Then** those requests run one after another in their arrival order within that lane, while requests with different Request Keys, or with the same Request Key in different lanes, run in parallel; order across lanes is not guaranteed (Requesters send same-key requests to one lane; SAD §11, tightened 2026-10-05 per review r2)

### AC-08 (US-04): domain invariant
**Given** a reply cannot be delivered for any reason specific to it (too large, cannot be encoded, rejected on send)
**When** the worker tries to commit the Cycle
**Then** the Requester gets an Error Reply instead, the Handlers of that Cycle are not run again, and the other requests in it are unaffected

**Exception (AC deviation, same as AC-07b, needs Tech Lead sign-off; review r5 R1, decision 2026-10-05, code unchanged):** when a Cycle fails after its Handlers ran and before the commit takes ownership of the results (membership-change commit attempts exhausted, keep-alive failure in that retry, any unexpected failure after dispatch), the worker hands every request of the Cycle back and runs them again, including requests on partitions it keeps and requests whose Handler already failed or timed out. `request_reply.cycle.failed` is alerted once per consecutive failure streak (the streak ends on a committed iteration). The first run's allowance units stay spent and the rerun takes new units. Pending Tech Lead sign-off (§8).

### AC-08b (US-04): error
**Given** an Error Reply itself cannot be delivered because the reply destination is unavailable
**When** the worker tries to commit the Cycle
**Then** the worker pauses as in AC-09, alerts the Operator, stays in its group, does not run the Handlers of that Cycle again, and resumes automatically when the destination returns

**Exception (AC deviation, needs Tech Lead sign-off; review r3 Group A, decision 2026-10-05, code unchanged):** if the commit stays outstanding past the commit window, the broker aborts the transaction and the producer is fenced. The worker then pauses with alert `request_reply.reply_destination.fenced` (raised on any commit attempt made in the Cycle loop; after a commit timeout hold the alert is `unavailable` and a fence found later by the probe raises no new alert, §8 A1), does not resume automatically, and needs a manual restart. After the restart the Cycle's requests are fetched again and its Handlers run again (at-least-once; no duplicate committed reply, AC-05). Residual gaps are logged in §8 (A1, A2, A3, A5)

### AC-09 (US-03): authorization
**Given** the worker has no permission to write to the reply destination
**When** it starts or a Cycle tries to send replies
**Then** the worker stops accepting requests, reports a configuration fault to the Operator, loses no request, stays in its group (no lane reassignment) and resumes automatically once the permission is restored, without running the Handlers of a Cycle in progress again

### AC-10 (US-05): happy path
**Given** several workers share one Rate Budget and a backlog of requests exists
**When** the group catches up
**Then** the total number of requests accepted across all workers stays within the Rate Budget in every sliding one-second window, within the tolerance in §6. A request counts as accepted when it is handed to a Handler, taking one unit of allowance at that moment

### AC-10b (US-05): domain invariant
**Given** more requests are waiting than the Rate Budget allows
**When** the worker has no allowance left
**Then** the remaining requests stay unconsumed, get no reply and wait for a later Cycle; requests answered at once with an Error Reply because they are malformed or oversized use no allowance

### AC-11 (US-06): happy path
**Given** three Priority Lanes with Priority Weights and traffic waiting on all of them
**When** the group is running at its Rate Budget
**Then** each lane's share of accepted requests matches its Priority Weight within the tolerance in §6

### AC-12 (US-06): domain invariant
**Given** a Priority Lane with a Priority Weight and pending requests
**When** other lanes have heavy traffic
**Then** that lane still receives at least its minimum share of the Rate Budget (5%, see §8) and its requests keep being served

### AC-13 (US-06): domain invariant
**Given** a slow Handler on a low-weight lane
**When** a high-weight lane request is in the same Cycle
**Then** no request in the Cycle waits longer than the Cycle deadline, and unfinished requests receive Error Replies

### AC-14 (US-07): happy path
**Given** workers are restarted one by one, each returning with the same identity within the allowed window
**When** the rollout runs
**Then** the other workers keep consuming without any reassignment of lanes

### AC-15 (US-07): error
**Given** a worker starts without an explicitly configured worker identity
**When** it starts
**Then** it refuses to start and explains that a stable identity must be configured

### AC-16 (US-08): happy path
**Given** replies are being committed
**When** each Cycle commits
**Then** Consistency Lag is recorded for every committed reply and shown to the Operator per lane

### AC-17 (US-08): error
**Given** requests are pending or a Cycle is open, and the worker has committed nothing for longer than the stall threshold in §6
**When** the Operator looks at the monitoring view
**Then** a stall indicator is raised, so missing lag data is not mistaken for a healthy worker; an idle worker with nothing pending raises no stall, and a pause (AC-09, AC-18) is shown as its own state and suppresses the stall indicator

### AC-18 (US-09): cross-context
**Given** the shared store behind the Rate Budget becomes unavailable
**When** a worker needs allowance to accept requests
**Then** it stops accepting new requests, stays a member of its group, shows a paused state to the Operator, and resumes automatically when the store returns without exceeding the Rate Budget

### AC-19 (US-05): cross-context
**Given** a downstream service is degraded and its calls are slow or failing
**When** Handlers hit their per-request timeout
**Then** the Requesters receive Error Replies and the accepted rate stays within the Rate Budget, so the degraded service is not hit harder by a retry storm from the worker

## 6. Non-functional requirements

Provisional numbers are marked and confirmed or deferred in §8.

| Aspect | Target | Measurement |
|---|---|---|
| Aggregate throughput | ≥ 2,000 requests/s per worker group (provisional, "thousands" per interview) | load test in the performance environment |
| Rate Budget accuracy | accepted rate ≤ Rate Budget × 1.10 in any sliding 1 s window (provisional) | worker "accepted per second" metric vs configured budget |
| Priority Weight accuracy | each lane within ±10 percentage points of its effective share (weights normalised, minimum share 5%) when all lanes are busy (provisional) | per-lane accepted-rate metric |
| Per-request Handler timeout | default 30 s from dispatch, configurable (provisional); implies a commit window above 37.5 s | startup validation + timeout counter |
| Cycle deadline | ≤ 80% of the commit window, checked at startup (provisional) | startup validation + cycle-duration metric |
| Priority Weight validity | every lane has a weight above 0; startup refuses a weight of 0 | startup validation |
| Commit window | 60 s (provisional) | startup validation |
| Identity window for a returning worker | 45 s (provisional) | rollout test |
| Lost or duplicated committed replies | 0 | failure-scenario test suite |
| Lane reassignment during rolling restart | 0 for workers that return within the identity window | rollout test + group membership change counter |
| Pause on limiter outage | new requests stop within 5 s of the store becoming unreachable; resume within 30 s of its return (provisional) | failure-scenario test |
| Stall detection | stall indicator when work is pending and 60 s pass without a commit (provisional) | stall-indicator metric |
| Consistency Lag p95 | TBD, see §8 | Consistency Lag metric per lane |

Measurement note (AC-12, review r2): the minimum-share check in `WeightAccuracyIT` allows 0.5 percentage points of measurement slack (minimum share 5% is asserted as ≥ 4.5%) for window-edge and per-worker rounding noise over a finite window of accepted counts; a starved lane is far below it. The ±10 pp Priority Weight accuracy assertion has no slack.

## 6.1 Security / privacy

- **Data classification:** internal. Request and reply payloads belong to the owning teams and may carry confidential data; the starter does not inspect them.
- **Personal data touched:** none added by the starter. Payloads may contain personal data; the starter must not copy payload content into logs, metrics or Error Replies.
- **AuthZ/AuthN impact:** the worker identity needs permission to read its request lanes and write the reply destination and the commit records; a missing permission is a stop-and-alert condition (AC-09). No new end-user permission checks.
- **Abuse cases:**
  - Flooding by one Requester: the Rate Budget and Priority Weights bound the effect on downstream services and other lanes.
  - Oversized or malformed requests: handled per request as an Error Reply, never as a reason to stop the worker (AC-08).
  - Business retry after a Requester timeout: the Idempotency Key identifies the attempt, not the business operation, so Handler Owners must deduplicate with their own business identifier; documented in the starter guide.
  - Forged or skewed request timestamps: Consistency Lag may go negative or inflate; the metric flags and excludes implausible samples.
  - Information leak through Error Replies: replies carry a failure category and correlation only, no internal details.
- **Security review:** Required (shared infrastructure that carries every adopting team's payloads).

## 7. Metrics / KPIs

- **Adoption:** baseline: 0 services; target: 1 service in production, then a second team live by writing only a Handler (timeframe TBD, see §8).
- **Downstream overload incidents attributable to workers using the starter:** baseline: TBD (count from the incident log of the pilot service before adoption); target: 0 in the first 90 days of production use.
- **Lost or duplicated committed replies:** baseline: 0 (new); target: 0 in production, verified by reconciliation of requests against replies.
- **Lane reassignments during a rolling deployment:** baseline: TBD (measure on the pilot service's current worker); target: 0.
- **Consistency Lag p95 per lane:** baseline: TBD (measure the current worker); target: TBD (see §8).

## 8. Open questions

- [ ] What is the trigger and deadline for this work (incident, contract, planned service)? Default now: none stated, treated as a platform investment. owner: Product Owner, due: before `sdd:design`
- [ ] Are the provisional NFR numbers right (2,000 requests/s, budget tolerance x1.10 (confirmed 2026-10-05, see §1), 30 s Handler timeout, 80% Cycle deadline, 60 s commit window, 45 s identity window, 60 s stall threshold)? Note: with 30 s Handler timeout, one slow request can hold a whole Cycle, high-weight lanes included, for up to the Cycle deadline, and the fixed 60 s stall threshold gives false stalls if the commit window is 75 s or more. Default now: as listed in §6. owner: Tech Lead, due: before `sdd:design`
- [ ] Do idle lanes hand their share to busy lanes, and what is the minimum share that prevents starvation? Default now: idle shares are redistributed, minimum share 5%. owner: Product Owner, due: before `sdd:design`
- [ ] Which already-operated shared store hosts the Rate Budget? Default now: the in-memory store XME already runs, first adapter; others pluggable. owner: Tech Lead, due: before `sdd:design`
- [ ] What are the correlation identifier name, the echo rules and the Error Reply format (category list)? Default now: correlation echoed unchanged, categories: failure, timeout, undeliverable. owner: Tech Lead, due: before `sdd:api`
- [ ] Requesters must read only committed replies, otherwise they may see replies from abandoned commits; and a stuck commit delays replies for everyone. Is this an acceptable documented requirement, and is the provisional 60 s commit window right? Default now: documented requirement, 60 s window (§6). owner: Tech Lead, due: before `sdd:design`
- [ ] Per-Request-Key ordering (AC-07c) may cut parallelism when many requests share a key, against the 2,000 requests/s target. Default now: ordering kept per key, target measured with the pilot service's real key distribution. owner: Tech Lead, due: before `sdd:design`
- [ ] Stable-identity workers that are lost or scaled down hold their lanes until the Identity window ends. Is a runbook plus a stranded-lane alert enough? Default now: yes. owner: Operator lead, due: before `sdd:tasks`
- [ ] Target and baseline for Consistency Lag p95 and the adoption timeframe? Default now: measure the pilot service first. owner: Product Owner, due: before the first production release
- [ ] Rate Budget change never reaches a running group: the shared counter keeps its first capacity (Bucket4jBudgetCounters). Default now: documented in the guide, change only by a rollout with a counter key reset. owner: Tech Lead, due: before the first production release
- [ ] Redis down at worker startup crashes the context instead of starting paused (AC-18 covers only an outage while running). Default now: the Operator restores the store before start. owner: Tech Lead, due: before the first production release
- [ ] Commit retries run back-to-back without backoff (CommitRetry). Default now: `commit-retry-attempts` small, then pause and probe every `probe-interval`. owner: Tech Lead, due: before the first production release
- [ ] Timed-out Handler threads are never interrupted, only signalled (cooperative, AC-07 and public-api). Is a hard interrupt wanted for non-cooperative Handlers? Default now: cooperative only, as specified. owner: Tech Lead, due: before the first production release
- [ ] Lag plausibility bound (1 day) is too loose for skewed timestamps; should it be configurable? Default now: 1 day. owner: Tech Lead, due: before the first production release
- [ ] Stranded-lane alert (SAD §8) is not implemented: no worker-side signal exists. Default now: runbook watches `requestreply.group.membership.changes` and Consistency Lag. owner: Operator lead, due: before the first production release
- [ ] Interrupt during Redis `reserve` on graceful stop raises a false limiter pause and alert (Intake). Default now: accepted, appears only on shutdown under load. owner: Tech Lead, due: before the first production release
- [ ] Interrupted commit on graceful stop can leave the Kafka transaction open up to the transaction timeout (60 s), delaying `read_committed` replies on touched partitions. Default now: accepted. owner: Tech Lead, due: before the first production release
- [x] A commit held past the commit window (= `transaction.timeout.ms`) is aborted by the broker, and kafka-clients 4.2.1 reports it as `ProducerFencedException` (INVALID_PRODUCER_EPOCH mapped to PRODUCER_FENCED), the same as a real fence. Decision 2026-10-05 (review r2 A2, F13): every fence on commit is permanent: the worker pauses with alert `request_reply.reply_destination.fenced` (`unavailable` if a commit timeout hold came first, A1), never re-creates the producer or re-sends (no duplicate replies, AC-05); a worker restart recovers: the held Cycle's requests are fetched again and its Handlers re-run (nothing is re-sent from memory). Unknown-outcome commits whose retry fails definitively (TRANSACTION_ABORTABLE, INVALID_TXN_STATE) and InvalidProducerEpoch on the produce path are aborted and re-sent without restart. owner: Tech Lead
- [ ] Mid-batch reject residual (review r2 F14, r4 S5), verified: a whole-batch failure from MESSAGE_TOO_LARGE is not reproducible, the producer splits the batch and resends it (Sender.completeBatch), so good replies ahead are delivered. A per-record broker validation failure (INVALID_RECORD) is classified per reply (F28, F31): the rejected reply is demoted or dropped individually, a collateral batch-sibling `KafkaException` (Sender.failBatch) is skipped and the siblings are resent with their primary values, and a round where every reply (and every fallback) is rejected with `InvalidRecordException` is a destination fault (the worker pauses, nothing is dropped); a reply without a fallback (an Error Reply) counts as already on its last stage, so any all-`InvalidRecordException` round that contains such a reply is a destination fault too, mixed rounds and a single-reply Cycle included, without first trying the fallbacks of the other replies (review r6 G1, r7 H1, F35). The mid-batch MESSAGE_TOO_LARGE split-and-resend statement is pinned by `KafkaReplySinkIT.midBatchBrokerRejectedReplyKeepsOneCommittedReplyPerRequestAndReportsExactlyTheSubstitutedOnes`. Cause: producer batch splitting. The dropped-ahead scenario has no known reproduction. Reply records are keyless, so a compacted reply topic rejects every reply: compaction is unsupported for reply topics and is refused at start with `request_reply.config.reply_destination_compacted` (starter guide). Default now: accepted. owner: Tech Lead, due: before the first production release
- [ ] Moving a running consumer group from the eager Range assignor to the CooperativeStickyAssignor: builds before F12 set no assignor, so they ran the kafka-clients 4.2.1 default `[Range, CooperativeSticky]` (ConsumerConfig), and a plain rolling restart works. Stop-all and restart is needed only for groups that forced range-only. The starter hard-codes `cooperative-sticky` (runbook in the starter guide). owner: Tech Lead, due: before the first production release
- [ ] `KafkaRequestLanesIT.restartWithSameIdentityWithinWindowCausesNoRebalanceForTheOtherWorker` failed once under load; unconfirmed whether related to the cooperative assignor (F12). Default now: investigate if it reproduces (F18 ran it 5 times in isolation on 2026-10-05: 5 of 5 passed, not reproduced; under full-suite load it is still unconfirmed). owner: Tech Lead, due: before the first production release
- [ ] Full-scale load and full-time-value test runs are not configurable and the documented `loadTest` commands do not complete; ThroughputLoadIT does not assert zero Error Replies; thread-scan test in RequestReplyAutoConfigurationTest is order-dependent. Default now: reduced scale recorded in test-plan. owner: Tech Lead, due: before the first production release
- [ ] **AC-08b deviation, sign-off needed (review r3 A1):** the `fenced` alert is raised on any commit attempt made in the Cycle loop (`CommitRetry.commit`). After a `TimeoutException` hold the alert is `unavailable`; if the broker then aborts the transaction, a fence found later in `tick()` moves the reason without a new alert: the Operator sees only `unavailable` and may wait for a resume that never comes. Default now: runbook says a destination outage lasting longer than the commit window needs a restart. owner: Tech Lead, due: before the first production release
- [ ] **AC-08b deviation (review r3 A2):** a fence is not sticky. If revocation empties the held Cycle after a fence, the probe passes (it does not see the fence), the worker resumes and runs a Cycle that cannot commit. Default now: accepted; restart clears it. owner: Tech Lead, due: before the first production release
- [ ] **AC-08b deviation (review r3 A3):** `delivery.timeout.ms` (120 s) exceeds the commit window (60 s), so an outage shows STALLED, not PAUSED, for up to about 228 s; `max.poll.interval.ms` (300 s) is not tied to the commit window, and a window above about 100 s can lose group membership. Default now: keep the 60 s window. owner: Tech Lead, due: before the first production release
- [ ] **AC-08b test gap (review r3 A5):** no real-broker outage IT for AC-08b: `DestinationUnavailableIT` injects the fault before the real sink and covers normal replies only, not an undeliverable Error Reply. Also no real-broker test of an unknown commit outcome (`KafkaReplySinkTransactionTimeoutIT` covers producer recovery only). Default now: accepted for the pilot. owner: Tech Lead, due: before the first production release
- [ ] **Throughput bound per worker (review r3):** accepted rate is limited to (Rate Budget / 20) divided by the Cycle duration per worker, because each Cycle reserves at most that allowance (`Intake`, `Bucket4jBudgetCounters` capacity) and Cycles run strictly one after another (`CycleLoop`). With 1 s Handlers and 4 workers about 400 requests/s is reachable against the 2,000 requests/s target. `ThroughputLoadIT` uses only 2 ms Handlers. Tech Lead decision: accept, pipeline Cycles, or reserve allowance several times per Cycle. Also run the load test at `load.handler-ms` 100 and 1000. Default now: not decided. owner: Tech Lead, due: before the first production release
- [ ] **Rebalance while a Cycle commits (review r4):** no real-broker IT where a second member joins while a Cycle is committing and asserts that only moved-partition requests run twice. Open point: whether the `poll(Duration.ZERO)` keep-alive retries (`KafkaRequestLanes.keepAlive`, 3 attempts) bring the group generation current; consider a bounded non-zero keep-alive. owner: Tech Lead, due: before the first production release
- [ ] **Hand-back rerun after Handlers ran (review r5 R1, AC-07b/AC-08 exception, sign-off needed):** a Cycle that fails after its Handlers ran and before the commit takes ownership is handed back and run again by the same worker, including retained partitions and requests whose Handler already failed or timed out (Handlers run twice, side effects repeat; the Requester gets one reply, AC-05). Alert `request_reply.cycle.failed` once per failure streak. Default now: accepted for the pilot, Handlers must be idempotent (Idempotency Key, starter guide). owner: Tech Lead, due: before the first production release
- [ ] **Rerun after rebalance consumes budget (review r4, F26):** when the Handlers already ran and the commit fails, the first run's units stay spent and the rerun takes new units. After a rebalance the rate bound may be consumed by reruns (requests run twice, charged twice). Default now: accepted. owner: Tech Lead, due: before the first production release
- [ ] **Compacted reply topic swallowed by permission gate and runtime probe (review r6 G2, deferred):** if the start probe fails on `PermissionDenied` before the compaction check, the permission gate treats a later `Invalid` (`reply_destination_compacted`) as a generic fault and loops, so the worker stays paused with reason `permission`, no refusal, no code logged; at runtime `CommitRetry.tick` maps `Invalid` to the destination reason (`unavailable`) with no code logged. No loss. RequestReplyLifecycle.java:86, DefaultKafkaDestinationProbe.java:61-65, CommitRetry.java:169-171. owner: Tech Lead, due: before the first production release
- [ ] **Handler-facing docs and doc consistency (review r6 G3, deferred):** public-api.md:44, guide :9/:39/:45, events.md:79, sad.md:313, test-plan.md:88 still say a failed or timed-out Handler is never re-run, contradicting the AC-07b/AC-08 hand-back exception; test-plan.md:59 is a truncated table row (delete); "fence found later by the probe" (spec AC-08b, sad.md:222, events.md:79, public-api.md:109, guide :116/:163) should read "found later when the held results are re-committed after a probe"; the AC-07b exception must state the Requester gets the reply or Error Reply of the rerun, exactly one committed (AC-05); F33 `acs` lacks AC-08b. owner: Tech Lead, due: before the first production release
- [ ] **firstFailed may demote a good reply (review r6 G5, deferred, source reading only):** kafka-clients 4.2.1 `Sender` aborts undrained batches with the top-level `InvalidRecordException`, so with several reply partitions a lower-index good reply can be chosen for demotion when a higher-index batch is rejected per record; it then gets an undeliverable Error Reply instead of its reply. Needs a genuine per-reply INVALID_RECORD plus ordering inversion. KafkaReplySink.java:378-393. owner: Tech Lead, due: before the first production release
- [ ] **F35 pauses a mixed all-InvalidRecord round before demoting a reply with an untried fallback (review r7 H1, deferred, source reading only):** with one truly invalid normal reply alone in its batch plus Error Replies in undrained batches, every reply shows as `InvalidRecordException`; a no-fallback reply counts as last stage, so the Cycle is reported as a destination fault before the bad reply is moved to its fallback, and CommitRetry may repeat the pause until restart. No loss. Related to G5. The `ReplySink` javadoc (ReplySink.java:14-16, review r8 J1, deferred with H1) reads as if the other replies' fallbacks were tried first; reword it together with the H1 rule change. KafkaReplySink.java:416. Better rule: demote replies with an untried fallback first, raise the fault only when every non-dropped reply is on its last stage. owner: Tech Lead, due: before the first production release
