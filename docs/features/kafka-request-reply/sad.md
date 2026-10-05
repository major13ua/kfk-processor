---
status: Draft
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-04"
feature_size: "M"
target_surfaces: [library-sdk, worker]  # library-sdk: the starter's public Handler + configuration contract; worker: the running engine inside each adopting service. → _shared/surfaces.md
---

# Software Architecture Document — kafka-request-reply

<!-- 12 Arc42 sections. Empty section → <!-- N/A: <one-line reason> -->. -->
<!-- C4 Context (L1) lives inline in §3. C4 Container (L2) lives inline in §5. -->
<!-- Numbers in §10 come VERBATIM from spec.md §6 NFR — no inventing, no rounding. -->

## 1. Introduction and goals

**Intent.** A reusable internal starter that lets a Handler Owner build a request-reply worker by supplying only a Handler and configuration. The worker enforces one global Rate Budget across all workers of a group, shares it between Priority Lanes by Priority Weight, runs Handlers in parallel under a per-request timeout, and commits every reply (or Error Reply) together with the request positions so a Requester never sees a duplicate committed reply. Operators watch Consistency Lag and stalls.

**Top-3 quality goals (1-liners; full scenarios in §10):**

1. **Downstream protection:** accepted requests stay within the Rate Budget (×1.10 in any sliding 1 s window) across all workers, and the worker fails closed when the limiter is unavailable.
2. **Reply integrity:** 0 lost or duplicated committed replies, including under restarts and failed Cycles.
3. **Rollout stability:** 0 lane reassignment for workers that return within the Identity window.

**Stakeholders.**

| Role | Interest | Sign-off owner? |
|---|---|---|
| Handler Owner | Writes a Handler and configuration only; gets an Idempotency Key | No |
| Requester | Gets one reply or Error Reply carrying its Request Key and correlation | No |
| Operator | Sets Rate Budget and Priority Weights; watches Consistency Lag, stalls, paused state | No |
| Tech Lead | SAD approval | Yes |
| Security Lead | Review of payload handling and permissions (spec §6.1: required) | Yes |

Decision override: allowance is counted at Cycle intake — rationale: allowance is reserved before fetching and returned when unused (ADR-0006), so "accepted" means the unit stays consumed when the request enters the Cycle; spec AC-10 words this as "handed to a Handler", which can be seconds later for same-key requests. The limiter and the "accepted per second" metric count the same event.

## 2. Constraints

**Technical.**
- Java 25 (Gradle toolchain), Spring Boot 4.1.1 (`build.gradle`), Spring for Apache Kafka via `spring-boot-starter-kafka`, Micrometer via `spring-boot-starter-actuator`.
- Streaming platform: Apache Kafka; the reply path needs transactions (reply and request positions committed together), so the cluster must support them.
- Rate Budget store: a shared in-memory store that XME already operates, Redis-compatible for the first adapter, behind a port (see ADR-0003).
- Architecture convention: none yet in this repo (greenfield skeleton, single `xme.common.kfkprocessor` package); this SAD sets it.
- Override note: the repo is a runnable Spring Boot application (boot plugin, web and REST client starters, devtools). It hosts the library for now (ADR-0001, §5); web and REST client dependencies are not part of the starter contract and must not be pulled into adopters. Risk in §11.

**Organisational.**
- Deadline and effort budget: not stated (to be set by the Product Owner, see §11 and spec §8).
- Success: one production service, then a second team adopting by writing only a Handler.

**Conventions.**
- No convention file in the repo; Lombok is available; tests use `@SpringBootTest` + Testcontainers (`TestcontainersConfiguration`).
- Spec §6.1: payload content is never copied into logs, metrics or Error Replies.

**Regulatory / external.**
- Data classification internal; payloads may contain personal data owned by the adopting teams; security review required (spec §6.1).
- A stable worker identity is a hard requirement (spec §3, AC-15).

## 3. Context and scope

XME teams run workers that take requests from a streaming platform, call other services or databases, and send replies. The starter is the shared engine inside such a worker: it controls how fast requests are accepted, how they are executed, and how replies are committed.

<!-- brownfield: bare Spring Boot 4.1.1 / Java 25 / Gradle application skeleton (`xme.common.kfkprocessor`), no worker code, Testcontainers Kafka wired in tests; no architecture map (`survey` not run). -->

**External systems (in / out):**

| Actor or system | Type | Interaction |
|---|---|---|
| Requester | System | Sends requests with a correlation identifier and Request Key; reads committed replies |
| Handler Owner | Person | Supplies the Handler and configuration |
| Operator | Person | Sets Rate Budget and Priority Weights; watches metrics and alerts |
| Request lanes and reply destination | System (internal) | The worker reads requests from N lanes and writes replies and commit records |
| Rate Budget store | System (internal) | Shared allowance counter for the worker group |
| Downstream services and databases | System (internal/external) | Called by Handlers; protected by the Rate Budget |
| Metrics backend | System (internal) | Receives Consistency Lag, accepted rate, pause and stall indicators |

**C4 Context (L1):**

```mermaid
C4Context
    title kafka-request-reply — System Context

    Person(owner, "Handler Owner", "Supplies the Handler and configuration")
    Person(operator, "Operator", "Sets Rate Budget and Priority Weights, watches metrics")
    System_Ext(requester, "Requester", "System that sends requests and reads committed replies")

    System(worker, "Request-reply worker", "Starter-based worker that accepts, handles and answers requests")
    System_Ext(platform, "Streaming platform", "Request lanes, reply destination, commit records")
    System_Ext(store, "Rate Budget store", "Shared in-memory allowance counter")
    System_Ext(downstream, "Downstream services", "Called by Handlers")
    System_Ext(metrics, "Metrics backend", "Lag, rate, pause and stall indicators")

    Rel(requester, platform, "Sends requests, reads replies")
    Rel(owner, worker, "Supplies Handler and configuration")
    Rel(operator, worker, "Sets Rate Budget and Priority Weights in configuration")
    Rel(operator, metrics, "Watches lag, stalls, paused state")
    Rel(worker, platform, "Reads requests, writes replies and commit records")
    Rel(worker, store, "Takes allowance before accepting requests")
    Rel(worker, downstream, "Handler calls")
    Rel(worker, metrics, "Publishes metrics")
```

## 4. Solution strategy

**Target surfaces:** `library-sdk` (Handler interface and configuration are the public contract) and `worker` (the engine running inside each adopting team's service). Decided in ADR-0001; there is no UI surface, so no UI-architecture decision applies.

**Top strategic choices (the seeds for ADRs):**

1. **One Cycle, one commit.** A Cycle gathers requests from all lanes, runs Handlers in parallel, then commits every reply (or Error Reply) together with the request positions in one transaction whose timeout is the Commit window. Handlers stay at-least-once and receive an Idempotency Key. Serves quality goal 2 (ADR-0002).
2. **Rate Budget at intake from a shared store, fail closed.** Allowance is taken before requests are fetched and unused allowance is returned; the worker pauses when the store is unreachable. Serves quality goal 1 (ADR-0003, ADR-0006).
3. **Every lane in every worker, weights split per worker.** Each worker consumes every Priority Lane (partitions spread across workers) and splits its allowance draw by Priority Weight, redistributing idle shares with a 5% minimum (ADR-0004).
4. **Stable identity everywhere.** One configured worker identity drives group membership, the transaction identity and lane holding for the 45 s Identity window. Serves quality goal 3 (ADR-0005).
5. **Handlers on virtual threads.** Each request runs on its own virtual thread; the timeout starts at dispatch, cancellation is cooperative, requests sharing a Request Key run in arrival order within a lane, and every failure becomes an Error Reply. Ordering across lanes is an open question (§11). Execution detail inside one module (no ADR: reversible).

Each tactical decision in later sections should trace to one of these seeds. Tactical decisions that contradict a strategic choice are red flags and go to §11.

## 5. Building block view

Hexagonal layering inside one module for now (the repo stays a single Gradle module, ADR-0001 notes the later split): an `engine` core that knows nothing about the platform client, the store or the metrics registry, talking through ports implemented by adapters. Spring Boot auto-configuration wires it from configuration properties and the team's Handler bean.

**Internal decomposition:**

```
xme.common.kfkprocessor.requestreply/
├── api/          Handler interface, request/reply/Error Reply types, configuration properties (library-sdk contract)
├── engine/       Cycle loop, intake with allowance, lane weights, Handler execution, commit, pause/stall state
├── ports/        AllowanceStore, RequestLanes, ReplySink (transactional), WorkerMetrics
├── adapters/     Kafka (lanes + transactional reply sink), shared-store allowance (Redis-compatible), Micrometer
└── autoconfig/   Spring Boot auto-configuration, startup validation: configuration (AC-02), explicit unique worker identity (AC-15, ADR-0005), permission to write the reply destination (AC-09)
```

Intake follows ADR-0006 (allowance reserved before fetching, unused returned). **Decision (inline, no ADR):** single Gradle module for now; extract a `starter` module plus an example worker when a second team adopts (risk in §11).

**C4 Container (L2):** one container per declared target surface: the starter API (`library-sdk`) and the engine (`worker`).

```mermaid
C4Container
    title kafka-request-reply — Containers

    Person(owner, "Handler Owner", "Supplies the Handler and configuration")
    Person(operator, "Operator", "Sets Rate Budget and Priority Weights")
    System_Ext(requester, "Requester", "System that sends requests and reads committed replies")

    Container_Boundary(workerapp, "Adopting team's worker") {
        Container(sdk, "Starter API", "Java library", "Handler interface, configuration properties, startup validation")
        Container(engine, "Request-reply engine", "Java, Spring Boot auto-configuration", "Intake with allowance, Cycles, Handler execution, atomic commit, pause and stall state")
        Container(handler, "Handler", "Team code", "Business logic for one request")
    }

    System_Ext(platform, "Streaming platform", "Request lanes, reply destination, transactions")
    System_Ext(store, "Rate Budget store", "Shared allowance counter")
    System_Ext(downstream, "Downstream services", "Called by Handlers")
    System_Ext(metrics, "Metrics backend", "Lag, rate, pause, stall")

    Rel(owner, sdk, "Implements Handler, sets configuration")
    Rel(operator, sdk, "Sets Rate Budget and Priority Weights in configuration")
    Rel(sdk, engine, "Configures")
    Rel(engine, handler, "Runs with Idempotency Key")
    Rel(handler, downstream, "Calls")
    Rel(engine, platform, "Reads requests, commits replies and positions")
    Rel(engine, store, "Takes and returns allowance")
    Rel(engine, metrics, "Publishes metrics")
    Rel(requester, platform, "Sends requests, reads committed replies")
    Rel(operator, metrics, "Watches")
```

## 6. Runtime view

**Critical flow 1: normal Cycle**

```mermaid
sequenceDiagram
    actor Requester
    participant Platform as Streaming platform
    participant Engine as Request-reply engine
    participant Store as Rate Budget store
    participant Handler
    participant Downstream as Downstream services
    Requester->>Platform: sends request with correlation and Request Key
    Engine->>Store: reserves allowance for the next Cycle
    Store-->>Engine: allowance granted
    Engine->>Platform: fetches requests from all lanes by weighted share
    Engine->>Store: returns unused allowance
    Engine->>Handler: runs each request with Idempotency Key (same-key requests of a lane in order)
    Handler->>Downstream: enriches request
    Downstream-->>Handler: result
    Handler-->>Engine: reply
    Engine->>Platform: commits replies and request positions together
    Platform-->>Requester: committed reply with same correlation and Request Key
    Engine->>Engine: records Consistency Lag per lane
```

**Critical flow 2: failures inside a Cycle (timeout, undeliverable reply, failed commit), per ADR-0007**

```mermaid
sequenceDiagram
    participant Platform as Streaming platform
    participant Engine as Request-reply engine
    participant Handler
    actor Operator
    Engine->>Handler: runs request A and request B
    Handler-->>Engine: A times out, cancelled cooperatively
    Handler-->>Engine: B replies with a result that cannot be encoded
    Engine->>Engine: builds Error Reply for A (timeout) and B (undeliverable)
    Engine->>Platform: commits replies and positions
    alt commit fails
        Engine->>Platform: retries the same results (up to 3 attempts, no Handler re-run)
    end
    alt attempts exhausted or destination unavailable
        Engine->>Operator: pauses, shows paused state, alerts, stays in group
        loop until the destination accepts writes
            Engine->>Platform: probes the destination
        end
        Engine->>Platform: re-commits the held results in a new transaction, Handlers not re-run
    end
    alt commit outstanding past the commit window (producer fenced)
        Engine->>Operator: pauses, alerts fenced, does not resume by itself
        Operator->>Engine: restarts the worker, requests are fetched again, Handlers re-run
    end
```

**Critical flow 3: limiter outage and recovery**

```mermaid
sequenceDiagram
    participant Engine as Request-reply engine
    participant Store as Rate Budget store
    actor Operator
    Engine->>Store: reserves allowance
    Store--xEngine: unreachable
    Engine->>Engine: stops accepting new requests within 5 s, keeps group membership
    Engine->>Operator: shows paused state
    loop until the store returns
        Engine->>Store: probes for allowance
    end
    Store-->>Engine: allowance granted
    Engine->>Engine: resumes within 30 s without exceeding the Rate Budget
```

**Critical flow 4: startup validation and reply-destination permission (AC-02, AC-15, AC-09)**

```mermaid
sequenceDiagram
    actor Operator
    participant Engine as Request-reply engine
    participant Platform as Streaming platform
    Note over Engine: Precondition: worker process starting with its configuration
    Engine->>Engine: validates configuration (Handler present, weights above 0, timeout fits Cycle deadline, Cycle deadline within share of Commit window, explicit worker identity)
    alt configuration invalid or identity missing
        Engine-->>Operator: refuses to start, names the conflicting values in plain language
    else configuration valid
        Engine->>Engine: logs effective share per lane (5% minimum applied, other lanes scaled down)
        Engine->>Platform: checks permission to read lanes and write the reply destination
        alt permission missing, at start or when a Cycle sends replies
            Engine-->>Operator: reports configuration fault, accepts no requests, stays in group
            loop until permission is restored
                Engine->>Platform: probes write permission
            end
            Engine->>Engine: resumes intake, a Cycle in progress is committed without re-running Handlers
        else permission present
            Engine->>Platform: joins group with the configured identity and starts the first Cycle
        end
    end
    Note over Engine,Operator: Postcondition: either refused to start, paused with no lost request, or consuming
```

**Critical flow 5: worker killed mid-Cycle, restart with the same identity (AC-03, AC-05, AC-14)**

```mermaid
sequenceDiagram
    actor Requester
    participant Platform as Streaming platform
    participant Engine as Request-reply engine
    participant Handler
    Note over Engine,Platform: Precondition: worker holds its lanes under a stable identity, a Cycle is open
    Engine->>Handler: runs request R with Idempotency Key K (lane, partition, position)
    Handler-->>Engine: side effects started
    Note over Engine: worker killed before commit
    Note over Platform: uncommitted transaction is abandoned, committed-only readers never see it
    Note over Platform,Engine: lanes held for the Identity window (45 s), other workers keep consuming, no reassignment
    alt worker returns with the same identity within the Identity window
        Engine->>Platform: rejoins and takes its lanes back
        Platform-->>Engine: delivers R again (position was never committed)
        Engine->>Handler: runs R with the same Idempotency Key K
        Handler-->>Engine: reply
        Engine->>Platform: commits reply and request position together
        Note over Engine,Platform: persists reply and request position (one transaction)
        Platform-->>Requester: exactly one committed reply for this attempt
    else worker does not return within the Identity window
        Platform->>Platform: reassigns its lanes to the other workers
        Platform-->>Requester: reply comes from another worker, still one committed reply
        Note over Platform: stranded-lane alert raised
    end
```

**Critical flow 6: failed, slow and ordered requests in one Cycle (AC-06, AC-07b, AC-07c, AC-13, AC-19)**

```mermaid
sequenceDiagram
    participant Platform as Streaming platform
    participant Engine as Request-reply engine
    participant Handler
    participant Downstream as Downstream services
    Note over Engine: Precondition: Cycle holds A (fails), B (slow, low-weight lane), C and D (same Request Key, high-weight lane)
    Engine->>Handler: dispatches A, B and C in parallel, D queued behind C
    Handler->>Downstream: calls for A, B, C
    Handler-->>Engine: A fails
    Engine->>Engine: builds Error Reply for A (failure), A is never re-run
    Downstream-->>Handler: C answered
    Handler-->>Engine: C replies
    Engine->>Handler: dispatches D after C finished (arrival order per key)
    Handler-->>Engine: D replies
    alt B finishes within its timeout
        Handler-->>Engine: B replies
    else B reaches timeout or the Cycle deadline
        Engine->>Handler: signals B to cancel (cooperative)
        Engine->>Engine: builds Error Reply for B (timeout), no request waited past the Cycle deadline
    end
    Engine->>Platform: commits replies, Error Replies and positions together
    Note over Engine,Platform: persists replies and request positions (one transaction)
    Note over Engine,Downstream: degraded downstream means Error Replies, not retries: next Cycle takes allowance only within the Rate Budget
```

**Critical flow 7: allowance split across lanes, exhausted budget and idle lanes (AC-10b, AC-11, AC-12)**

```mermaid
sequenceDiagram
    participant Engine as Request-reply engine
    participant Store as Rate Budget store
    participant Platform as Streaming platform
    Note over Engine: Precondition: several lanes with Priority Weights, backlog larger than the Rate Budget
    Engine->>Store: reserves allowance for the next Cycle
    Store-->>Engine: grants allowance (possibly less than asked)
    alt no allowance left
        Engine->>Engine: fetches nothing, waits for the next window, requests stay unconsumed with no reply
    else allowance granted
        Engine->>Engine: splits the draw by Priority Weight, 5% minimum per lane
        alt a lane is idle
            Engine->>Engine: hands its unused share to busy lanes
        end
        Engine->>Platform: fetches from each lane up to its share
        Platform-->>Engine: requests
        alt request is malformed or oversized
            Engine->>Engine: answers with an Error Reply at once, uses no allowance
        end
        Engine->>Store: returns unused allowance
    end
    Note over Engine,Store: remaining requests wait for a later Cycle
```

**Critical flow 8: stall detection and pause state (AC-16, AC-17)**

```mermaid
sequenceDiagram
    participant Engine as Request-reply engine
    participant Metrics as Metrics backend
    actor Operator
    Note over Engine: Precondition: worker running, last commit time known
    loop periodic check
        Engine->>Engine: compares pending work or open Cycle with time since last commit
    end
    alt paused (limiter or reply destination)
        Engine->>Metrics: shows paused state, stall indicator suppressed
    else pending work or open Cycle and no commit for 60 s
        Engine->>Metrics: raises stall indicator
        Metrics-->>Operator: stall alert
    else nothing pending
        Engine->>Metrics: reports idle, no stall
    end
    Engine->>Engine: next commit succeeds
    Engine->>Metrics: clears stall, records Consistency Lag per lane, flags and excludes implausible samples
```

**Flow notes (for `design`):** flows 4-8 reuse the domain participant names of flows 1-3 (§5 building blocks), not the generic `<service>`/`<data-store>` vocabulary, to stay consistent with the existing blocks. Flows 5 and 6 persist only the reply and request position inside the platform transaction, and flow 7 touches only the allowance counter, so no new relational entity or index is implied. Idempotency strategy (key = lane + partition + position) and commit retry shape are already covered by ADR-0002 and ADR-0007; no new ADR-worthy decision found.

## 7. Deployment view

The engine runs inside each adopting team's service as replicas with a stable identity (one identity per replica, held for the Identity window of 45 s after a replica disappears). Replicas can be added up to the partition count of the busiest lane; beyond that they sit idle. The Rate Budget store and the streaming platform are shared infrastructure operated outside this feature.

**Monitoring:**
- Metrics: accepted requests per second, Consistency Lag per lane, paused state, stall indicator, timeouts and Error Replies by category, commit attempts, group membership changes.
- Alerts: stall (work pending, no commit for 60 s), pause (limiter or reply destination), commit retries exhausted, stranded lane (a worker gone longer than the Identity window).
- Alert rules (AC-17): a stall is raised only when requests are pending or a Cycle is open and nothing commits for 60 s; an idle worker raises none, and a pause is shown as its own state and suppresses the stall indicator.
- Tracing: correlation identifier carried in logs and spans; never payload content.

**Scaling thresholds:**
- Target throughput: ≥ 2,000 requests/s per worker group (spec §6, provisional), checked by a load test in the performance environment.
- Parallelism per lane is capped by its partition count.

## 8. Crosscutting concepts

| Concept | Convention | Where defined |
|---|---|---|
| Logging | Structured; correlation identifier and lane only; payload content never logged | here, spec §6.1 |
| Authentication / authorization | The worker identity needs permission to read its lanes and write the reply destination and commit records; a missing permission pauses intake (AC-09) | here |
| Error handling | Handler failure, timeout and undeliverable reply become Error Replies (failure, timeout, undeliverable) with category and correlation only; destination outage pauses the worker (ADR-0007) | here, spec §8 |
| ID strategy | Idempotency Key = lane + partition + position of the request: stable across re-execution, identifies the attempt, not the business operation; correlation identifier and Request Key echoed unchanged | here |
| Ordering | Same-key requests run in order within a lane; Requesters must send same-key requests to one lane (see §11) | here |
| Internationalisation | N/A, no human-facing text beyond operator messages in English | none |
| Observability | Micrometer metrics (§7); lag samples that are negative or implausible are flagged and excluded | here |
| Events | Reply and Error Reply format and correlation rules finalised at the `api` stage (spec §8) | spec §8 |
| Rate limiting | Allowance reserved before fetching, returned when unused, fail closed; the limiter and the "accepted per second" metric count the same event (allowance consumed at Cycle intake, §1 override); Rate Budget and Priority Weights are set by the Operator in the worker configuration, identical in all workers of a group (the store holds only the counter), so changing them needs a rollout and startup logs the effective shares | ADRs, here |

## 9. Architecture decisions

| # | Title | Status | Section |
|---|---|---|---|
| 0001 | Ship the engine as a Spring Boot starter run inside each worker | Accepted | §4 |
| 0002 | Commit replies and request positions in one transaction per Cycle | Accepted | §4 |
| 0003 | Take the Rate Budget from a shared store behind a port and fail closed | Accepted | §4 |
| 0004 | Consume every lane in every worker and split each worker's allowance draw by Priority Weight | Accepted | §4 |
| 0005 | Require a stable worker identity for group membership and transactions | Accepted | §4 |
| 0006 | Reserve Rate Budget allowance before fetching requests and return the unused part | Accepted | §5 |
| 0007 | Turn send failures into Error Replies and retry commits without re-running Handlers | Accepted | §6 |

ADR files live in `docs/features/kafka-request-reply/adr/`, named `NNNN-title.md`.

## 10. Quality requirements

**QG-1. Downstream protection**
- **When:** several workers share one Rate Budget and a backlog exists, or the Rate Budget store becomes unreachable.
- **Then:** accepted rate ≤ Rate Budget × 1.10 in any sliding 1 s window (provisional); new requests stop within 5 s of the store becoming unreachable and resume within 30 s of its return (provisional); aggregate throughput ≥ 2,000 requests/s per worker group (provisional).
- **How verify:** load test in the performance environment against the worker "accepted per second" metric (counted at Cycle intake, the same event the limiter counts) vs the configured budget; failure-scenario test that cuts the store.

**QG-2. Reply integrity**
- **When:** a worker is killed mid-Cycle, a commit fails, or a reply is too large or cannot be encoded.
- **Then:** lost or duplicated committed replies = 0; Handlers are not re-run because of a send failure.
- **How verify:** failure-scenario test suite (kill mid-Cycle, failed commit, oversized reply, unavailable destination) reading committed replies only; production reconciliation of requests against replies.

**QG-3. Rollout stability**
- **When:** workers are restarted one by one, each returning with the same identity within 45 s (provisional).
- **Then:** lane reassignment = 0 for workers that return within the identity window; other workers keep consuming.
- **How verify:** rollout test plus the group membership change counter.

**QG-4. Predictable Cycle behaviour**
- **When:** lanes are busy, a Handler is slow, or commits take long.
- **Then:** each lane within ±10 percentage points of its weight when all lanes are busy (provisional); Handler timeout default 30 s from dispatch, configurable (provisional); Cycle deadline ≤ 80% of the commit window, checked at startup (provisional); commit window 60 s (provisional); every lane has a weight above 0, startup refuses a weight of 0; stall indicator when work is pending and 60 s pass without a commit (provisional). Consistency Lag p95 target is TBD (spec §8) and is added here when set.
- **How verify:** per-lane accepted-rate metric under a busy-lanes load test; startup validation tests; timeout counter and cycle-duration metric; stall-indicator metric in a failure-scenario test.

## 11. Risks and technical debt

| Risk / debt | Severity | Mitigation | Owner | Due |
|---|---|---|---|---|
| No trigger or deadline for the feature is confirmed (spec §8 Q1) | Medium | Treat as platform investment; confirm before tasks are planned | Product Owner | before `sdd:tasks` |
| Provisional timing numbers: a 30 s Handler timeout can hold a Cycle, high-weight lanes included, up to the Cycle deadline; a fixed 60 s stall threshold gives false stalls if the Commit window is 75 s or more (spec §1 decision override, spec §8 Q2) | Medium | Tech Lead confirms numbers knowingly; load test measures head-of-line delay | Tech Lead | before `sdd:tasks` |
| The Rate Budget store is a hard dependency of intake: its outage pauses every worker (ADR-0003) | Medium | Operate the store highly available; pause alert; failure-scenario test | Operator lead | before first production release |
| Weight accuracy depends on partitions spread evenly across workers (ADR-0004) | Medium | Load test against ±10 percentage points; per-lane buckets as a later upgrade | Tech Lead | before first production release |
| Rate Budget and Priority Weights are set per worker configuration; a mismatch between workers or a mid-rollout change gives an inconsistent budget | Medium | Same values in all workers; changes only through a rollout; startup logs the effective shares | Operator lead | before first production release |
| Requesters reading uncommitted replies see abandoned commits; a stuck commit delays everyone for the Commit window (ADR-0002, spec §8 Q6) | High | Document the committed-reads-only requirement in the starter guide | Tech Lead | before `sdd:tasks` |
| Lost or scaled-down workers hold their lanes until the Identity window ends (ADR-0005, spec §8 Q7) | Medium | Runbook plus stranded-lane alert | Operator lead | before `sdd:tasks` |
| Cycle results are held in memory during commit retries and pauses; a crash re-runs Handlers (at-least-once, ADR-0007) | Low | Idempotency Key; documented in the starter guide | Handler Owner | starter guide |
| Repo is a Spring Boot application, not a library; the engine lives in one module for now and web dependencies could leak into adopters (§2 override) | Medium | Extract a `starter` module plus an example worker when a second team adopts; keep web dependencies out of the starter contract | Tech Lead | when a second team adopts |
| Open architectural decision: cross-lane ordering of same-key requests | Open question | Guarantee is order per key within a lane; Requesters must send same-key requests to one lane (cross-lane order is not guaranteed: a low-share lane can deliver an older request in a later Cycle); tighten spec AC-07c ("arrival order") to "within a lane" and state the rule in the starter guide | Tech Lead | before `sdd:tasks` |
| Open architectural decision: per-key ordering may cut parallelism against the 2,000 requests/s target (spec §8) | Open question | Measure with the pilot service's real key distribution | Tech Lead | before `sdd:tasks` |
| Open architectural decision: correlation identifier name, echo rules and Error Reply format (spec §8 Q5) | Open question | Defaults: echoed unchanged; categories failure, timeout, undeliverable | Tech Lead | before `sdd:api` |
| Open architectural decision: Consistency Lag p95 target and adoption timeframe (spec §8 Q9) | Open question | Measure the pilot service first | Product Owner | before first production release |
| Open architectural decision: shared-store client for the Rate Budget (Bucket4j candidate, ADR-0003) | Open question | Confirm library and Redis-compatible store version | Tech Lead | before `sdd:tasks` |

**Accepted debt (acceptable in v1, plan to fix later):**
- Weights are enforced per worker, not exactly across the group (ADR-0004).
- Reply destination is one fixed destination; per-Requester routing is a non-goal in v1 (spec §3).

## 12. Glossary

| Term | Meaning |
|---|---|
| Consistency Lag | Time from a request's creation to the moment its reply is durably committed; includes queue wait |
| Cycle | One pass of the worker that gathers requests from all lanes, runs Handlers and commits replies plus request positions together |
| Error Reply | Reply telling the Requester its request failed (failure, timeout or undeliverable) |
| Handler / Handler Owner | Team-supplied business logic for one request / the team that writes it |
| Idempotency Key | Stable identifier of one request attempt handed to the Handler |
| Operator | Person who runs a worker group, sets Rate Budget and Priority Weights and watches metrics |
| Priority Lane / Priority Weight | One request source with its own weight / its share of the global Rate Budget |
| Rate Budget | Global cap on requests accepted per second across all workers of one group |
| Request Key | Identifier a Requester sets on a request, echoed unchanged on its reply |
| Requester | System that sends a request and waits for its reply |
| Commit window / Cycle deadline / Stall threshold / Identity window | See `CONTEXT.md`: transaction timeout of a Cycle / latest time every request must have a reply / time without commit while work is pending before a stall / time lanes are held for a returning worker |

