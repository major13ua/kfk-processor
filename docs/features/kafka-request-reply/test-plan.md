---
status: Draft
owner: "QA owner (TBD)"
reviewers: ["Ievgen Chupryna", "Tech Lead"]
updated_at: "2026-10-04"
feature_size: "M"
---

# Test plan: kafka-request-reply

A reusable worker starter: global Rate Budget at intake, weighted Priority Lanes, per-request timeout with Error Replies, atomic reply commit, stable-identity rollouts, pause and stall states. Acceptance is by a failure-scenario suite, so most error ACs live at integration level against real dependencies.

## Levels

| Level | Scope | Strategy (generic, no tool names) |
|---|---|---|
| Unit | Pure logic: configuration validation, lane share calculation, per-key ordering, allowance accounting, pause/stall state machine, timeout and Cycle deadline arithmetic. | In-memory, injected fake clock, no external dependency. |
| Integration | Engine adapters against the real streaming platform (lanes, transactional reply sink, group membership) and the real shared allowance store. | Ephemeral real dependencies (throwaway containers) per suite; faults injected by stopping, pausing or cutting the container. |
| Contract | Shape of requests, replies and Error Replies (correlation, Request Key, failure category) against `contracts/events.md` and `contracts/public-api.md`. | Validate real produced and consumed records against the agreed contract; no hand-rolled stubs. |
| E2E | Full flow: Requester sends, worker group runs a sample Handler, Requester reads committed replies only. | Real entry point, real ephemeral platform and store, multiple workers. |
| Load | Numeric NFRs of §6. | The load tool already in the repo, or e.g. k6 or Locust, run in the performance environment. |
| Component / Visual-regression / E2E-through-UI | <!-- N/A: target_surfaces is [library-sdk, worker], no UI surface --> | |

## AC coverage

| AC (spec.md §5) | Test name (intent-based) | Level | Expected outcome |
|---|---|---|---|
| AC-01 happy path | `request is answered with the handler reply and same correlation` | e2e + unit | Requester reads one reply carrying its correlation identifier |
| AC-02 invalid configuration | `timeout larger than cycle deadline is refused at startup` | unit | Worker refuses to start, message names the conflicting values |
| AC-02 invalid configuration | `cycle deadline above allowed share of commit window is refused` | unit | Refused, conflicting values named |
| AC-02 invalid configuration | `non-positive priority weight is refused` | unit | Refused, offending lane named |
| AC-02 invalid configuration | `missing handler is refused` | unit | Refused, absence of Handler named |
| AC-02 valid configuration | `effective lane shares are logged and a lane under 5 percent is raised, others scaled down` | unit | Effective shares logged, minimum share applied, shares sum to the whole |
| AC-03 idempotency key | `same request re-executed after restart gets the same idempotency key` | integration | Key identical across first run and re-execution after worker kill |
| AC-03 idempotency key | `idempotency key is derived deterministically from request identity` | unit | Same input gives same key, different requests give different keys |
| AC-04 happy path | `committed reply echoes correlation identifier and request key` | contract | Reply record matches contract and echoes both values unchanged |
| AC-05 domain invariant | `cycle repeated after failure shows one committed reply per request attempt` | integration | Reader of committed replies sees exactly one reply per attempt |
| AC-05 domain invariant | `a new attempt by the requester is treated as a new request` | integration | Second attempt gets its own reply |
| AC-06 error | `handler failure yields error reply with failure category, others unaffected` | e2e + unit | Failing request gets Error Reply naming the category, siblings get normal replies |
| AC-07 error | `handler exceeding timeout yields timeout error reply and is cancelled` | e2e + unit | Timeout Error Reply, cancel signal delivered to Handler, Cycle ends within its deadline |
| AC-07 error | `timeout timer starts at dispatch, not at intake` | unit | Queued request waiting for a slot is not timed out before dispatch |
| AC-07b error | `failed or timed out handler is not run again` | integration | Handler invoked once, Requester holds the Error Reply from the first failure |
| AC-07c domain invariant | `same request key runs in arrival order, other keys in parallel` | unit | Per-key sequence preserved, different keys overlap in time |
| AC-07c domain invariant | `ordering per key holds across a full cycle on the real platform` | integration | Replies and Handler start order for one key match arrival order |
| AC-08 domain invariant | `oversized reply becomes error reply and cycle handlers are not re-run` | integration | Requester gets undeliverable Error Reply, other replies committed, Handlers not repeated |
| AC-08 domain invariant | `reply that cannot be encoded becomes error reply` | integration | Same outcome as oversized |
| AC-08 domain invariant | `reply rejected on send becomes error reply` | integration | Same outcome, no replay of the Cycle |
| AC-08b error | `error reply undeliverable because destination is down pauses the worker` | integration | Worker pauses, alert raised, stays in group, Handlers not re-run, resumes when destination returns |
| AC-09 authorization | `missing write permission on reply destination at startup stops intake` | integration | Configuration fault reported, no request accepted, no request lost, stays in group |
| AC-09 authorization | `permission revoked mid-cycle pauses without re-running handlers` | integration | Cycle in progress not re-executed, worker pauses and shows paused state |
| AC-09 authorization | `permission restored resumes intake automatically` | integration | Intake resumes with no restart, no lane reassignment |
| AC-10 happy path | `accepted requests across workers stay within budget in any one-second window` | integration + load | Max accepted in any sliding second at most budget plus 5 percent |
| AC-10 happy path | `request counts as accepted when handed to a handler` | unit | One allowance unit taken at hand-off, not at fetch |
| AC-10b domain invariant | `no allowance leaves requests unconsumed and unanswered` | integration | Remaining requests stay on the platform, no reply, picked up by a later Cycle |
| AC-10b domain invariant | `malformed or oversized request gets error reply without using allowance` | unit + integration | Error Reply sent, allowance counter unchanged |
| AC-11 happy path | `lane shares match weights when all lanes are busy` | unit + load | Each lane within 10 percentage points of its effective share |
| AC-12 domain invariant | `low-weight lane keeps at least its minimum share under heavy neighbours` | unit + integration | Lane served at 5 percent minimum, requests keep flowing |
| AC-13 domain invariant | `slow handler on low-weight lane does not hold requests past cycle deadline` | e2e | No request waits longer than the deadline, unfinished requests get Error Replies |
| AC-14 happy path | `rolling restart with returning identity causes no lane reassignment` | integration | Group membership change counter stays 0, other workers keep consuming |
| AC-15 error | `worker without explicit identity refuses to start` | unit | Refused, message explains a stable identity is required |
| AC-16 happy path | `consistency lag recorded per committed reply and per lane` | contract + integration | Metric present for every committed reply, labelled by lane |
| AC-17 error | `stall indicator raised when work pending and no commit past threshold` | unit + integration | Indicator raised after the stall threshold with pending work |
| AC-17 error | `idle worker raises no stall` | unit | No pending work, no indicator |
| AC-17 error | `pause shows its own state and suppresses stall` | unit | Paused state shown, stall indicator off |
| AC-18 cross-context | `allowance store outage stops intake and resumes without exceeding budget` | integration | Intake stops, worker stays in group, paused state shown, resumes on return, no burst above budget |
| AC-19 cross-context | `degraded downstream yields timeout error replies at bounded rate` | e2e | Requesters get Error Replies, accepted rate stays within budget, no worker retry |

## Edge cases / error paths

- Request with no correlation identifier → expected: Error Reply for a malformed request, no allowance used, worker keeps running.
- Request with no Request Key → expected: handled as its own key, runs in parallel with others.
- Weight that normalises below 5 percent → expected: lane raised to the minimum, others scaled down.
- Single lane configured → expected: lane gets the whole budget.
- Idle lane → expected: its share is redistributed to busy lanes (per open question default).
- Worker killed mid-Cycle before commit → expected: Cycle replayed on restart, no duplicated committed reply, same Idempotency Key.
- Worker killed after commit but before next fetch → expected: no replay, no duplicate.
- Commit fails as a whole (transaction aborted) → expected: no Handler re-run, outcome reconciled per request, zero lost or duplicated committed replies.
- Allowance returned for unconsumed requests (ADR-0006) → expected: counter restored, no leak of budget over time.
- Allowance store returns after outage with stale counter → expected: no burst above budget.
- Implausible request timestamp (future or negative lag) → expected: sample flagged and excluded from lag metric.
- Payload content → expected: never appears in logs, metrics or Error Replies.
- Error Reply content → expected: failure category and correlation only, no internal details.
- Returning worker outside the 45 s identity window → expected: lanes reassigned, runbook alert for stranded lane fires.
- Handler ignores the cancel signal and finishes side effects later → expected: Error Reply stands, late result discarded, no second reply.

## Test data

- Seed strategy: factories for request records (correlation identifier, Request Key, lane, payload size variants: normal, oversized, malformed) and for configuration (valid, and one invalid variant per AC-02 rule). Sample Handlers: fast, failing, slow, cancel-ignoring, side-effect-counting. No persistent entities, `data-model.md` is absent by design.
- Integration dependency: ephemeral real streaming platform and real shared allowance store as throwaway containers, NOT mocked. Time-dependent rules (30 s timeout, 60 s commit window, 45 s identity window, 60 s stall) run with a shortened configurable scale in tests; one rollout and one stall test run at full values pre-release.
- Cleanup boundary: per-test fresh lanes, reply destination and consumer group names (unique suffix), allowance counter reset; containers per suite and torn down after.

## NFR validation (load)

- Aggregate throughput ≥ 2,000 requests/s → sustain 2,000 requests/s on a group of workers with fast Handlers for 10 minutes, assert sustained accepted rate ≥ 2,000/s and no rise in error replies.
- Throughput with real key skew → replay the pilot service's key distribution at 2,000 requests/s for 10 minutes, assert target still met (open question on per-key ordering).
- Rate Budget accuracy ≤ budget × 1.10 → build a backlog of 5 minutes of traffic, start workers, assert accepted per second ≤ 1.10 × budget in every sliding 1 s window during 10 minutes of catch-up, including one worker restart.
- Priority Weight accuracy ±10 pp → three lanes, all busy, run 10 minutes at the budget, assert each lane's accepted-rate share within 10 pp of its effective share.
- Pause on limiter outage (new requests stop within 5 s, resume within 30 s) → cut the allowance store for 2 minutes under 1,000 requests/s, assert intake stops within 5 s and resumes within 30 s of return (integration scale, also run in the performance environment).
- Consistency Lag p95 and Stall detection: <!-- N/A: lag target is TBD (§8); stall threshold is covered by the integration test above -->

## Scale of the implemented tests (reduced, recorded in F9)

The integration and load tests in the repository run at a reduced scale so they fit a PR build. Full scale is a pre-release run.

| NFR test | Test-plan scale | Implemented default | Full scale |
|---|---|---|---|
| Rate Budget accuracy (`RateBudgetAccuracyIT`) | 5 min backlog, 10 min catch-up, 1 restart | 3,000 requests at 200/s (about 15 s), 3 workers, 1 restart | `./gradlew preReleaseTest -Dperf.rate.budget=2000 -Dperf.rate.backlog=600000` |
| Priority Weight accuracy (`WeightAccuracyIT`) | 10 min at the budget | window of 10 s of accepted requests at 2,000/s and at 200/s | not parameterised yet |
| Aggregate throughput (`ThroughputLoadIT`, tag `load`) | 2,000/s for 10 min | 2,000/s for 20 s, 4 workers; unique-key phase asserted | `./gradlew loadTest -Dload.seconds=600` |
| Time-dependent rules (30 s timeout, 60 s commit window, 45 s identity window, 60 s stall) | full values pre-release | shortened (2 to 10 s) | not run at full values yet |

Integration tests need Docker and fail without it; `-PskipDockerTests` is a local-only opt-out.

## CI placement

- On every PR: unit and contract suites.
- Integration suite (throwaway containers) on every PR when containers are available, otherwise nightly.
- On schedule / pre-release: e2e, failure-scenario suite at full time values, rollout test, load scenarios.
