---
id: T15
title: "Wire the Spring Boot auto-configuration and startup permission check"
layer: "wiring"
deps: ["T3", "T4", "T5", "T6", "T13", "T14"]
blocks: ["T16", "T17", "T18"]
acs: ["AC-01", "AC-09"]
files_hint: ["src/main/java/xme/common/kfkprocessor/requestreply/autoconfig/RequestReplyAutoConfiguration.java", "src/main/resources/META-INF/spring/"]
owner: "<TBD lead>"
estimate: "M"
context_budget: "S"
status: "todo"
---

<!-- Self-contained task. Inlined slices are snapshots with provenance signatures; the source always wins.
To the executing agent: work from what is inlined here. If a slice is insufficient, ambiguous or contradicts the code, open the named file for the full text. Do not invent the missing part. -->

# T15: Wire the Spring Boot auto-configuration and startup permission check

## Place in the sequence

- **Blocked by:** T3 (Add configuration properties and startup validation), T4 (Implement the Redis-compatible AllowanceStore adapter), T5 (Implement the Kafka RequestLanes adapter with stable worker identity), T6 (Implement the transactional Kafka ReplySink), T13 (Implement the Cycle loop that ties intake, execution and commit together), T14 (Implement the Micrometer WorkerMetrics adapter) · **Blocks:** T16 (Add the failure-scenario test suite), T17 (Add rollout, weight-accuracy and throughput tests), T18 (Write the starter guide and operator runbook) · **Wave:** 5, after its dependencies
- **Lane:** own lane.

## Why (user story)

> **As a** Handler Owner
> **I want** to supply only a Handler and configuration
> **So that** I get a working request-reply worker without writing consumption, rate control or commit logic
>
> Source: `spec.md §4, US-01, verbatim` · full text: [spec.md](../spec.md)

Assembles the engine and its adapters from configuration plus the Handler bean, and pauses instead of failing when the worker may not write to the reply destination.

## Inlined context

> **Chosen:** Starter library embedded in each worker. Target surfaces are `library-sdk` (public Handler and configuration contract) and `worker` (the running engine). It matches the spec's handler-only adoption goal and keeps Handlers next to the services they call.
>
> Source: `adr/0001, Decision outcome, verbatim` · full text: [0001-ship-engine-as-spring-boot-starter.md](../adr/0001-ship-engine-as-spring-boot-starter.md)

> web and REST client dependencies are not part of the starter contract and must not be pulled into adopters.
>
> Source: `sad.md §2, Override note, abridged` · full text: [sad.md](../sad.md)

> └── autoconfig/   Spring Boot auto-configuration, startup validation: configuration (AC-02), explicit unique worker identity (AC-15, ADR-0005), permission to write the reply destination (AC-09)
>
> Source: `sad.md §5, internal decomposition, verbatim` · full text: [sad.md](../sad.md)

> `Engine->>Platform: checks permission to read lanes and write the reply destination`
> `Engine->>Platform: joins group with the configured identity and starts the first Cycle`
>
> Source: `sad.md §6, flow 4 «startup validation and permission», abridged` · full text: [sad.md](../sad.md)

**Fallback:** insufficient or contradicted by the code → read the named file in full ([spec.md](../spec.md) · [sad.md](../sad.md) · [public-api.md](../contracts/public-api.md) · [events.md](../contracts/events.md) · [adr/](../adr/)) and follow it. Do not guess.

## Data delta

No DB changes.

## API contract

- A worker is built from a Handler bean and `xme.request-reply.*` properties only. Runtime fault `request_reply.reply_destination.permission_denied` (AC-09).

: `contracts/public-api.md §1, 3, abridged` · full text: [public-api.md](../contracts/public-api.md)

## Acceptance criteria

### AC-01: happy path

> **Given** a Handler Owner has supplied a Handler and configured the Priority Lanes, the Rate Budget and the reply destination
> **When** the worker group starts and a Requester sends a request
> **Then** the Requester receives the Handler's reply carrying the same correlation identifier as the request
>
> Source: `spec.md §5, AC-01, verbatim` · full text: [spec.md](../spec.md)

### AC-09: authorization

> **Given** the worker has no permission to write to the reply destination
> **When** it starts or a Cycle tries to send replies
> **Then** the worker stops accepting requests, reports a configuration fault to the Operator, loses no request, stays in its group (no lane reassignment) and resumes automatically once the permission is restored, without running the Handlers of a Cycle in progress again
>
> Source: `spec.md §5, AC-09, verbatim` · full text: [spec.md](../spec.md)

## Checklist

- [ ] Create `RequestReplyAutoConfiguration` in `src/main/java/xme/common/kfkprocessor/requestreply/autoconfig/` and register it in `src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- [ ] Wire properties, `StartupValidator` (T3), adapters (T4, T5, T6, T14), `Intake`, `HandlerExecutor`, `CycleCommitter`, `CycleLoop` and start the loop
- [ ] At start check permission to read lanes and write the reply destination; if missing, start in paused(permission), report a configuration fault, stay in the group, resume on restore
- [ ] Keep web and REST client types out of every class the auto-configuration exposes
- [ ] `@SpringBootTest` under `src/test/java/xme/common/kfkprocessor/requestreply/autoconfig/` with Testcontainers Kafka: a Handler bean plus properties starts a worker that answers one request

## Edge cases

| Case | Behaviour |
|---|---|
| No Handler bean | Auto-configuration refuses to start via T3 validation. |
| Permission to write the destination missing at start | Worker starts paused with a configuration fault, accepts no requests, loses none, resumes when restored. |
| Adopting service has no web starter | Auto-configuration still loads; no web class on the starter classpath path. |

## Definition of Done

- [ ] `@SpringBootTest` proves one request gets a reply with the same correlation id
- [ ] Permission-denied start leaves the worker paused and in its group
- [ ] every Hard Rule inlined above still holds
- [ ] lint + vet clean (`./gradlew build`)
