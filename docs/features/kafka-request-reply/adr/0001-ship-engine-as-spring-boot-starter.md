---
status: Accepted
owner: "Ievgen Chupryna"
reviewers: ["Tech Lead", "Security Lead"]
updated_at: "2026-10-04"
feature_size: "M"
ticket: "none stated (spec §8)"
---

# 0001 — Ship the engine as a Spring Boot starter run inside each worker

- **Status:** Accepted
- **Date:** 2026-10-04
- **Deciders:** Ievgen Chupryna (Architect), decided with the author during the design walk

## Context

Several XME teams need the same worker shape and each writes its own consume loop. The feature must let a team supply only a Handler and configuration (US-01), and the repo today is a bare Spring Boot application skeleton.

The core of this decision was fixed in the spec and interview; the options marked "excluded by spec" record why the alternative does not stand, not an open choice.

## Decision drivers

- Spec §2: a second team stands up a worker by writing only a Handler and configuration
- Spec §3: internal starter only, no external API-stability promise
- Existing stack: Java 25, Spring Boot 4.1.1, Gradle

## Considered options

1. **Starter library embedded in each worker (library-sdk + worker)** — the engine auto-configures inside the adopting team's service; the public contract is the Handler interface and configuration.
2. **Standalone shared service** (excluded by spec §1) — teams send work to a central worker; Handlers cannot run in the owners' process or reach their databases directly.
3. **Thin layer over the framework's request-reply support** (excluded by interview, spec §1) — declined in the interview: covers only the basic reply path, not rate limiting, lanes, timeouts or atomic commit.

## Decision outcome

**Chosen:** Starter library embedded in each worker. Target surfaces are `library-sdk` (public Handler and configuration contract) and `worker` (the running engine). It matches the spec's handler-only adoption goal and keeps Handlers next to the services they call.

## Consequences

**Positive**
- Handler Owners keep their own deployment, scaling and downstream access
- One engine to fix and improve for all adopters

**Negative**
- Engine bugs reach every adopter on upgrade; version discipline is needed
- Public Handler and configuration signatures become a de facto contract even without a stability promise

**Neutral**
- The repo is a single application module for now; it is split into a `starter` module and an example worker when a second team adopts (§5, §11)

## Links

- Spec: [[../spec.md]]
- SAD: [[../sad.md]] §4
- Related ADR: none
