# API sync report: kafka-request-reply

Date 2026-10-04. Interface kind from `sad.md` `target_surfaces`: `library-sdk` + `worker` → `contracts/public-api.md` + `contracts/events.md`. No `openapi.yaml`: no HTTP surface. Size M (from `.size`). **`data-model.md` absent: legal fast-lane skip** (no schema change; the only store is a counter behind `AllowanceStore`, SAD flows 5-7). Fields derived from spec, ADRs and SAD, not from DDL.

## A. Field origins

| schema_path | origin | confidence |
|---|---|---|
| RequestContext.requestKey / correlationId | spec AC-04, SAD §8 ID strategy | medium (names: OQ-1) |
| RequestContext.idempotencyKey | SAD §8 (lane + partition + position), AC-03 | high |
| RequestContext.cancellation | spec AC-07 | high |
| ErrorCategory | spec §8 Q5 default | medium |
| config worker-identity | AC-15, ADR-0005 | high |
| config rate-budget-per-second, lanes[].weight, min-lane-share | AC-02, AC-10, AC-12, ADR-0003/0004 | high |
| config handler-timeout, cycle-deadline, commit-window, identity-window, stall-threshold | spec §6 (provisional) | medium |
| config commit-retry-attempts | ADR-0007 (3) | high |
| config property names and prefix | proposal, no repo convention | low |
| AllowanceStore.reserve/giveBack | ADR-0003, ADR-0006 | medium |
| meter names | SAD §7 metrics list | low |
| request.created_at | AC-16 Consistency Lag | medium |
| wire header vs body placement | none in sources | low |

## B. Drift checklist

1. Endpoint to data-model (core): ✓ N/A form. Every public item maps to a §4 story and the SAD §5 `api/` package; no entity needed.
2. Error code to repo registry (core): ✓ with note: no error registry in the repo, codes are the contract's proposal.
3. Validation to constraint (core): ✓ every rule traces to AC-02 / spec §6 (weight > 0, timeout < deadline ≤ 80% commit window, 5% min share).
4. Contract to sequence (supporting): ✓ flows 1-8 each have a contract counterpart. Flow 4 → startup codes; 5 → idempotency key; 6 → categories; 7 → AllowanceStore; 8 → `requestreply.state`.

Total flags: 3 (all declared `low`, none core failing, below the pause threshold).

## C. Back-feed coverage

| AC | Contract |
|---|---|
| 01, 04 | Handler, request/reply events |
| 02, 15 | config table + startup codes |
| 03 | `idempotencyKey` |
| 05 | events: commit and duplicate prevention |
| 06, 07, 07b, 08 | Error Reply, categories, Handler rules |
| 07c | Handler rules (per key, within lane) |
| 08b, 09 | runtime fault codes, `state` meter |
| 10, 10b, 11, 12, 13 | config (budget, weights, min share, deadline), `accepted` meter |
| 14 | `identity-window`, `membership.changes` |
| 16, 17 | `consistency.lag`, `state` |
| 18 | `AllowanceStore` fail closed |
| 19 | Handler timeout rule, Error Reply |

Every AC maps. No orphan sequence branch, no sequence gap found.

## D. Open questions (saved, not resolved)

- **OQ-1** (owner Tech Lead, due before the contract is finalized): confirm correlation id name, header vs body placement, wire format, Error Reply categories (malformed request → `failure`?), config prefix and meter names. This is spec §8 Q5.
- **OQ-2** (owner Tech Lead, via `specify`): AC-07c says "arrival order"; SAD §11 narrows it to "within a lane". Contract states within a lane. Spec wording to be tightened.
- **OQ-3** (owner Tech Lead): `AllowanceStore` public SPI shape depends on the Bucket4j choice (SAD §11).

Lint: no OpenAPI to lint. Proposed commit: `api: kafka-request-reply contract`.
