# module/market-data IMPLEMENTATION PLAN

## 1. Goal

Deliver `module/market-data` v1.0.0 as the exchange-neutral downstream dispatch receiving module.

## 2. Current State

- Docs baseline published (v0.1.1): DownstreamDispatchPort semantics, AcceptedMarketEvent contract (12 fields), 8 reject reasons with binance-native mapping, cross-module naming mapping, FR-MD-001~008 / BR-MD-001~006 / NFR-MD-001~004 / AC-MD-001~006
- SPEC.md: merged to main via PR #545 (audit fix: naming alignment, reject mapping, ingestion contract)
- TRACEABILITY.md / goal.md / IMPLEMENTATION-PLAN.md: PR-000 root docs (this PR)
- Runtime: Pending
- Upstream dependencies: module/domain-market canonical types, module/contracts wire contract (§8.4)

## 3. PR Sequence

```text
PR-000 module/market-data root docs (this PR)                  ← DONE (SPEC merged #545; TRACEABILITY/goal/IMPL-PLAN here)
PR-001 runtime: dispatch port interface + no-op implementation
PR-002 runtime: idempotency store
PR-003 runtime: quality gate
PR-004 runtime: ordering enforcement
PR-005 runtime: observability metrics
PR-006 runtime: contract tests + integration tests
```

## 4. PR-000 Root Docs

Scope: Create `module/market-data/` with SPEC.md, TRACEABILITY.md, goal.md, IMPLEMENTATION-PLAN.md.

Acceptance:
- [x] SPEC.md published (v0.1.1, merged via PR #545) — includes 10-section format, §4.2.1 cross-module naming mapping, §4.4.1 binance-native reject mapping, §9 implementation gates, §10.1 runtime checklist
- [x] TRACEABILITY.md aligned with SPEC v0.1.1 — all FR/BR/NFR/TC/AC with closed traceability chains, NFR count 4
- [x] Adapter Gate: module/binance SPEC references this dispatch port (verified: binance SPEC lines 10, 26-27, 79-80, 461, 735-741)
- [x] No runtime code introduced
- [x] goal.md updated to v0.1.1

## 5. Runtime Gate

Runtime implementation starts when:
- [x] module/contracts provides approved IngestRequest/IngestResult types (§8.4 merged via #545)
- [x] module/domain-market provides approved, consumable ProductLine/InstrumentKey/MarketFactEnvelope types (v1.0.1, canonical types + §10.1 Binance C/S ingestion semantics frozen)
- [x] module/binance server OQ-001 (contracts wire ready?) closed — contracts §8.4 defines all wire types
- [x] module/binance server OQ-002 (market-data dispatch port ready?) closed — market-data SPEC v0.1.1 defines DownstreamDispatchPort semantics, 12 input fields, 8 reject reasons with binance reject mapping
- [ ] All SPEC §9 implementation gates pass

## 6. DoD

- [ ] All FR-MD-001~008 implemented and tested
- [ ] Idempotency: duplicate key → deterministic outcome
- [ ] Ordering: sequence gap/reversal detected
- [ ] Quality gate: stale/future/dirty → fail-closed
- [ ] Observability: metrics by venue/productLine/channel/outcome/reason
- [ ] Contract tests pass
- [ ] Coverage >= 80%
