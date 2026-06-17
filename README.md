# market-data

FoundationX 行情接收与分发模块 — downstream dispatch port receiving-side module.

接收交易所 adapter（binance 等）已归一化的市场事件，执行接收侧校验、幂等判定、排序键约束和分发结果表达。

## 文档

- [goal.md](goal.md) — 模块定位、边界、非目标
- [SPEC.md](SPEC.md) — 23 节规格（DownstreamDispatchPort 契约，8 FR，6 BR）
- [TRACEABILITY.md](TRACEABILITY.md) — 完整追溯矩阵（FR/BR 100% 覆盖）
- [IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md) — 6 步 PR 序列，Runtime Gate

## 状态

v1.0.0 — runtime ready.

## 依赖

- `module/domain-market` — canonical MarketFactEnvelope/ProductLine/InstrumentKey
- `module/contracts` — wire / service contract

## 消费者

- `module/binance` — 通过 DownstreamDispatchPort 提交归一化行情事件
- 其他交易所 adapter

## 许可证

MIT
