# market-data 规格

- Status: Approved
- Spec-Version: v1.0.0
- Last-Updated: 2026-06-17
- Layer: L3 行情摄取与分发
- Module-Version: v1.0.0-spec
- Related: `module/binance`, `module/domain-market`, `module/contracts`

> 本文件发布 downstream dispatch port / receiving-side SPEC 的文档基线，不引入运行时代码、依赖或 wire schema。`market-data` 的运行时实现仍为 Pending；上游 `module/domain-market` 与 `module/contracts` 的 docs-only 契约基线已补充完毕（ProductLine/InstrumentKey/MarketFactEnvelope + §8.4 Ingestion Contract），运行时冻结待各模块发布。

---

## 1. 摘要

`market-data` 是交易所行情 adapter 与内部行情消费链路之间的接收侧模块。它接收 adapter 已归一化的市场事件，执行接收侧校验、幂等判定、排序键约束和分发结果表达，并向上游 adapter 返回可审计的 dispatch outcome。

`module/binance` 在采集 Binance 原始数据后，不直接写入存储、队列或策略入口；它必须通过本规格定义的 downstream dispatch port 将归一化事件交给 `market-data` 接收侧。

## 2. 边界

| 类型 | 说明 |
| --- | --- |
| Owns | downstream dispatch port 语义、接收侧校验、幂等键约束、排序键约束、ack/reject/failure 分类、分发可观测性要求 |
| Depends on | `module/domain-market` canonical market event 语义；`module/contracts` wire / service contract（后续实现阶段） |
| Consumed by | `module/binance` 与其他交易所 adapter |
| Excludes | 交易所 HTTP/WS adapter、provider DTO、protobuf/gRPC/REST schema、Kafka/NATS/DB 实现、策略/回测/执行逻辑 |

## 3. 术语

| 术语 | 定义 |
| --- | --- |
| AcceptedMarketEvent | adapter 已完成来源归一化、时间标注与基础合法性检查后交给 `market-data` 的事件载荷。它必须引用 `domain-market` canonical `MarketFactEnvelope` 语义，不携带交易所原始 DTO。 |
| DownstreamDispatchPort | adapter 调用 `market-data` 接收侧的抽象端口；用于提交单条或批量事件并获取接收结果。 |
| DispatchAck | 接收侧确认事件已被接受，可由后续持久化/队列/消费链路处理。 |
| DispatchReject | 接收侧拒绝事件；通常为不可重试的契约、质量或幂等冲突问题。 |
| DispatchFailure | 接收侧暂时无法完成接收；调用方可按 retry policy 重试。 |
| IdempotencyKey | 同一 venue、product line、instrument、channel、event time、source sequence 与 payload fingerprint 组合出的稳定去重键。 |
| OrderingKey | 保证同一 venue + product line + instrument + channel 内事件顺序约束的分区键。 |
| RejectReasonMapping | binance adapter 的 native reject（如 `terminal_validation`、`unauthorized`、`rate_limited`、`server_unavailable`）在 dispatch 适配层映射为 market-data 统一分类（见 §4.4.1）的规则。映射由 adapter 负责，market-data 接收侧只处理统一分类。 |

## 4. downstream dispatch port 契约

### 4.1 端口语义

`DownstreamDispatchPort` 是文档级端口名称，不是本任务要新增的代码接口。后续实现可用本语义映射为进程内接口、消息总线生产者或 RPC client，但不得改变以下语义：

```text
Dispatch(ctx, AcceptedMarketEvent) -> DispatchOutcome
DispatchBatch(ctx, []AcceptedMarketEvent) -> []DispatchOutcome
```

### 4.2 输入字段要求

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| venue | 是 | 交易所或来源场所，例如 Binance；取值归属由上游 contract 冻结。 |
| productLine | 是 | 现货、U 本位合约、币本位合约等产品线；不得使用 adapter 私有枚举。 |
| instrumentKey | 是 | canonical instrument 标识；不得直接使用未归一化 symbol 作为唯一键。 |
| channel | 是 | trade、kline、bookTicker、depth、funding 等来源通道。 |
| eventTime | 是 | 交易所事件时间；缺失或晚于 receivedAt 超出容忍窗口时拒绝。 |
| receivedAt | 是 | adapter 接收时间；用于延迟、stale 与 future gate。 |
| sourceSequence | 条件必填 | 通道存在 sequence/update id 时必须提供。 |
| payload | 是 | 引用 `domain-market` canonical `MarketFactEnvelope` 语义的载荷，不允许原始 Binance DTO。 |
| quality | 是 | 来源质量、延迟、可靠性与降级原因。 |
| idempotencyKey | 是 | 稳定去重键；重复提交必须产生可审计 outcome。 |
| orderingKey | 是 | 同一排序域内事件必须可串行化处理。 |
| source | 是 | 上游 adapter 标识（如 `"binance"`），用于指标分组、审计追踪和多 adapter 来源区分。 |

> 以上共 12 个字段（含 1 个条件必填）。

#### 4.2.1 跨模块字段命名映射

market-data 文档使用 camelCase 风格描述字段语义；在下游实现中，字段命名必须与 `domain-market` Go 类型、`contracts` JSON tag 保持一致。以下为各方命名对照：

| 概念 | market-data (文档) | domain-market (Go 字段) | contracts (JSON tag) | binance (§9) |
| --- | --- | --- | --- | --- |
| 交易所/场所 | venue | Venue | source (**MarketEvent.Source**) | exchange |
| 产品线 | productLine | ProductLine | product_line | product_line |
| 标的标识 | instrumentKey | InstrumentKey | instrument_key | instrument_key |
| 数据通道 | channel | (**domain-market 不定义**) | — | (**parser 内部**) |
| 事件载荷 | payload (MarketFactEnvelope) | MarketEventEnvelope | payload | MarketFactEnvelope |
| 事件时间 | eventTime | EventTime | timestamp | decision_time |
| 接收时间 | receivedAt | ReceivedAt | — | (**adapter 内部**) |
| 源序列号 | sourceSequence | (**domain-market 不定义**) | source_sequence | — |
| 质量标签 | quality | MarketDataQuality | — | (**adapter 内部**) |
| 幂等键 | idempotencyKey | (**market-data 内部**) | idempotency_key | idempotency_key |
| 排序键 | orderingKey | (**market-data 内部**) | — | — |
| 来源适配器 | source | (**market-data 内部**) | — | — |

> **命名约束**: Go 代码中 struct 字段使用 PascalCase（domain-market 拥有），JSON 序列化使用 snake_case（contracts BR-009 强制），文档表格使用 camelCase（本 SPEC 惯例）。实现时必须从 domain-market import 对应类型，不得在 market-data 内部重新定义同名类型。

### 4.3 输出结果

| Outcome | 可重试 | 语义 |
| --- | --- | --- |
| DispatchAck | 否 | 事件被接收侧接受；重复提交同一 idempotencyKey 可返回幂等 ack。 |
| DispatchReject | 否 | 事件违反契约、质量门禁、幂等冲突或排序不可恢复；adapter 不得无限重试。 |
| DispatchFailure | 是 | 接收侧内部暂时不可用、背压或下游依赖不可用；adapter 可按退避策略重试。 |

### 4.4 拒绝原因分类

| Reject Reason | 触发条件 | 对标 binance §9 reject classification |
| --- | --- | --- |
| contract_violation | 缺少必填字段、枚举不在 canonical contract、payload 类型不匹配。 | terminal_validation |
| quality_rejected | stale、future、dirty、latency 超限或 quality 不可靠。 | terminal_validation（子类） |
| idempotency_conflict | 同一 idempotencyKey 对应不同 payload fingerprint。 | terminal_conflict |
| ordering_violation | 同一 orderingKey 下 sourceSequence 倒退且不可恢复。 | terminal_validation（子类） |
| unsupported_channel | channel 尚未纳入 `market-data` 接收侧支持矩阵。 | terminal_validation（子类） |
| unauthorized | adapter 凭证无效或权限不足；由上游 adapter 验证并映射，本层收到后直接拒绝。 | unauthorized |
| rate_limited | 上游 adapter 或接收侧自身频率超限；adapter 应按退避策略重试，不属于无限重试。 | rate_limited |
| server_unavailable | 接收侧内部依赖（持久化、队列）不可用；adapter 应退避重试。<br/>**产出: DispatchFailure（非 DispatchReject），可重试。** 与 generic DispatchFailure 的区别：本分类表示已知的临时不可用原因，方便监控和告警分类。 | server_unavailable |

> 以上共 8 种 reject reason。binance adapter 的 dispatch 适配层负责将 binance-native 6 种分类映射为上述 8 种中的对应项（`retryable` 根据 context 转 `DispatchFailure`）。映射规则见 §4.4.1。

#### 4.4.1 binance-native reject 到 market-data 映射规则

| binance §9 分类 | market-data outcome / reason | 说明 |
| --- | --- | --- |
| retryable | DispatchFailure | 不映射到 reject reason；转 failure 让 adapter 重试 |
| terminal_validation | DispatchReject（子类: contract_violation / quality_rejected / ordering_violation / unsupported_channel） | 按具体子类细分，共 4 种 market-data reject reason 对应 binance terminal_validation |
| terminal_conflict | DispatchReject / idempotency_conflict | 直接映射 |
| unauthorized | DispatchReject / unauthorized | 直接映射 |
| rate_limited | DispatchReject / rate_limited | 直接映射 |
| server_unavailable | DispatchFailure | binance 侧不可用 = 非 adapter 错误，adapter 应重试 |

## 5. 功能需求

| ID | 需求 | WHEN | THEN |
| --- | --- | --- | --- |
| FR-MD-001 | dispatch-port | Binance adapter 完成事件归一化后提交事件 | 必须调用 downstream dispatch port，不得绕过 `market-data` 直写存储、队列或策略入口。 |
| FR-MD-002 | canonical-input | 接收侧读取事件载荷 | 载荷必须引用 `domain-market` canonical `MarketEventEnvelope` 语义（Go 类型 `MarketEventEnvelope`，即 `MarketFactEnvelope` 的 canonical 名称），不允许 Binance 原始 DTO 泄漏。 |
| FR-MD-003 | idempotency | 接收同一 idempotencyKey | 相同 payload fingerprint 返回幂等 ack；不同 fingerprint 返回 reject。 |
| FR-MD-004 | ordering | 接收同一 orderingKey 的带序列事件 | 必须检测 sequence 倒退、跳跃和重复，并返回明确 outcome。 |
| FR-MD-005 | quality-gate | eventTime、receivedAt 或 quality 不合法 | 必须 fail-closed，返回 reject，且记录原因分类。 |
| FR-MD-006 | retry-classification | 接收侧无法完成处理 | 必须区分不可重试 reject 与可重试 failure。 |
| FR-MD-007 | batch-semantics | 批量提交事件 | 必须逐条返回 outcome，不允许整批成功掩盖单条失败。 |
| FR-MD-008 | observability | 任一 dispatch 调用完成 | 必须可按 venue/productLine/channel/outcome/reason 统计。 |

## 6. 行为约束

| ID | 规则 |
| --- | --- |
| BR-MD-001 | `market-data` 不拥有交易所 adapter；Binance 原始响应只能停留在 `module/binance` adapter 边界内。 |
| BR-MD-002 | `market-data` 不拥有 canonical market entity；领域语义归 `module/domain-market`。 |
| BR-MD-003 | `market-data` 不拥有跨进程 wire schema；protobuf/gRPC/REST schema 归 `module/contracts`。 |
| BR-MD-004 | 接收侧对 contract、quality、idempotency 与 ordering 问题 fail-closed，不做静默修正。 |
| BR-MD-005 | adapter 不得将 DispatchFailure 当作成功；必须按 retry policy 或上游 backpressure 处理。 |
| BR-MD-006 | 文档批准前不得新增运行时代码、依赖、存储表或队列 topic。 |

## 7. 非功能需求

| ID | 类别 | 需求 |
| --- | --- | --- |
| NFR-MD-001 | 可审计性 | 每个 outcome 必须包含 outcome、reason、idempotencyKey、orderingKey 与 retryable 分类。 |
| NFR-MD-002 | 稳定性 | v0.1.0 后 outcome 分类与幂等语义不得破坏性变更；变更需迁移说明。 |
| NFR-MD-003 | 可观测性 | 指标维度至少包含 venue、productLine、channel、outcome、reason。 |
| NFR-MD-004 | 边界纯净 | 本模块文档与后续 public API 不得暴露 vendor DTO、transport tag 或 storage tag。 |

## 8. Acceptance Criteria Registry

| AC ID | FR/BR Ref | Criterion | Verification | Status |
| --- | --- | --- | --- | --- |
| AC-MD-001 | FR-MD-001, BR-MD-001 | Binance SPEC 可明确以 downstream dispatch port 作为行情事件交付边界，禁止直写下游。 | 文档引用检查 | Baseline Published |
| AC-MD-002 | FR-MD-002, BR-MD-002 | 接收侧输入字段只引用 `ProductLine`、`InstrumentKey`、`MarketEventEnvelope` canonical 语义，不包含 Binance DTO 名称或原始响应字段。 | 边界扫描 | Baseline Published |
| AC-MD-003 | FR-MD-003, FR-MD-004 | 幂等键与排序键规则已形成后续单元测试基线。 | 任务基线检查 | Baseline Published |
| AC-MD-004 | FR-MD-005, FR-MD-006, BR-MD-004 | reject/failure 分类清晰区分 retryable。 | TRACEABILITY 检查 | Baseline Published |
| AC-MD-005 | FR-MD-007, FR-MD-008, NFR-MD-001, NFR-MD-003 | 批量 outcome 与观测维度覆盖 venue/productLine/channel/outcome/reason。 | TRACEABILITY 检查 | Baseline Published |
| AC-MD-006 | BR-MD-006 | 本次闭环只更新 markdown 文档，不新增运行时代码或依赖。 | `git diff --check` + 文件列表检查 | Baseline Published |

## 9. 后续实现门禁

| 门禁 | 要求 | 当前状态 |
| --- | --- | --- |
| Contract Gate | `module/contracts` 批准对应 wire schema（§8.4 ingestion contract）或明确无需跨进程 wire schema。 | Docs baseline present — contracts §8.4 已补充（docs-only），运行时 wire schema 待后续批准 |
| Domain Gate | `module/domain-market` 批准 `ProductLine`、`InstrumentKey`、`MarketEventEnvelope`、quality 语义。 | Docs baseline present — domain-market 已定义 ProductLine/InstrumentKey/MarketFactEnvelope（docs-only），运行时冻结待后续批准 |
| Adapter Gate | `module/binance` SPEC 引用本 dispatch port，且不再将下游交付语义留空。 | 已确认 — binance OQ-001 已关闭（contracts §8.4 wire types defined），OQ-002 已关闭（market-data SPEC v1.0.0 已定义 DownstreamDispatchPort 语义、12 字段、8 种 reject reason 及 binance reject 映射规则）；binance SPEC 已引用 market-data downstream dispatch port |
| Reject Mapping Gate | binance-native reject classification 到 market-data §4.4.1 的映射规则已文档化。 | Baseline Published（本次新增） |
| Naming Mapping Gate | 跨模块字段命名映射表（§4.2.1）已纳入 SPEC。 | Baseline Published（本次新增） |
| Test Gate | 后续实现必须覆盖幂等、排序、quality fail-closed、retry classification 与 batch partial failure。 | Pending — 无运行时代码 |

## 10. 发布状态与运行时边界

| 项目 | 状态 | 说明 |
| --- | --- | --- |
| DownstreamDispatchPort docs baseline | Published | 本 SPEC 已定义端口语义、输入字段、outcome、reject reason、FR/BR/NFR/AC 与后续实现门禁。 |
| Receiving-side SPEC baseline | Published | 接收侧 fail-closed、idempotency、ordering、quality gate、batch outcome 与 observability 语义已可被 `module/binance` 引用。 |
| Runtime implementation | Pending | 本次不新增 Go 源码、依赖、wire schema、存储表、队列 topic 或运行时测试声明。 |
| Canonical domain dependency | External / Docs baseline present | `ProductLine`、`InstrumentKey`、`MarketEventEnvelope`（= `MarketFactEnvelope`）语义由 `module/domain-market` 拥有，docs-only 类型定义已补充；运行时冻结待 domain-market 发布。 |
| Cross-module naming alignment | Baseline Published | §4.2.1 已建立跨模块字段命名映射表。 |
| Binance reject mapping | Baseline Published | §4.4.1 已建立 binance-native → market-data reject 映射规则。 |

### 10.1 Runtime Pending → Published 推进检查清单

在将本模块状态从 `Runtime Pending` 转为 `Published` 之前，必须逐项确认：

- [x] **Contract Gate 通过**: `module/contracts/SPEC.md` 的 §8.4 ingestion contract（`MarketDataService` / `IngestRequest` / `IngestResult`）已补充（docs baseline），运行时 proto 编译待后续阶段。
- [x] **Domain Gate 通过**: `module/domain-market/SPEC.md` v1.0.1 已定义 `ProductLine`(spot/um_perp/cm_perp/option)、`InstrumentKey`(11字段)、`MarketFactEnvelope`(=MarketEventEnvelope) 类型，语义已补充（docs baseline）；运行时冻结待 domain-market 发布。
- [x] **Adapter Gate 通过**: `module/binance/SPEC.md` 的 OQ-001（contracts wire 就绪？）已关闭（contracts §8.4）；OQ-002（market-data dispatch port 就绪？）已关闭（market-data SPEC v1.0.0 DownstreamDispatchPort 语义 + 12 字段 + 8 种 reject reason + binance reject 映射规则）。
- [ ] **Reject Mapping 验证**: `module/binance` 的 dispatch 适配层可引用 §4.4.1 映射规则实现 reject 转换。
- [ ] **Naming Mapping 验证**: 跨模块字段命名无残余矛盾；Go 代码中 struct 字段来自 domain-market import，JSON 序列化遵循 contracts BR-009 snake_case。
- [ ] **Test Gate 通过**: TC-MD-003 至 TC-MD-008 的运行时测试已实现并通过。
- [ ] `module/binance` 不再将 `module/contracts`、`module/domain-market`、`module/market-data` 列为 Blocking。

> 以上全部满足后，本模块状态可推进为 `Published`。

---

## 变更历史

| 日期 | 版本 | 变更内容 | 作者 |
| --- | --- | --- | --- |
| 2026-06-16 | v0.1.0 | 初始文档基线：downstream dispatch port、接收侧 SPEC、FR/BR/NFR/AC、后续实现门禁 | ZoneCNH |
| 2026-06-17 | v0.1.1 | 审计修复：补充跨模块字段命名映射 §4.2.1；reject 分类扩充至 8 种并对齐 binance；新增 binance-native → market-data reject 映射规则 §4.4.1；补充 source 字段（12 字段完整）；新增状态转换 checklist §10.1；FR-MD-002 引用改为 canonical `MarketEventEnvelope` | ZoneCNH |
