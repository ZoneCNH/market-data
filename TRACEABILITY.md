# module/market-data TRACEABILITY

- Spec-Version: v0.1.1
- Last-Updated: 2026-06-17
- Status: Docs Baseline Published / Runtime Pending

Status semantics: `Baseline Published` 表示文档中的 dispatch/receiving contract 已对齐且可被下游引用；runtime 实现和测试为 Pending。

---

## §1 FR 追溯表

| FR ID | 功能需求 | AC | TC ID(s) | 实现状态 |
| --- | --- | --- | --- | --- |
| FR-MD-001 | dispatch-port：Binance adapter 完成事件归一化后提交事件 | AC-MD-001 | TC-MD-001 | Baseline Published |
| FR-MD-002 | canonical-input：接收侧输入必须引用 domain-market canonical `MarketEventEnvelope` 语义，不允许 Binance 原始 DTO 泄漏 | AC-MD-002 | TC-MD-002 | Baseline Published |
| FR-MD-003 | idempotency：同一 idempotencyKey 相同 payload 返回幂等 ack，不同 payload 返回 reject | AC-MD-003 | TC-MD-003 | Baseline Published |
| FR-MD-004 | ordering：同一 orderingKey 下检测 sequence 倒退、跳跃和重复 | AC-MD-003 | TC-MD-004 | Baseline Published |
| FR-MD-005 | quality-gate：eventTime/receivedAt/quality 不合法时 fail-closed | AC-MD-004 | TC-MD-005 | Baseline Published |
| FR-MD-006 | retry-classification：区分不可重试 reject 与可重试 failure | AC-MD-004 | TC-MD-006 | Baseline Published |
| FR-MD-007 | batch-semantics：批量提交逐条返回 outcome | AC-MD-005 | TC-MD-007 | Baseline Published |
| FR-MD-008 | observability：dispatch outcome 可按 venue/productLine/channel/outcome/reason 统计 | AC-MD-005 | TC-MD-008 | Baseline Published |

## §2 BR 追溯表

| BR ID | 业务规则 | 验证方式 | 实现状态 |
| --- | --- | --- | --- |
| BR-MD-001 | 不拥有交易所 adapter；Binance 原始响应只能停留在 `module/binance` adapter 边界内 | CI import check + spec boundary scan | Baseline Published |
| BR-MD-002 | 不拥有 canonical market entity；领域语义归 `module/domain-market` | CI type/lint check | Baseline Published |
| BR-MD-003 | 不拥有跨进程 wire schema；protobuf/gRPC/REST schema 归 `module/contracts` | spec lint | Baseline Published |
| BR-MD-004 | 接收侧对 contract、quality、idempotency 与 ordering 问题 fail-closed，不做静默修正 | 测试用例 | Baseline Published |
| BR-MD-005 | adapter 不得将 DispatchFailure 当作成功；必须按 retry policy 或上游 backpressure 处理 | 测试用例 | Baseline Published |
| BR-MD-006 | 文档批准前不得新增运行时代码、依赖、存储表或队列 topic | 文件变更审计 | Baseline Published |

## §3 NFR 追溯表

| NFR ID | 非功能需求 | 来源(SPEC §) | 验证方式 |
| --- | --- | --- | --- |
| NFR-MD-001 | 可审计性：每个 outcome 必须包含 outcome、reason、idempotencyKey、orderingKey 与 retryable 分类 | §7 | 审计日志检查 |
| NFR-MD-002 | 稳定性：v0.1.0 后 outcome 分类与幂等语义不得破坏性变更；变更需迁移说明 | §7 | version diff check |
| NFR-MD-003 | 可观测性：指标维度至少包含 venue、productLine、channel、outcome、reason | §7 | metrics 检查 |
| NFR-MD-004 | 边界纯净：本模块文档与后续 public API 不得暴露 vendor DTO、transport tag 或 storage tag | §7 | spec lint + API lint |

## §4 TC→FR 反向追溯

| TC ID | 覆盖 FR(s) | 测试类型 | 状态 |
| --- | --- | --- | --- |
| TC-MD-001 | FR-MD-001 | 文档引用检查 | Baseline Published |
| TC-MD-002 | FR-MD-002 | 边界扫描 | Baseline Published |
| TC-MD-003 | FR-MD-003, FR-MD-004 | 任务基线检查 | Baseline Published |
| TC-MD-004 | FR-MD-003, FR-MD-004 | 任务基线检查 | Baseline Published |
| TC-MD-005 | FR-MD-005 | TRACEABILITY 检查 | Baseline Published |
| TC-MD-006 | FR-MD-006 | TRACEABILITY 检查 | Baseline Published |
| TC-MD-007 | FR-MD-007 | TRACEABILITY 检查 | Baseline Published |
| TC-MD-008 | FR-MD-008 | TRACEABILITY 检查 | Baseline Published |
| TC-MD-009 | BR-MD-006 | 文件变更审计 | Baseline Published |

## §5 AC 注册表

| AC ID | 所属 FR/BR | AC 描述 | 验证方式 | 状态 |
| --- | --- | --- | --- | --- |
| AC-MD-001 | FR-MD-001, BR-MD-001 | Binance SPEC 可明确以 downstream dispatch port 作为行情事件交付边界，禁止直写下游 | 文档引用检查 | Baseline Published |
| AC-MD-002 | FR-MD-002, BR-MD-002 | 接收侧输入字段只引用 `ProductLine`、`InstrumentKey`、`MarketEventEnvelope` canonical 语义，不包含 Binance DTO 名称或原始响应字段 | 边界扫描 | Baseline Published |
| AC-MD-003 | FR-MD-003, FR-MD-004 | 幂等键与排序键规则已形成后续单元测试基线 | TC-MD-003, TC-MD-004 | Baseline Published |
| AC-MD-004 | FR-MD-005, FR-MD-006, BR-MD-004 | reject/failure 分类清晰区分 retryable | TC-MD-005, TC-MD-006 | Baseline Published |
| AC-MD-005 | FR-MD-007, FR-MD-008, NFR-MD-001, NFR-MD-003 | 批量 outcome 与观测维度覆盖 venue/productLine/channel/outcome/reason | TC-MD-007, TC-MD-008 | Baseline Published |
| AC-MD-006 | BR-MD-006 | 本次闭环只更新 markdown 文档，不新增运行时代码或依赖 | TC-MD-009 | Baseline Published |

## §6 覆盖率仪表盘

| 指标 | 数量 |
| --- | --- |
| 总 FR | 8 |
| 总 BR | 6 |
| 总 NFR | 4 |
| 总 TC | 9 |
| 总 AC | 6 |
| FR 覆盖率 | 100% (8/8) |
| BR 覆盖率 | 100% (6/6) |

## §7 变更历史

| 日期 | 版本 | 变更内容 | 作者 |
| --- | --- | --- | --- |
| 2026-06-17 | v0.1.0 | 初始 docs baseline：全部 FR/BR/NFR/TC/AC 定义（基于 SPEC v0.1.0 23-section 格式） | ZoneCNH |
| 2026-06-17 | v0.1.1 | 对齐 SPEC v0.1.1 audit fix：NFR 从 3 条性能/观测维度改为 4 条可审计性/稳定性/可观测性/边界纯净（匹配 SPEC §7 重构）；BR/FR/AC 描述与 SPEC §5-§8 对齐；§6 仪表盘 NFR 计数 3→4 | ZoneCNH |
