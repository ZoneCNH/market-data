# goal: market-data v1.0.0

- Status: Approved
- Created: 2026-06-17
- Updated: 2026-06-17
- Owner: ZoneCNH
- Related SPEC: module/market-data/SPEC.md v1.0.0

## 定位

`market-data` 是交易所行情 adapter 与内部行情消费链路之间的接收侧模块。接收 adapter 已归一化的市场事件，执行接收侧校验、幂等判定、排序键约束和分发结果表达。

## 目标

- 定义 DownstreamDispatchPort 接收侧端口语义
- 定义 AcceptedMarketEvent 输入契约（12 字段，含跨模块命名映射）
- 定义 DispatchAck/DispatchReject/DispatchFailure 三级 outcome 分类（8 种 reject reason，含 binance-native 映射）
- 提供 FR-MD-001~008 / BR-MD-001~006 / NFR-MD-001~004 完整需求覆盖
- 暴露按 venue/productLine/channel/outcome/reason 维度的可观测性要求
- 建立跨模块字段命名映射表（contracts JSON ↔ domain-market Go ↔ market-data doc）

## 边界

| 类型 | 说明 |
| --- | --- |
| Owns | downstream dispatch port 语义、接收侧校验、幂等键约束、排序键约束、ack/reject/failure 分类、binance-native reject 映射、分发可观测性要求 |
| Depends on | `module/domain-market` canonical MarketEventEnvelope/ProductLine/InstrumentKey 语义；`module/contracts` IngestRequest/IngestResult wire contract |
| Consumed by | `module/binance` server（通过 DownstreamDispatchPort 提交 canonical events）；其他交易所 adapter |
| Excludes | 交易所 HTTP/WS adapter（归 adapter 模块）、gRPC/protobuf schema（归 contracts）、Kafka/NATS/DB 实现、策略/回测/执行逻辑、query API、storage engine |

## 不做什么

- 不实现 transport adapter（HTTP、WebSocket、Kafka producer/consumer）
- 不定义 proto/gRPC schema（由 `module/contracts` 拥有）
- 不拥有 storage engine
- 不暴露 query API
- 不实现策略、因子或回测逻辑
- 不连接远程服务、不读取密钥
- 文档批准前不新增运行时代码、依赖、存储表或队列 topic

## 后续实现门禁

Runtime 实现必须在以下全部门禁通过后启动：
- Contract Gate: `module/contracts` §8.4 ingestion contract 已批准
- Domain Gate: `module/domain-market` ProductLine / InstrumentKey / MarketEventEnvelope 语义已冻结
- Adapter Gate: `module/binance` OQ-001（contracts wire）和 OQ-002（market-data dispatch port）已关闭
- Reject Mapping Gate: binance-native → market-data reject 映射已在 SPEC §4.4.1 定义
- Naming Mapping Gate: 跨模块字段命名映射已在 SPEC §4.2.1 定义
