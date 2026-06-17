package dispatch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// SinkPort 是存储写入端口（interface 注入，不连真实 TD/Kafka）。
// 生产实现双写 TDengine + Kafka；测试用 RecordingSink。
type SinkPort interface {
	Write(ctx context.Context, event AcceptedMarketEvent) error
}

// IdempotencyStore 是幂等键存储端口。
type IdempotencyStore interface {
	// CheckAndSet 检查 key 是否已存在。
	// 返回值：
	//   isNew=true: key 不存在，已设置 fingerprint，是首次提交。
	//   isNew=false, isSame=true: key 已存在且 fingerprint 相同（幂等重发）。
	//   isNew=false, isSame=false: key 已存在但 fingerprint 不同（冲突）。
	CheckAndSet(ctx context.Context, key, fingerprint string) (isNew bool, isSame bool, err error)
}

// OrderingTracker 跟踪同一 orderingKey 的 sourceSequence。
type OrderingTracker interface {
	// Check 接收 orderingKey + sourceSequence，返回是否合法（单调递增）。
	Check(ctx context.Context, orderingKey string, seq int64) (ok bool, err error)
}

// Receiver 实现 DownstreamDispatchPort（接收侧）。
// 它对 adapter 提交的 AcceptedMarketEvent 执行校验/幂等/排序/质量门禁，
// 通过的经 SinkPort 写入存储。
type Receiver struct {
	sink       SinkPort
	idempotency IdempotencyStore
	ordering   OrderingTracker
	mu         sync.Mutex

	// supportedChannels 是允许的 channel 集合（§4.4 unsupported_channel 门禁）。
	supportedChannels map[string]bool

	// metrics 回调（FR-MD-008 observability）。
	metricsFn func(DispatchMetrics)
}

// ReceiverOption 配置 Receiver。
type ReceiverOption func(*Receiver)

// WithMetrics 设置 metrics 回调。
func WithMetrics(fn func(DispatchMetrics)) ReceiverOption {
	return func(r *Receiver) { r.metricsFn = fn }
}

// WithSupportedChannels 设置允许的 channel 集合。
func WithSupportedChannels(channels ...string) ReceiverOption {
	m := make(map[string]bool, len(channels))
	for _, c := range channels {
		m[c] = true
	}
	return func(r *Receiver) { r.supportedChannels = m }
}

// NewReceiver 创建接收侧。
func NewReceiver(sink SinkPort, idem IdempotencyStore, ord OrderingTracker, opts ...ReceiverOption) *Receiver {
	r := &Receiver{
		sink:       sink,
		idempotency: idem,
		ordering:   ord,
		supportedChannels: map[string]bool{
			"trade": true, "quote": true, "kline": true,
			"orderbook": true, "funding": true,
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Dispatch 处理单条事件（FR-MD-001）。
func (r *Receiver) Dispatch(ctx context.Context, event AcceptedMarketEvent) (DispatchOutcome, error) {
	start := time.Now()
	outcome := r.process(ctx, event)
	r.recordMetric(event, outcome, time.Since(start))
	return outcome, nil
}

// DispatchBatch 批量处理（FR-MD-007：逐条返回 outcome）。
func (r *Receiver) DispatchBatch(ctx context.Context, events []AcceptedMarketEvent) ([]DispatchOutcome, error) {
	outcomes := make([]DispatchOutcome, len(events))
	for i, event := range events {
		start := time.Now()
		outcomes[i] = r.process(ctx, event)
		r.recordMetric(event, outcomes[i], time.Since(start))
	}
	return outcomes, nil
}

// process 执行完整的接收侧流水线。
func (r *Receiver) process(ctx context.Context, event AcceptedMarketEvent) DispatchOutcome {
	// FR-MD-005 quality gate（fail-closed）
	if err := r.validateQuality(event); err != reason("") {
		return reject(event, RejectQualityRejected)
	}

	// §4.4 contract_violation（必填字段校验）
	if err := r.validateContract(event); err != reason("") {
		return reject(event, RejectContractViolation)
	}

	// §4.4 unsupported_channel
	if len(r.supportedChannels) > 0 && !r.supportedChannels[event.Channel] {
		return reject(event, RejectUnsupportedChannel)
	}

	// FR-MD-003 idempotency（payload fingerprint）—— 先于 ordering
	fp := fingerprint(event)
	isNew, isSame, idemErr := r.idempotency.CheckAndSet(ctx, event.IdempotencyKey, fp)
	if idemErr != nil {
		// FR-MD-006 retry classification：存储不可用 = failure（可重试）
		return DispatchFailure{EventID: event.IdempotencyKey, Reason: idemErr.Error()}
	}
	if !isNew && isSame {
		// 幂等重发：相同 key + 相同 payload → 幂等 ack，不走后续流水线
		return DispatchAck{
			EventID:        event.IdempotencyKey,
			IdempotencyKey: event.IdempotencyKey,
			Durable:        true,
		}
	}
	if !isNew && !isSame {
		// 幂等冲突：相同 key 但不同 payload → reject
		return reject(event, RejectIdempotencyConflict)
	}

	// FR-MD-004 ordering（sequence 单调）—— 幂等通过后再检查
	if ok, _ := r.ordering.Check(ctx, event.OrderingKey, event.SourceSequence); !ok {
		return reject(event, RejectOrderingViolation)
	}

	// 写入存储
	if writeErr := r.sink.Write(ctx, event); writeErr != nil {
		// FR-MD-006：存储写入失败 = failure（可重试）
		return DispatchFailure{EventID: event.IdempotencyKey, Reason: writeErr.Error()}
	}

	return DispatchAck{
		EventID:        event.IdempotencyKey,
		IdempotencyKey: event.IdempotencyKey,
		Durable:        true,
	}
}

// validateQuality 检查 eventTime/receivedAt/quality 合法性（FR-MD-005）。
func (r *Receiver) validateQuality(event AcceptedMarketEvent) reason {
	if event.EventTime.IsZero() {
		return "event_time_zero"
	}
	if event.ReceivedAt.IsZero() {
		return "received_at_zero"
	}
	// future gate：eventTime 不得晚于 receivedAt + 容忍
	if event.EventTime.After(event.ReceivedAt.Add(5 * time.Second)) {
		return "event_time_future"
	}
	// stale gate：eventTime 不得早于 receivedAt - 24h（超过一天视为 stale）
	if event.ReceivedAt.Sub(event.EventTime) > 24*time.Hour {
		return "event_time_stale"
	}
	if !event.Quality.IsReliable {
		return "quality_unreliable"
	}
	return ""
}

// validateContract 检查必填字段（§4.4 contract_violation）。
func (r *Receiver) validateContract(event AcceptedMarketEvent) reason {
	if event.Venue == "" {
		return "venue_empty"
	}
	if event.Channel == "" {
		return "channel_empty"
	}
	if event.IdempotencyKey == "" {
		return "idempotency_key_empty"
	}
	if event.OrderingKey == "" {
		return "ordering_key_empty"
	}
	if event.Payload == nil {
		return "payload_nil"
	}
	return ""
}

// reason 是内部用的 string 别名，用于 validate* 的错误返回。
type reason string

func (r reason) Error() string { return string(r) }

// reject 构造 DispatchReject。
func reject(event AcceptedMarketEvent, reason RejectReason) DispatchReject {
	return DispatchReject{
		EventID:        event.IdempotencyKey,
		IdempotencyKey: event.IdempotencyKey,
		Reason:         reason,
	}
}

// fingerprint 计算事件的 payload 指纹（SHA-256）。
func fingerprint(event AcceptedMarketEvent) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%d|%v",
		event.Venue, event.ProductLine, event.InstrumentKey,
		event.SourceSequence, event.Payload)
	return hex.EncodeToString(h.Sum(nil))
}

// recordMetric 发送 metrics（FR-MD-008）。
func (r *Receiver) recordMetric(event AcceptedMarketEvent, outcome DispatchOutcome, latency time.Duration) {
	if r.metricsFn == nil {
		return
	}
	m := DispatchMetrics{
		Venue:       event.Venue,
		ProductLine: event.ProductLine.String(),
		Channel:     event.Channel,
		Outcome:     outcomeLabel(outcome),
		Count:       1,
		LatencyUs:   latency.Microseconds(),
	}
	if rej, ok := outcome.(DispatchReject); ok {
		m.Reason = string(rej.Reason)
	}
	r.metricsFn(m)
}

func outcomeLabel(o DispatchOutcome) string {
	switch o.(type) {
	case DispatchAck:
		return "ack"
	case DispatchReject:
		return "reject"
	case DispatchFailure:
		return "failure"
	default:
		return "unknown"
	}
}
