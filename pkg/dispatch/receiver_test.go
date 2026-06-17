package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ZoneCNH/domain-market/pkg/domainmarket"
)

// ============ Mock 实现 ============

// recordingSink 记录所有 Write 调用（测试用）。
type recordingSink struct {
	mu     sync.Mutex
	events []AcceptedMarketEvent
	fail   bool
}

func (s *recordingSink) Write(ctx context.Context, event AcceptedMarketEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("sink unavailable")
	}
	s.events = append(s.events, event)
	return nil
}

// memoryIdempotency 内存幂等存储。
type memoryIdempotency struct {
	mu   sync.Mutex
	seen map[string]string // key → fingerprint
}

func newMemoryIdempotency() *memoryIdempotency {
	return &memoryIdempotency{seen: make(map[string]string)}
}

func (m *memoryIdempotency) CheckAndSet(ctx context.Context, key, fp string) (bool, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.seen[key]; ok {
		return false, existing == fp, nil // isNew=false, isSame=(fingerprint 匹配)
	}
	m.seen[key] = fp
	return true, false, nil // isNew=true
}

// memoryOrdering 内存排序追踪。
type memoryOrdering struct {
	mu  sync.Mutex
	max map[string]int64
}

func newMemoryOrdering() *memoryOrdering {
	return &memoryOrdering{max: make(map[string]int64)}
}

func (o *memoryOrdering) Check(ctx context.Context, key string, seq int64) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if seq <= o.max[key] {
		return false, nil // 倒退或重复
	}
	o.max[key] = seq
	return true, nil
}

// ============ 测试辅助 ============

func validEvent() AcceptedMarketEvent {
	now := time.Now().Truncate(time.Millisecond)
	return AcceptedMarketEvent{
		Venue:          "binance",
		ProductLine:    domainmarket.ProductLineSpot,
		InstrumentKey:  domainmarket.InstrumentKey{Symbol: "BTCUSDT"},
		Channel:        "trade",
		EventTime:      now.Add(-100 * time.Millisecond),
		ReceivedAt:     now,
		SourceSequence: 1,
		Payload:        map[string]interface{}{"price": "50000"},
		Quality:        DataQuality{IsReliable: true},
		IdempotencyKey: "binance|spot|BTCUSDT|trade|1|1",
		OrderingKey:    "binance|spot|BTCUSDT|trade",
		Source:         "binance",
	}
}

func newTestReceiver() (*Receiver, *recordingSink) {
	sink := &recordingSink{}
	r := NewReceiver(sink, newMemoryIdempotency(), newMemoryOrdering())
	return r, sink
}

// ============ TC-MD-003: 幂等 ============

func TestIdempotencySamePayload(t *testing.T) {
	r, _ := newTestReceiver()
	ctx := context.Background()
	event := validEvent()

	out1, _ := r.Dispatch(ctx, event)
	if !out1.IsAccepted() {
		t.Fatalf("first dispatch should be accepted, got: %v", out1)
	}

	// 相同 idempotencyKey + 相同 payload → 幂等 ack（process 不会再次写）
	out2, _ := r.Dispatch(ctx, event)
	if !out2.IsAccepted() {
		t.Fatalf("idempotent re-dispatch should be accepted, got: %v", out2)
	}
}

func TestIdempotencyDifferentPayload(t *testing.T) {
	r, _ := newTestReceiver()
	ctx := context.Background()
	event := validEvent()

	out1, _ := r.Dispatch(ctx, event)
	if !out1.IsAccepted() {
		t.Fatalf("first dispatch should be accepted")
	}

	// 相同 key 但不同 payload → reject
	event2 := event
	event2.Payload = map[string]interface{}{"price": "99999"} // 不同 payload → 不同 fingerprint
	out2, _ := r.Dispatch(ctx, event2)
	if out2.IsAccepted() {
		t.Fatal("different payload with same key should be rejected")
	}
	rej, ok := out2.(DispatchReject)
	if !ok || rej.Reason != RejectIdempotencyConflict {
		t.Fatalf("expected idempotency_conflict, got: %v", out2)
	}
}

// ============ TC-MD-004: 排序 ============

func TestOrderingSequenceIncrease(t *testing.T) {
	r, _ := newTestReceiver()
	ctx := context.Background()

	e1 := validEvent()
	e1.SourceSequence = 1
	e2 := validEvent()
	e2.SourceSequence = 2
	e2.IdempotencyKey = "binance|spot|BTCUSDT|trade|2"

	o1, _ := r.Dispatch(ctx, e1)
	o2, _ := r.Dispatch(ctx, e2)
	if !o1.IsAccepted() || !o2.IsAccepted() {
		t.Fatalf("increasing sequence should be accepted: %v, %v", o1, o2)
	}
}

func TestOrderingSequenceRegression(t *testing.T) {
	r, _ := newTestReceiver()
	ctx := context.Background()

	e1 := validEvent()
	e1.SourceSequence = 5
	r.Dispatch(ctx, e1)

	// 倒退 → ordering_violation
	e2 := validEvent()
	e2.SourceSequence = 3 // 倒退
	e2.IdempotencyKey = "binance|spot|BTCUSDT|trade|3"
	out, _ := r.Dispatch(ctx, e2)
	if out.IsAccepted() {
		t.Fatal("sequence regression should be rejected")
	}
	rej, ok := out.(DispatchReject)
	if !ok || rej.Reason != RejectOrderingViolation {
		t.Fatalf("expected ordering_violation, got: %v", out)
	}
}

// ============ TC-MD-005: quality gate (fail-closed) ============

func TestQualityGateEventTimeZero(t *testing.T) {
	r, _ := newTestReceiver()
	event := validEvent()
	event.EventTime = time.Time{} // zero
	out, _ := r.Dispatch(context.Background(), event)
	rej, ok := out.(DispatchReject)
	if !ok || rej.Reason != RejectQualityRejected {
		t.Fatalf("zero eventTime should be quality_rejected, got: %v", out)
	}
}

func TestQualityGateFuture(t *testing.T) {
	r, _ := newTestReceiver()
	event := validEvent()
	event.EventTime = time.Now().Add(time.Hour) // future
	out, _ := r.Dispatch(context.Background(), event)
	rej, ok := out.(DispatchReject)
	if !ok || rej.Reason != RejectQualityRejected {
		t.Fatalf("future eventTime should be quality_rejected, got: %v", out)
	}
}

func TestQualityGateStale(t *testing.T) {
	r, _ := newTestReceiver()
	event := validEvent()
	event.EventTime = time.Now().Add(-48 * time.Hour) // stale
	out, _ := r.Dispatch(context.Background(), event)
	rej, ok := out.(DispatchReject)
	if !ok || rej.Reason != RejectQualityRejected {
		t.Fatalf("stale eventTime should be quality_rejected, got: %v", out)
	}
}

func TestQualityGateUnreliable(t *testing.T) {
	r, _ := newTestReceiver()
	event := validEvent()
	event.Quality.IsReliable = false
	out, _ := r.Dispatch(context.Background(), event)
	rej, ok := out.(DispatchReject)
	if !ok || rej.Reason != RejectQualityRejected {
		t.Fatalf("unreliable quality should be rejected, got: %v", out)
	}
}

// ============ TC-MD-006: retry classification ============

func TestRetryClassificationRejectNotRetryable(t *testing.T) {
	r, _ := newTestReceiver()
	event := validEvent()
	event.Venue = "" // contract_violation → reject（不可重试）
	out, _ := r.Dispatch(context.Background(), event)
	if out.IsAccepted() || out.IsRetryable() {
		t.Fatalf("reject should not be retryable, got: %v", out)
	}
}

func TestRetryClassificationFailureRetryable(t *testing.T) {
	sink := &recordingSink{fail: true}
	r := NewReceiver(sink, newMemoryIdempotency(), newMemoryOrdering())
	event := validEvent()
	out, _ := r.Dispatch(context.Background(), event)
	if out.IsAccepted() {
		t.Fatal("sink failure should not be accepted")
	}
	if !out.IsRetryable() {
		t.Fatal("sink failure should be retryable")
	}
}

// ============ TC-MD-007: batch semantics ============

func TestBatchPartialFailure(t *testing.T) {
	r, _ := newTestReceiver()
	ctx := context.Background()

	good := validEvent()
	bad := validEvent()
	bad.Venue = "" // contract_violation

	outcomes, _ := r.DispatchBatch(ctx, []AcceptedMarketEvent{good, bad})
	if len(outcomes) != 2 {
		t.Fatalf("expected 2 outcomes, got %d", len(outcomes))
	}
	if !outcomes[0].IsAccepted() {
		t.Error("first event (good) should be accepted")
	}
	if outcomes[1].IsAccepted() {
		t.Error("second event (bad) should be rejected")
	}
}

func TestBatchAllValid(t *testing.T) {
	r, _ := newTestReceiver()
	ctx := context.Background()

	events := make([]AcceptedMarketEvent, 5)
	for i := range events {
		events[i] = validEvent()
		events[i].SourceSequence = int64(i + 1)
		events[i].IdempotencyKey = fmt.Sprintf("key-%d", i+1)
	}

	outcomes, _ := r.DispatchBatch(ctx, events)
	for i, o := range outcomes {
		if !o.IsAccepted() {
			t.Errorf("event %d should be accepted, got: %v", i, o)
		}
	}
}

// ============ TC-MD-008: observability ============

func TestObservabilityMetricsEmitted(t *testing.T) {
	var emitted []DispatchMetrics
	r := NewReceiver(
		&recordingSink{},
		newMemoryIdempotency(),
		newMemoryOrdering(),
		WithMetrics(func(m DispatchMetrics) { emitted = append(emitted, m) }),
	)

	event := validEvent()
	r.Dispatch(context.Background(), event)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(emitted))
	}
	m := emitted[0]
	if m.Venue != "binance" || m.Outcome != "ack" || m.Count != 1 {
		t.Errorf("unexpected metric: %+v", m)
	}
}

func TestObservabilityMetricsOnReject(t *testing.T) {
	var emitted []DispatchMetrics
	r := NewReceiver(
		&recordingSink{},
		newMemoryIdempotency(),
		newMemoryOrdering(),
		WithMetrics(func(m DispatchMetrics) { emitted = append(emitted, m) }),
	)

	event := validEvent()
	event.Venue = "" // contract_violation
	r.Dispatch(context.Background(), event)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(emitted))
	}
	m := emitted[0]
	if m.Outcome != "reject" || m.Reason != "contract_violation" {
		t.Errorf("expected reject/contract_violation, got: %+v", m)
	}
}

// ============ contract_violation 字段校验 ============

func TestContractViolationEmptyVenue(t *testing.T) {
	r, _ := newTestReceiver()
	event := validEvent()
	event.Venue = ""
	out, _ := r.Dispatch(context.Background(), event)
	rej, ok := out.(DispatchReject)
	if !ok || rej.Reason != RejectContractViolation {
		t.Fatalf("empty venue should be contract_violation, got: %v", out)
	}
}

func TestContractViolationNilPayload(t *testing.T) {
	r, _ := newTestReceiver()
	event := validEvent()
	event.Payload = nil
	out, _ := r.Dispatch(context.Background(), event)
	rej, ok := out.(DispatchReject)
	if !ok || rej.Reason != RejectContractViolation {
		t.Fatalf("nil payload should be contract_violation, got: %v", out)
	}
}

// ============ unsupported_channel ============

func TestUnsupportedChannel(t *testing.T) {
	r, _ := newTestReceiver()
	event := validEvent()
	event.Channel = "exotic_unsupported"
	out, _ := r.Dispatch(context.Background(), event)
	rej, ok := out.(DispatchReject)
	if !ok || rej.Reason != RejectUnsupportedChannel {
		t.Fatalf("unsupported channel should be rejected, got: %v", out)
	}
}

// ============ happy path ============

func TestHappyPath(t *testing.T) {
	sink := &recordingSink{}
	r := NewReceiver(sink, newMemoryIdempotency(), newMemoryOrdering())
	event := validEvent()
	out, _ := r.Dispatch(context.Background(), event)

	if !out.IsAccepted() {
		t.Fatalf("valid event should be accepted, got: %v", out)
	}
	ack, ok := out.(DispatchAck)
	if !ok {
		t.Fatalf("expected DispatchAck, got %T", out)
	}
	if !ack.Durable {
		t.Error("ack should be durable")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.events) != 1 {
		t.Errorf("sink should have 1 event, got %d", len(sink.events))
	}
}
