package sink

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ZoneCNH/domain-market/pkg/domainmarket"
	"github.com/ZoneCNH/kafkax/pkg/kafkax"
	"github.com/ZoneCNH/market-data/pkg/dispatch"
	"github.com/ZoneCNH/taosx/pkg/taosx"
)

// ============ Mock Producer ============

type mockProducer struct {
	mu       sync.Mutex
	messages []kafkax.Message
	fail     bool
}

func (m *mockProducer) Send(ctx context.Context, msg kafkax.Message, opts ...kafkax.ProduceOption) (kafkax.ProduceResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return kafkax.ProduceResult{}, errors.New("kafka unavailable")
	}
	m.messages = append(m.messages, msg)
	return kafkax.ProduceResult{Topic: msg.Topic}, nil
}

func (m *mockProducer) SendBatch(ctx context.Context, msgs []kafkax.Message, opts ...kafkax.ProduceOption) (kafkax.BatchProduceResult, error) {
	return kafkax.BatchProduceResult{}, nil
}

func (m *mockProducer) Flush(ctx context.Context) error { return nil }
func (m *mockProducer) Close(ctx context.Context) error { return nil }

// ============ 测试辅助 ============

func testEvent() dispatch.AcceptedMarketEvent {
	now := time.Now().Truncate(time.Millisecond)
	return dispatch.AcceptedMarketEvent{
		Venue:          "binance",
		ProductLine:    domainmarket.ProductLineSpot,
		InstrumentKey:  domainmarket.InstrumentKey{Symbol: "BTCUSDT"},
		Channel:        "trade",
		EventTime:      now.Add(-100 * time.Millisecond),
		ReceivedAt:     now,
		SourceSequence: 1,
		Payload:        map[string]any{"price": "50000", "qty": "0.5"},
		Quality:        dispatch.DataQuality{IsReliable: true},
		IdempotencyKey: "binance|spot|BTCUSDT|trade|1|1",
		OrderingKey:    "binance|spot|BTCUSDT|trade",
		Source:         "binance",
	}
}

// newTestSink 构造测试用 DualWriteSink（绕过 NewDualWriteSink 的 kafka.Producer() 调用）。
func newTestSink(td taosx.Client, producer *mockProducer) *DualWriteSink {
	return &DualWriteSink{
		td:            td,
		kafkaProducer: producer,
	}
}

// ============ 测试 ============

func TestDualWriteSuccess(t *testing.T) {
	td := taosx.NewFakeClient()
	producer := &mockProducer{}
	sink := newTestSink(td, producer)

	err := sink.Write(context.Background(), testEvent())
	if err != nil {
		t.Fatalf("dual write should succeed: %v", err)
	}

	// TDengine 应有写入
	if td.WriteCalls() != 1 {
		t.Errorf("TD should have 1 write call, got %d", td.WriteCalls())
	}

	// Kafka 应有 1 条消息
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.messages) != 1 {
		t.Fatalf("Kafka should have 1 message, got %d", len(producer.messages))
	}

	// Kafka 消息 topic 正确
	msg := producer.messages[0]
	if msg.Topic != "mkt.binance.trade" {
		t.Errorf("topic = %q, want mkt.binance.trade", msg.Topic)
	}

	// Kafka 消息 key = idempotencyKey
	if string(msg.Key) != "binance|spot|BTCUSDT|trade|1|1" {
		t.Errorf("key = %q, want idempotency key", string(msg.Key))
	}

	// Kafka 消息 payload 是有效 JSON
	var parsed kafkaEvent
	if err := json.Unmarshal(msg.Value, &parsed); err != nil {
		t.Fatalf("kafka message should be valid JSON: %v", err)
	}
	if parsed.Venue != "binance" || parsed.Channel != "trade" {
		t.Errorf("parsed venue/channel = %q/%q", parsed.Venue, parsed.Channel)
	}
}

func TestDualWriteTDFailure(t *testing.T) {
	td := taosx.NewFakeClient()
	td.WriteError = errors.New("td unavailable")
	producer := &mockProducer{}
	sink := newTestSink(td, producer)

	err := sink.Write(context.Background(), testEvent())
	if err == nil {
		t.Fatal("TD failure should return error")
	}

	// Kafka 不应有写入（TD 先失败，Kafka 不执行）
	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.messages) != 0 {
		t.Errorf("Kafka should have 0 messages on TD failure, got %d", len(producer.messages))
	}
}

func TestDualWriteKafkaFailure(t *testing.T) {
	td := taosx.NewFakeClient()
	producer := &mockProducer{fail: true}
	sink := newTestSink(td, producer)

	err := sink.Write(context.Background(), testEvent())
	if err == nil {
		t.Fatal("Kafka failure should return error")
	}

	// TD 应已写入（TD 先执行成功）
	if td.WriteCalls() != 1 {
		t.Errorf("TD should have 1 write call even on Kafka failure, got %d", td.WriteCalls())
	}
}

func TestDualWriteTDDatabaseOverride(t *testing.T) {
	td := taosx.NewFakeClient()
	producer := &mockProducer{}
	sink := &DualWriteSink{
		td:            td,
		kafkaProducer: producer,
		tdDatabase:    "market_custom",
	}

	sink.Write(context.Background(), testEvent())

	if td.WriteCalls() != 1 {
		t.Errorf("expected 1 TD write, got %d", td.WriteCalls())
	}
}

func TestDualWriteKafkaTopicOverride(t *testing.T) {
	td := taosx.NewFakeClient()
	producer := &mockProducer{}
	sink := &DualWriteSink{
		td:            td,
		kafkaProducer: producer,
		kafkaTopic:    "custom.topic",
	}

	sink.Write(context.Background(), testEvent())

	producer.mu.Lock()
	defer producer.mu.Unlock()
	if len(producer.messages) > 0 && producer.messages[0].Topic != "custom.topic" {
		t.Errorf("topic = %q, want custom.topic", producer.messages[0].Topic)
	}
}

func TestDualWriteImplementsSinkPort(t *testing.T) {
	// 编译期断言（var _ 在 dualwrite.go 已声明，这里额外运行期验证）
	var s dispatch.SinkPort = &DualWriteSink{}
	_ = s
}
