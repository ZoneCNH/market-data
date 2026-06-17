// Package sink implements the production SinkPort for market-data:
// dual-write to TDengine (time-series) + Kafka (event stream backbone).
//
// 对齐数据域基础架构报告 §14.4 双写一致性策略：
//   - TD + Kafka 双写成功才 ACK
//   - TD 成功 Kafka 失败 → 重试 Kafka；超限 → DispatchFailure（可重试）
//   - TD 失败 → 整体失败，不 ACK（幂等键保证不重复）
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ZoneCNH/kafkax/pkg/kafkax"
	"github.com/ZoneCNH/market-data/pkg/dispatch"
	"github.com/ZoneCNH/taosx/pkg/taosx"
)

// DualWriteSink implements dispatch.SinkPort by writing to both TDengine and Kafka.
type DualWriteSink struct {
	td          taosx.Client
	kafkaProducer kafkax.Producer
	tdDatabase  string // per-provider TD database, e.g. "market_binance"
	kafkaTopic  string // Kafka topic, e.g. "mkt.binance.trade"
}

// DualWriteOption configures DualWriteSink.
type DualWriteOption func(*DualWriteSink)

// WithTDDatabase sets the TDengine database name.
func WithTDDatabase(db string) DualWriteOption {
	return func(s *DualWriteSink) { s.tdDatabase = db }
}

// WithKafkaTopic sets the Kafka topic.
func WithKafkaTopic(topic string) DualWriteOption {
	return func(s *DualWriteSink) { s.kafkaTopic = topic }
}

// NewDualWriteSink creates a dual-write sink.
// td: TDengine client (from bootstrap stores.TD)
// kafka: Kafka client (from bootstrap stores.Kafka); Producer() is called internally.
func NewDualWriteSink(td taosx.Client, kafka *kafkax.Client, opts ...DualWriteOption) (*DualWriteSink, error) {
	producer, err := kafka.Producer()
	if err != nil {
		return nil, fmt.Errorf("sink: kafka producer: %w", err)
	}
	s := &DualWriteSink{
		td:            td,
		kafkaProducer: producer,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Write implements dispatch.SinkPort — dual-write to TDengine + Kafka.
// Returns error only if the write is not durable (should be retried).
func (s *DualWriteSink) Write(ctx context.Context, event dispatch.AcceptedMarketEvent) error {
	// 1. Write to TDengine (time-series)
	if err := s.writeTD(ctx, event); err != nil {
		return fmt.Errorf("td write: %w", err)
	}

	// 2. Write to Kafka (event stream)
	if err := s.writeKafka(ctx, event); err != nil {
		// TD succeeded but Kafka failed — return error for retry.
		// The idempotency key ensures TD re-write is a no-op on retry.
		return fmt.Errorf("kafka write: %w", err)
	}

	return nil
}

// writeTD writes the event as a TDengine point.
func (s *DualWriteSink) writeTD(ctx context.Context, event dispatch.AcceptedMarketEvent) error {
	db := s.tdDatabase
	if db == "" {
		db = "market_" + event.Venue
	}

	point := taosx.Point{
		Table:     s.tableName(event),
		Timestamp: event.EventTime,
		Tags: map[string]any{
			"venue":       event.Venue,
			"productline": event.ProductLine.String(),
			"channel":     event.Channel,
		},
		Fields: s.eventFields(event),
	}

	_, err := s.td.WriteBatch(ctx, taosx.Batch{
		Database: db,
		Points:   []taosx.Point{point},
	})
	return err
}

// writeKafka produces the event to Kafka as JSON.
func (s *DualWriteSink) writeKafka(ctx context.Context, event dispatch.AcceptedMarketEvent) error {
	topic := s.kafkaTopic
	if topic == "" {
		topic = fmt.Sprintf("mkt.%s.%s", event.Venue, event.Channel)
	}

	payload, err := json.Marshal(kafkaEvent{
		Venue:          event.Venue,
		ProductLine:    event.ProductLine.String(),
		InstrumentKey:  event.InstrumentKey.Symbol,
		Channel:        event.Channel,
		EventTime:      event.EventTime,
		ReceivedAt:     event.ReceivedAt,
		SourceSequence: event.SourceSequence,
		Payload:        event.Payload,
		IdempotencyKey: event.IdempotencyKey,
		Source:         event.Source,
	})
	if err != nil {
		return fmt.Errorf("kafka marshal: %w", err)
	}

	_, err = s.kafkaProducer.Send(ctx, kafkax.Message{
		Topic: topic,
		Key:   []byte(event.IdempotencyKey),
		Value: payload,
	})
	return err
}

// tableName derives the TDengine subtable name from the event.
func (s *DualWriteSink) tableName(event dispatch.AcceptedMarketEvent) string {
	return fmt.Sprintf("%s_%s", event.Channel, event.InstrumentKey.Symbol)
}

// eventFields extracts fields from the event payload for TDengine.
func (s *DualWriteSink) eventFields(event dispatch.AcceptedMarketEvent) map[string]any {
	fields := map[string]any{
		"source_sequence": event.SourceSequence,
		"quality_reliable": event.Quality.IsReliable,
	}
	// Merge payload fields if it's a map
	if m, ok := event.Payload.(map[string]any); ok {
		for k, v := range m {
			fields[k] = v
		}
	}
	return fields
}

// kafkaEvent is the JSON envelope for Kafka messages.
type kafkaEvent struct {
	Venue          string    `json:"venue"`
	ProductLine    string    `json:"product_line"`
	InstrumentKey  string    `json:"instrument"`
	Channel        string    `json:"channel"`
	EventTime      time.Time `json:"event_time"`
	ReceivedAt     time.Time `json:"received_at"`
	SourceSequence int64     `json:"source_sequence"`
	Payload        any       `json:"payload"`
	IdempotencyKey string    `json:"idempotency_key"`
	Source         string    `json:"source"`
}

// Compile-time assertion that DualWriteSink implements dispatch.SinkPort.
var _ dispatch.SinkPort = (*DualWriteSink)(nil)
