// Package dispatch implements the exchange-neutral downstream dispatch receiving side.
// Adapters (e.g. binance/server) call DownstreamDispatchPort to submit
// AcceptedMarketEvents after normalization, validation, and dedup.
package dispatch

import (
	"context"
	"fmt"
	"time"

)

// DownstreamDispatchPort is the receiving-side entry point for normalized market events.
type DownstreamDispatchPort interface {
	Dispatch(ctx context.Context, event AcceptedMarketEvent) (DispatchOutcome, error)
	DispatchBatch(ctx context.Context, events []AcceptedMarketEvent) ([]DispatchOutcome, error)
}

// AcceptedMarketEvent is an adapter-normalized event ready for downstream dispatch.
type AcceptedMarketEvent struct {
	Venue          string
	ProductLine    domainmarket.ProductLine
	InstrumentKey  domainmarket.InstrumentKey
	Channel        string
	EventTime      time.Time
	ReceivedAt     time.Time
	SourceSequence int64
	Payload        interface{}
	Quality        DataQuality
	IdempotencyKey string
	OrderingKey    string
	Source         string
}

// DataQuality carries source quality metadata.
type DataQuality struct {
	Latency       time.Duration
	IsReliable    bool
	IsRecovered   bool
	DegradeReason string
}

// DispatchOutcome represents the result of a dispatch call.
type DispatchOutcome interface {
	IsAccepted() bool
	IsRetryable() bool
}

// DispatchAck confirms the event was accepted.
type DispatchAck struct {
	EventID        string
	IdempotencyKey string
	Durable        bool
}

func (a DispatchAck) IsAccepted() bool  { return true }
func (a DispatchAck) IsRetryable() bool { return false }

// DispatchReject means the event was rejected and should not be retried.
type DispatchReject struct {
	EventID        string
	IdempotencyKey string
	Reason         RejectReason
}

func (r DispatchReject) IsAccepted() bool  { return false }
func (r DispatchReject) IsRetryable() bool { return false }

// DispatchFailure means the receiver is temporarily unavailable.
type DispatchFailure struct {
	EventID string
	Reason  string
}

func (f DispatchFailure) IsAccepted() bool  { return false }
func (f DispatchFailure) IsRetryable() bool { return true }
func (f DispatchFailure) Error() string     { return fmt.Sprintf("dispatch failure: %s", f.Reason) }

// RejectReason classifies rejection causes (8 canonical reasons per SPEC §4.4).
type RejectReason string

const (
	RejectContractViolation   RejectReason = "contract_violation"
	RejectQualityRejected     RejectReason = "quality_rejected"
	RejectIdempotencyConflict RejectReason = "idempotency_conflict"
	RejectOrderingViolation   RejectReason = "ordering_violation"
	RejectUnsupportedChannel  RejectReason = "unsupported_channel"
	RejectUnauthorized        RejectReason = "unauthorized"
	RejectRateLimited         RejectReason = "rate_limited"
	RejectServerUnavailable   RejectReason = "server_unavailable"
)

// IsTerminal returns true if retry will not help.
func (r RejectReason) IsTerminal() bool {
	return r != RejectServerUnavailable
}

// MapBinanceReject maps binance-native codes to market-data outcomes.
func MapBinanceReject(binanceCode string) (DispatchOutcome, RejectReason) {
	switch binanceCode {
	case "retryable":
		return DispatchFailure{}, ""
	case "terminal_validation":
		return DispatchReject{}, RejectContractViolation
	case "terminal_conflict":
		return DispatchReject{}, RejectIdempotencyConflict
	case "unauthorized":
		return DispatchReject{}, RejectUnauthorized
	case "rate_limited":
		return DispatchReject{}, RejectRateLimited
	case "server_unavailable":
		return DispatchFailure{}, ""
	default:
		return DispatchReject{}, RejectContractViolation
	}
}

// DispatchMetrics provides per-dimension counters.
type DispatchMetrics struct {
	Venue       string
	ProductLine string
	Channel     string
	Outcome     string
	Reason      string
	Count       int64
	LatencyUs   int64
}
