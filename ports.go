package gbus

import (
	"context"
	"errors"
	"time"
)

// Producer publishes messages. Publish returns only after the broker acknowledged
// at the configured level, so a nil error means "the broker has it".
type Producer interface {
	Publish(ctx context.Context, message Message) (PublishMetadata, error)
	// Close stops new publishes, flushes what was accepted within the caller's
	// deadline, and reports what it could not flush.
	Close(ctx context.Context) error
}

// Outcome tells the consumer what to do with a record.
type Outcome string

const (
	// Ack means the record was processed: the commit position may advance.
	Ack Outcome = "ack"
	// Retry means the record is not done: the position must not advance.
	Retry Outcome = "retry"
	// Reject means the record will never succeed. With a dead-letter queue
	// configured the consumer writes it there before advancing; otherwise the
	// position stays put and the operator is expected to intervene.
	Reject Outcome = "reject"
)

// Result is a handler's verdict.
type Result struct {
	Outcome Outcome
	// Reason is a stable, low-cardinality label for metrics and dead letters.
	Reason string
	// RetryAfter delays the next attempt; zero uses the configured backoff.
	RetryAfter time.Duration
}

// Handler processes one record. It must be safe for concurrent calls when the
// consumer is configured with more than one worker.
type Handler interface {
	Handle(ctx context.Context, record Record) Result
}

// HandlerFunc adapts a function.
type HandlerFunc func(ctx context.Context, record Record) Result

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, record Record) Result { return f(ctx, record) }

// Consumer runs a subscription. Run blocks until the context is cancelled or a
// fatal error occurs; only one Run may be active per consumer.
type Consumer interface {
	Run(ctx context.Context, handler Handler) error
	Close(ctx context.Context) error
}

// BackoffPolicy bounds retry pacing. A retry never spins: the consumer pauses the
// partition instead.
type BackoffPolicy struct {
	Min    time.Duration
	Max    time.Duration
	Factor float64
}

// DefaultBackoff is 50ms doubling to 5s.
var DefaultBackoff = BackoffPolicy{Min: 50 * time.Millisecond, Max: 5 * time.Second, Factor: 2}

// Next returns the delay for attempt (1-based), clamped to Max.
func (b BackoffPolicy) Next(attempt int) time.Duration {
	if b.Min <= 0 {
		b.Min = DefaultBackoff.Min
	}
	if b.Max <= 0 {
		b.Max = DefaultBackoff.Max
	}
	if b.Factor <= 1 {
		b.Factor = DefaultBackoff.Factor
	}
	delay := float64(b.Min)
	for i := 1; i < attempt; i++ {
		delay *= b.Factor
		if delay >= float64(b.Max) {
			return b.Max
		}
	}
	return time.Duration(delay)
}

// DeadLetterConfig records where rejected records go. Writing to the DLQ happens
// before the original position advances; a DLQ failure stops the partition instead
// of losing the record.
type DeadLetterConfig struct {
	Topic string
	// MaxAttempts bounds retries before a record is rejected. Zero means the
	// handler decides: only an explicit Reject goes to the dead letter queue.
	MaxAttempts int
}

// ConsumerConfig bounds one subscription.
type ConsumerConfig struct {
	// Group and Topics are required: a wildcard subscription that discovers topics
	// at runtime is not offered, because it makes blast radius unpredictable.
	Group  string
	Topics []string
	// HandlerConcurrency bounds in-flight handlers. Values above the partition
	// count do not add parallelism inside a partition.
	HandlerConcurrency int
	// MaxMessageBytes bounds the accepted value size.
	MaxMessageBytes int64
	// Retry paces re-delivery after Result{Outcome: Retry}.
	Retry BackoffPolicy
	// DeadLetter routes rejected records.
	DeadLetter *DeadLetterConfig
}

// Validate rejects an unusable consumer configuration.
func (c *ConsumerConfig) Validate() error {
	if c.Group == "" {
		return Permanent(errors.New("group is required"))
	}
	if len(c.Topics) == 0 {
		return Permanent(errors.New("topics are required"))
	}
	if c.HandlerConcurrency < 0 {
		return Permanent(errors.New("handler concurrency must not be negative"))
	}
	if c.HandlerConcurrency == 0 {
		c.HandlerConcurrency = 1
	}
	if c.MaxMessageBytes <= 0 {
		c.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if c.Retry.Min <= 0 {
		c.Retry = DefaultBackoff
	}
	if c.DeadLetter != nil && c.DeadLetter.Topic == "" {
		return Permanent(errors.New("dead letter topic is required when the dead letter queue is enabled"))
	}
	return nil
}

// Metrics receives neutral counters; a driver reports into it and the application
// decides how to expose them.
type Metrics interface {
	Published(topic string, bytes int, err error)
	Consumed(topic string, outcome Outcome, reason string)
	Retried(topic string, attempt int)
	DeadLettered(topic string, reason string, err error)
}
