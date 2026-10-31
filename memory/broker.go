// Package memory is an in-process bus that implements the gbus contract.
//
// It exists to run the contract tests and to let a single-process deployment use the
// same interfaces. It is NOT a broker: nothing survives the process, and "durable"
// guarantees are simulated only as far as the contract test needs.
package memory

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/maxid/gbus"
)

// Broker holds topics in memory. One broker may serve several producers and
// consumers; a real deployment replaces it with the Kafka driver.
type Broker struct {
	mu       sync.Mutex
	topics   map[string]*topic
	closed   bool
	metrics  gbus.Metrics
	now      func() time.Time
	MaxBytes int64
}

type topic struct {
	partitions map[int32][]gbus.Record
	offsets    map[int32]int64 // next offset to assign
	// committed is the per-group commit position: the next offset to deliver.
	committed map[string]map[int32]int64
}

// Option configures a Broker.
type Option func(*Broker)

// WithMetrics installs a metrics sink.
func WithMetrics(metrics gbus.Metrics) Option {
	return func(b *Broker) { b.metrics = metrics }
}

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option {
	return func(b *Broker) {
		if now != nil {
			b.now = now
		}
	}
}

// New creates an empty broker.
func New(opts ...Option) *Broker {
	b := &Broker{
		topics:   map[string]*topic{},
		now:      time.Now,
		MaxBytes: gbus.DefaultMaxMessageBytes,
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

func (b *Broker) topicLocked(name string) *topic {
	t, ok := b.topics[name]
	if !ok {
		t = &topic{
			partitions: map[int32][]gbus.Record{},
			offsets:    map[int32]int64{},
			committed:  map[string]map[int32]int64{},
		}
		b.topics[name] = t
	}
	return t
}

// partitionFor keeps the same key on the same partition, which is what gives
// per-key ordering. An empty key round-robins, so ordering is not promised.
func partitionFor(key []byte, partitions int32) int32 {
	if partitions <= 1 || len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int32(h.Sum32() % uint32(partitions))
}

// Publish implements gbus.Producer.
func (b *Broker) Publish(ctx context.Context, message gbus.Message) (gbus.PublishMetadata, error) {
	if err := ctx.Err(); err != nil {
		return gbus.PublishMetadata{}, gbus.Temporary(err)
	}
	if err := message.Validate(b.MaxBytes); err != nil {
		return gbus.PublishMetadata{}, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return gbus.PublishMetadata{}, gbus.ErrClosed
	}

	t := b.topicLocked(message.Topic)
	partition := partitionFor(message.Key, 1) // single partition keeps ordering total
	stored := message.Clone()
	if stored.Timestamp.IsZero() {
		stored.Timestamp = b.now()
	}
	offset := t.offsets[partition]
	t.offsets[partition] = offset + 1

	record := gbus.Record{Message: stored, Partition: partition, Offset: offset}
	t.partitions[partition] = append(t.partitions[partition], record)

	if b.metrics != nil {
		b.metrics.Published(message.Topic, len(message.Value), nil)
	}
	return gbus.PublishMetadata{
		Topic:     message.Topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: stored.Timestamp,
	}, nil
}

// Close implements gbus.Producer: nothing is buffered, so nothing can be lost.
func (b *Broker) Close(context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

// Consumer is a subscription over the broker.
type Consumer struct {
	broker *Broker
	cfg    gbus.ConsumerConfig
	dlq    *Broker

	mu     sync.Mutex
	closed bool
}

// NewConsumer creates a consumer. A dead-letter configuration gets its own broker
// topic space, so tests can inspect what was rejected.
func (b *Broker) NewConsumer(cfg gbus.ConsumerConfig) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	c := &Consumer{broker: b, cfg: cfg}
	if cfg.DeadLetter != nil {
		c.dlq = b
	}
	return c, nil
}

// Run implements gbus.Consumer. It delivers records in offset order and advances
// the commit position only after Ack (or after a successful dead letter write).
func (c *Consumer) Run(ctx context.Context, handler gbus.Handler) error {
	if handler == nil {
		return gbus.Permanent(errors.New("handler is required"))
	}

	delivered := map[int32]int64{} // next offset to hand out
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		records, done := c.nextBatch(delivered)
		if len(records) == 0 {
			if done {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Millisecond):
			}
			continue
		}

		for _, record := range records {
			result := handler.Handle(ctx, record)
			if c.broker.metrics != nil {
				c.broker.metrics.Consumed(record.Topic, result.Outcome, result.Reason)
			}

			switch result.Outcome {
			case gbus.Ack:
				c.commit(record)
				delivered[record.Partition] = record.Offset + 1

			case gbus.Retry:
				if c.broker.metrics != nil {
					c.broker.metrics.Retried(record.Topic, record.Attempts+1)
				}
				delay := result.RetryAfter
				if delay <= 0 {
					delay = c.cfg.Retry.Next(record.Attempts + 1)
				}
				// The record is redelivered: reset the cursor for this partition.
				delivered[record.Partition] = record.Offset
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(delay):
				}
				return c.Run(ctx, handler)

			case gbus.Reject:
				if c.cfg.DeadLetter != nil {
					if err := c.deadLetter(ctx, record, result.Reason); err != nil {
						if c.broker.metrics != nil {
							c.broker.metrics.DeadLettered(record.Topic, result.Reason, err)
						}
						// Not delivered anywhere: stop rather than drop it.
						return err
					}
				} else {
					// No dead-letter queue: the operator must intervene, so the
					// position does not advance.
					return gbus.Permanent(errors.New("record rejected and no dead letter queue is configured: " + result.Reason))
				}
				c.commit(record)
				delivered[record.Partition] = record.Offset + 1

			default:
				return gbus.Permanent(errors.New("unknown outcome " + string(result.Outcome)))
			}
		}
	}
}

// nextBatch returns the next undelivered records and whether the consumer is done
// (every topic closed and drained).
func (c *Consumer) nextBatch(delivered map[int32]int64) ([]gbus.Record, bool) {
	c.broker.mu.Lock()
	defer c.broker.mu.Unlock()

	var out []gbus.Record
	done := true

	topics := make([]string, 0, len(c.cfg.Topics))
	topics = append(topics, c.cfg.Topics...)
	sort.Strings(topics)

	for _, name := range topics {
		t, ok := c.broker.topics[name]
		if !ok {
			continue
		}
		committed := t.committed[c.cfg.Group]
		if committed == nil {
			committed = map[int32]int64{}
			t.committed[c.cfg.Group] = committed
		}
		for partition, records := range t.partitions {
			cursor, ok := delivered[partition]
			if !ok {
				cursor = committed[partition]
			}
			if int(cursor) < len(records) {
				done = false
				record := records[cursor]
				record.Attempts = 0
				out = append(out, record)
			}
		}
	}
	return out, done && !c.broker.hasPending()
}

// hasPending reports whether any record has not been committed for this group.
func (b *Broker) hasPending() bool {
	for _, t := range b.topics {
		for group, committed := range t.committed {
			for partition, records := range t.partitions {
				if int(committed[partition]) < len(records) {
					_ = group
					return true
				}
			}
		}
	}
	return false
}

// commit advances the group's position for the record's partition.
func (c *Consumer) commit(record gbus.Record) {
	c.broker.mu.Lock()
	defer c.broker.mu.Unlock()

	t, ok := c.broker.topics[record.Topic]
	if !ok {
		return
	}
	committed := t.committed[c.cfg.Group]
	if committed == nil {
		committed = map[int32]int64{}
		t.committed[c.cfg.Group] = committed
	}
	committed[record.Partition] = record.Offset + 1
}

// deadLetter writes the rejected record to the dead-letter topic before the
// original position may advance.
func (c *Consumer) deadLetter(ctx context.Context, record gbus.Record, reason string) error {
	headers := make([]gbus.Header, 0, len(record.Headers)+4)
	headers = append(headers, record.Headers...)
	headers = append(headers,
		gbus.Header{Key: "dlq-origin-topic", Value: []byte(record.Topic)},
		gbus.Header{Key: "dlq-origin-partition", Value: []byte(itoa(int(record.Partition)))},
		gbus.Header{Key: "dlq-origin-offset", Value: []byte(itoa(int(record.Offset)))},
		gbus.Header{Key: "dlq-reason", Value: []byte(reason)},
	)

	// A rejected record must still reach the dead-letter topic while the consumer is
	// shutting down: losing it because the handler's context was cancelled would make
	// the rejection silent.
	publishCtx := context.WithoutCancel(ctx)
	_, err := c.dlq.Publish(publishCtx, gbus.Message{
		Topic:   c.cfg.DeadLetter.Topic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: headers,
	})
	return err
}

// Close implements gbus.Consumer.
func (c *Consumer) Close(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// CommitPosition reports the next offset the group will deliver for a topic and
// partition; tests use it to assert at-least-once behaviour.
func (b *Broker) CommitPosition(topicName, group string, partition int32) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.topics[topicName]
	if !ok {
		return 0
	}
	return t.committed[group][partition]
}

// Records returns every record published to a topic, for assertions.
func (b *Broker) Records(topicName string) []gbus.Record {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.topics[topicName]
	if !ok {
		return nil
	}
	var out []gbus.Record
	for _, records := range t.partitions {
		out = append(out, records...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Offset < out[j].Offset })
	return out
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	negative := v < 0
	if negative {
		v = -v
	}
	var buf [20]byte
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
