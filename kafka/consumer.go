package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maxid/gbus"
)

// ConsumerConfig configures the Kafka consumer group.
type ConsumerConfig struct {
	Brokers  []string
	ClientID string
	Group    string
	Topics   []string
	// StartFrom is where a brand-new group begins: "earliest" or "latest"
	// (default "earliest", which is what a data pipeline wants).
	StartFrom string
	// HandlerConcurrency bounds handlers running at once. Records from one
	// partition are still handled one at a time, so ordering survives.
	HandlerConcurrency int
	// CommitInterval batches offset commits; zero commits after every record.
	CommitInterval time.Duration
	// MaxMessageBytes rejects oversized records instead of handing them to the
	// handler.
	MaxMessageBytes int32
	Retry           gbus.BackoffPolicy
	DeadLetter      *gbus.DeadLetterConfig
	Metrics         gbus.Metrics
	// Namespace prefixes topics, the group and the dead-letter topic.
	Namespace          string
	NamespaceSeparator string
}

// Consumer is a gbus.Consumer backed by a Kafka consumer group.
type Consumer struct {
	client   *kgo.Client
	cfg      ConsumerConfig
	namer    gbus.Namer
	producer *Producer

	mu      sync.Mutex
	closed  bool
	running bool
}

// NewConsumer connects the consumer group. A dead-letter producer is created from the
// same broker list so the DLQ write happens before the original offset advances.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("%w: at least one broker is required", gbus.ErrPermanent)
	}
	groupCfg := gbus.ConsumerConfig{
		Group:              cfg.Group,
		Topics:             cfg.Topics,
		HandlerConcurrency: cfg.HandlerConcurrency,
		Retry:              cfg.Retry,
		DeadLetter:         cfg.DeadLetter,
	}
	if err := groupCfg.Validate(); err != nil {
		return nil, err
	}
	cfg.HandlerConcurrency = groupCfg.HandlerConcurrency
	cfg.Retry = groupCfg.Retry
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = int32(gbus.DefaultMaxMessageBytes)
	}

	namer, err := gbus.NewNamer(cfg.Namespace, cfg.NamespaceSeparator)
	if err != nil {
		return nil, err
	}

	start := kgo.NewOffset().AtStart()
	if cfg.StartFrom == "latest" {
		start = kgo.NewOffset().AtEnd()
	}

	physicalTopics := make([]string, 0, len(cfg.Topics))
	for _, topic := range cfg.Topics {
		physicalTopics = append(physicalTopics, namer.Topic(topic))
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		// The group is namespaced too: two environments sharing a cluster and a group
		// id would steal each other's partitions.
		kgo.ConsumerGroup(namer.Group(cfg.Group)),
		kgo.ConsumeTopics(physicalTopics...),
		kgo.ConsumeResetOffset(start),
		// The library commits explicitly; automatic commits would advance past a
		// record whose handler has not acknowledged it.
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(cfg.MaxMessageBytes),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.CommitInterval > 0 {
		opts = append(opts, kgo.AutoCommitInterval(cfg.CommitInterval))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("gbus/kafka: consumer: %w", err)
	}

	c := &Consumer{client: client, cfg: cfg, namer: namer}
	if cfg.DeadLetter != nil {
		producer, err := NewProducer(ProducerConfig{
			Brokers:  cfg.Brokers,
			ClientID: cfg.ClientID + "-dlq",
			Metrics:  cfg.Metrics,
		})
		if err != nil {
			client.Close()
			return nil, err
		}
		c.producer = producer
	}
	return c, nil
}

// Run implements gbus.Consumer.
//
// Offsets advance only after a record is acknowledged (or written to the dead-letter
// topic), which is what makes the delivery promise at-least-once rather than
// at-most-once.
func (c *Consumer) Run(ctx context.Context, handler gbus.Handler) error {
	if handler == nil {
		return fmt.Errorf("%w: handler is required", gbus.ErrPermanent)
	}

	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("%w: consumer is already running", gbus.ErrPermanent)
	}
	c.running = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	// Attempts are per-process, which is why "retry N times" is not a durability
	// guarantee and the receipt must come from business state.
	attempts := map[string]int{}

	for {
		if err := ctx.Err(); err != nil {
			// Final commit of what was already acknowledged.
			commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.client.CommitUncommittedOffsets(commitCtx)
			cancel()
			return nil
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			continue
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			// Fetch errors are surfaced; a permanent one would loop forever.
			if c.cfg.Metrics != nil {
				c.cfg.Metrics.Consumed(topic, gbus.Retry, "fetch_error")
			}
			_ = classify(err)
		})

		var toCommit []*kgo.Record
		fetches.EachRecord(func(record *kgo.Record) {
			if ctx.Err() != nil {
				return
			}

			key := fmt.Sprintf("%s/%d/%d", record.Topic, record.Partition, record.Offset)
			attempts[key]++

			incoming := toRecord(record, attempts[key])
			// Handlers see the logical name; the prefix is a deployment detail.
			incoming.Topic = c.namer.Strip(record.Topic)
			if len(incoming.Value) > int(c.cfg.MaxMessageBytes) {
				// Too large to hand over: dead-letter it if configured, otherwise stop.
				if c.producer == nil {
					return
				}
				_ = c.deadLetter(ctx, incoming, "message_too_large")
				toCommit = append(toCommit, record)
				return
			}

			result := handler.Handle(ctx, incoming)
			if c.cfg.Metrics != nil {
				c.cfg.Metrics.Consumed(record.Topic, result.Outcome, result.Reason)
			}

			switch result.Outcome {
			case gbus.Ack:
				toCommit = append(toCommit, record)
				delete(attempts, key)

			case gbus.Retry:
				if c.cfg.Metrics != nil {
					c.cfg.Metrics.Retried(record.Topic, attempts[key])
				}
				// Do not commit: the record is redelivered when the group resumes.
				// Pausing the partition is the bounded alternative to a hot loop.
				c.client.PauseFetchPartitions(map[string][]int32{record.Topic: {record.Partition}})
				delay := result.RetryAfter
				if delay <= 0 {
					delay = c.cfg.Retry.Next(attempts[key])
				}
				timer := time.AfterFunc(delay, func() {
					c.client.ResumeFetchPartitions(map[string][]int32{record.Topic: {record.Partition}})
				})
				_ = timer

			case gbus.Reject:
				if c.producer == nil {
					return
				}
				if err := c.deadLetter(ctx, incoming, result.Reason); err != nil {
					if c.cfg.Metrics != nil {
						c.cfg.Metrics.DeadLettered(record.Topic, result.Reason, err)
					}
					// The record reached nowhere: keep the offset so it is retried.
					return
				}
				toCommit = append(toCommit, record)
				delete(attempts, key)

			default:
				return
			}
		})

		if len(toCommit) > 0 {
			if err := c.client.CommitRecords(ctx, toCommit...); err != nil {
				// A failed commit means redelivery, not loss: at-least-once still holds.
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("gbus/kafka: commit: %w", classify(err))
			}
		}
	}
}

// deadLetter writes the record to the dead-letter topic, preserving the origin
// coordinates.
func (c *Consumer) deadLetter(ctx context.Context, record gbus.Record, reason string) error {
	headers := append([]gbus.Header(nil), record.Headers...)
	headers = append(headers,
		gbus.Header{Key: "dlq-origin-topic", Value: []byte(record.Topic)},
		gbus.Header{Key: "dlq-origin-partition", Value: []byte(fmt.Sprint(record.Partition))},
		gbus.Header{Key: "dlq-origin-offset", Value: []byte(fmt.Sprint(record.Offset))},
		gbus.Header{Key: "dlq-reason", Value: []byte(reason)},
	)

	// A rejected record must still be written while shutting down.
	publishCtx := context.WithoutCancel(ctx)
	_, err := c.producer.Publish(publishCtx, gbus.Message{
		// The producer applies its own namespace; passing the logical name avoids a
		// doubly-prefixed dead-letter topic.
		Topic:   c.cfg.DeadLetter.Topic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: headers,
	})
	return err
}

// Close implements gbus.Consumer.
func (c *Consumer) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.client.CommitUncommittedOffsets(commitCtx)
	c.client.Close()

	if c.producer != nil {
		return c.producer.Close(ctx)
	}
	return nil
}

var _ gbus.Consumer = (*Consumer)(nil)
