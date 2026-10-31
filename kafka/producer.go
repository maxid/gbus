package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maxid/gbus"
)

// ProducerConfig configures the Kafka producer.
type ProducerConfig struct {
	Brokers  []string
	ClientID string
	// Acks is "-1"/"all" for full ISR acknowledgement. The default is all: a nil
	// error from Publish must mean the broker has the message.
	Acks string
	// Idempotent enables the broker-side idempotent producer, which removes
	// duplicates caused by internal retries. It does not make the business call
	// idempotent.
	Idempotent      bool
	MaxMessageBytes int
	RequestTimeout  time.Duration
	// Metrics receives neutral counters.
	Metrics gbus.Metrics
}

// Producer is a synchronous gbus.Producer: Publish returns after the broker
// acknowledged at the configured level, so there is no hidden application queue.
type Producer struct {
	client   *kgo.Client
	cfg      ProducerConfig
	maxBytes int64
	closed   bool
}

// NewProducer connects to Kafka.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("%w: at least one broker is required", gbus.ErrPermanent)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		// Synchronous from the caller's point of view: no unbounded client buffer.
		kgo.ProducerLinger(0),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	// franz-go enables the idempotent producer by default; the flag only opts out,
	// because turning it off lets internal retries duplicate records.
	if !cfg.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	if cfg.RequestTimeout > 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(cfg.RequestTimeout))
	}

	maxBytes := cfg.MaxMessageBytes
	if maxBytes <= 0 {
		maxBytes = gbus.DefaultMaxMessageBytes
	}
	opts = append(opts, kgo.ProducerBatchMaxBytes(int32(maxBytes)))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("gbus/kafka: producer: %w", err)
	}
	return &Producer{client: client, cfg: cfg, maxBytes: int64(maxBytes)}, nil
}

// Publish implements gbus.Producer.
func (p *Producer) Publish(ctx context.Context, message gbus.Message) (gbus.PublishMetadata, error) {
	if p.closed {
		return gbus.PublishMetadata{}, gbus.ErrClosed
	}
	if err := message.Validate(p.maxBytes); err != nil {
		return gbus.PublishMetadata{}, err
	}

	record := fromMessage(message)
	// ProduceSync blocks until the broker acknowledges every record, which is what
	// makes a nil error meaningful.
	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		classified := classify(err)
		if p.cfg.Metrics != nil {
			p.cfg.Metrics.Published(message.Topic, len(message.Value), classified)
		}
		return gbus.PublishMetadata{}, classified
	}

	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Published(message.Topic, len(message.Value), nil)
	}
	produced := results[0].Record
	return gbus.PublishMetadata{
		Topic:     produced.Topic,
		Partition: produced.Partition,
		Offset:    produced.Offset,
		Timestamp: produced.Timestamp,
	}, nil
}

// Close implements gbus.Producer: it flushes what the client still holds.
func (p *Producer) Close(ctx context.Context) error {
	if p.closed {
		return nil
	}
	p.closed = true

	// Bounded flush: a broker that stopped answering must not hang shutdown.
	done := make(chan struct{})
	go func() {
		p.client.Flush(context.Background())
		close(done)
	}()

	select {
	case <-done:
		p.client.Close()
		return nil
	case <-ctx.Done():
		// Records may still be in flight; report it instead of pretending.
		p.client.Close()
		return ctx.Err()
	}
}

var _ gbus.Producer = (*Producer)(nil)
