package redisstream

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gbus"
)

// Producer writes to a Stream with XADD, one field named "p" holding the Base64
// payload — byte-for-byte the Java layout. Headers and the message key have no place
// in that layout, so they are dropped rather than stored somewhere a Java consumer
// would never look.
type Producer struct {
	client goredis.UniversalClient
	cfg    Config
}

// NewProducer wraps a Redis client.
func NewProducer(client goredis.UniversalClient, cfg Config) (*Producer, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client is required", gbus.ErrPermanent)
	}
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}
	return &Producer{client: client, cfg: cfg}, nil
}

// Publish implements gbus.Producer.
func (p *Producer) Publish(ctx context.Context, message gbus.Message) (gbus.PublishMetadata, error) {
	if err := message.Validate(gbus.DefaultMaxMessageBytes); err != nil {
		return gbus.PublishMetadata{}, err
	}
	if message.Topic == "" {
		return gbus.PublishMetadata{}, fmt.Errorf("%w: topic is required", gbus.ErrPermanent)
	}

	key := p.cfg.streamKey(message.Topic)
	_, err := p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: key,
		// Java uses `*` (server-assigned id); MAXLEN is not applied because the Java
		// adapter does not trim either.
		ID:     "*",
		Values: []any{payloadField, base64.StdEncoding.EncodeToString(message.Value)},
	}).Result()
	if err != nil {
		if p.cfg.Metrics != nil {
			p.cfg.Metrics.Published(message.Topic, len(message.Value), err)
		}
		return gbus.PublishMetadata{}, classify(err)
	}

	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Published(message.Topic, len(message.Value), nil)
	}
	return gbus.PublishMetadata{
		Topic:     message.Topic,
		Partition: 0,
		Offset:    0,
		Timestamp: message.Timestamp,
	}, nil
}

// Close implements gbus.Producer. The client belongs to the caller.
func (p *Producer) Close(context.Context) error { return nil }

// Consumer reads a stream as part of a consumer group.
type Consumer struct {
	client goredis.UniversalClient
	cfg    Config
	topic  string
	// worker distinguishes several consumers of the same subscription, exactly like
	// the Java `c-{topic}-{group}-{i}` naming.
	worker int
	batch  bool
	name   string
}

// Subscribe describes one subscription.
type Subscribe struct {
	Topic string
	// Group overrides Config.Group.
	Group string
	// Workers starts this many consumers for the subscription, matching the Java
	// `concurrency` argument.
	Workers int
}

// NewConsumer builds a consumer for one subscription.
func NewConsumer(client goredis.UniversalClient, cfg Config, subscribe Subscribe, batch bool) (*Consumer, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client is required", gbus.ErrPermanent)
	}
	if subscribe.Topic == "" {
		return nil, fmt.Errorf("%w: topic is required", gbus.ErrPermanent)
	}
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}
	if subscribe.Group != "" {
		cfg.Group = subscribe.Group
	}
	if subscribe.Workers < 1 {
		subscribe.Workers = 1
	}

	return &Consumer{
		client: client,
		cfg:    cfg,
		topic:  subscribe.Topic,
		worker: subscribe.Workers,
		batch:  batch,
	}, nil
}

// Run implements gbus.Consumer.
func (c *Consumer) Run(ctx context.Context, handler gbus.Handler) error {
	if handler == nil {
		return fmt.Errorf("%w: handler is required", gbus.ErrPermanent)
	}
	return c.consume(ctx, func(records []gbus.Record) gbus.Result {
		// Single mode: every entry gets its own verdict, matching Java's
		// processMessageBatch (ack on success, skip ack on error).
		worst := gbus.Result{Outcome: gbus.Ack}
		for _, record := range records {
			result := handler.Handle(ctx, record)
			if result.Outcome == gbus.Ack {
				_ = c.ack(ctx, record)
				continue
			}
			if result.Outcome == gbus.Reject {
				if err := c.deadLetter(ctx, record, result.Reason); err == nil {
					_ = c.ack(ctx, record)
					continue
				}
			}
			worst = result
		}
		return worst
	})
}

// Lag implements gbus.LagReporter for one Redis Stream consumer group. Redis 7
// reports the group lag directly; older servers that cannot are represented by
// entries read so callers still get a monotonic position.
func (c *Consumer) Lag(ctx context.Context) ([]gbus.PartitionLag, error) {
	key := c.cfg.streamKey(c.topic)
	info, err := c.client.XInfoStream(ctx, key).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, classify(err)
	}
	end := info.EntriesAdded
	if end < info.Length {
		// Some servers/proxies do not populate entries-added; length is the
		// best conservative position available there.
		end = info.Length
	}
	committed := int64(0)
	lag := end
	groups, err := c.client.XInfoGroups(ctx, key).Result()
	if err != nil {
		return nil, classify(err)
	}
	for _, group := range groups {
		if group.Name != c.cfg.Group {
			continue
		}
		if group.Lag >= 0 {
			lag = group.Lag
			committed = end - lag
		} else {
			committed = group.EntriesRead
			lag = end - committed
		}
		break
	}
	if lag < 0 {
		lag = 0
	}
	if committed < 0 {
		committed = 0
	}
	return []gbus.PartitionLag{{
		Topic: c.topic, Partition: 0, Committed: committed, End: end, Lag: lag,
	}}, nil
}

// RunBatch implements gbus.BatchConsumer: one poll becomes one call, and the whole
// batch is acknowledged only when the handler returns Ack — the Java
// processMessageBatchAsBatch behaviour.
func (c *Consumer) RunBatch(ctx context.Context, handler gbus.BatchHandler) error {
	if handler == nil {
		return fmt.Errorf("%w: batch handler is required", gbus.ErrPermanent)
	}
	return c.consume(ctx, func(records []gbus.Record) gbus.Result {
		result := handler.HandleBatch(ctx, records)
		if result.Outcome != gbus.Ack {
			return result
		}
		for _, record := range records {
			_ = c.ack(ctx, record)
		}
		return result
	})
}

// consume runs the read loop.
//
// The group is created from id 0 with MKSTREAM, so a stream that does not exist yet
// does not lose the first messages. BUSYGROUP is ignored, exactly like Java.
func (c *Consumer) consume(ctx context.Context, dispatch func([]gbus.Record) gbus.Result) error {
	key := c.cfg.streamKey(c.topic)
	if err := c.ensureGroup(ctx, key); err != nil {
		return err
	}

	name := c.consumerName()
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		// Claim entries another consumer abandoned, when the deployment enabled it.
		if c.cfg.PendingIdle > 0 {
			if err := c.claimPending(ctx, key, name, dispatch); err != nil {
				return err
			}
		}

		streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    c.cfg.Group,
			Consumer: name,
			Streams:  []string{key, ">"},
			Count:    readCount,
			Block:    c.blockDuration(),
		}).Result()
		if errors.Is(err, goredis.Nil) {
			// No new entries: a non-blocking configuration sleeps to avoid a hot loop.
			if c.cfg.PollBlock <= 0 {
				if !sleep(ctx, fallbackSleep) {
					return nil
				}
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return classify(err)
		}

		for _, stream := range streams {
			records := make([]gbus.Record, 0, len(stream.Messages))
			for _, message := range stream.Messages {
				records = append(records, c.toRecord(message, key))
			}
			if len(records) == 0 {
				continue
			}

			result := dispatch(records)
			if c.cfg.Metrics != nil {
				c.cfg.Metrics.Consumed(c.topic, result.Outcome, result.Reason)
			}
			if result.Outcome == gbus.Retry {
				if !sleep(ctx, c.retryDelay(result)) {
					return nil
				}
			}
		}
	}
}

// blockDuration renders PollBlock for XREADGROUP.
func (c *Consumer) blockDuration() time.Duration {
	if c.cfg.PollBlock <= 0 {
		// go-redis treats 0 as "block forever", which is not the Java fallback.
		return -1
	}
	return c.cfg.PollBlock
}

// consumerName follows the Java naming so operators see the same identifiers.
func (c *Consumer) consumerName() string {
	if c.cfg.Consumer != "" {
		return c.cfg.Consumer
	}
	return consumerName(c.topic, c.cfg.Group, 0, c.batch)
}

// ensureGroup creates the consumer group from id 0 and the stream if missing.
func (c *Consumer) ensureGroup(ctx context.Context, key string) error {
	err := c.client.XGroupCreateMkStream(ctx, key, c.cfg.Group, "0").Err()
	if err == nil {
		return nil
	}
	// BUSYGROUP means the group already exists; everything else is a real failure.
	if contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return classify(err)
}

// ack acknowledges one entry.
func (c *Consumer) ack(ctx context.Context, record gbus.Record) error {
	return c.client.XAck(ctx, c.cfg.streamKey(record.Topic), c.cfg.Group, record.ID).Err()
}

// toRecord maps a stream entry onto the driver-neutral record.
func (c *Consumer) toRecord(message goredis.XMessage, key string) gbus.Record {
	payload := ""
	if raw, ok := message.Values[payloadField]; ok {
		payload, _ = raw.(string)
	}
	value, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		// A malformed payload must not be silently dropped: hand it over empty so the
		// handler can reject it and the entry still reaches the dead-letter path.
		value = nil
	}

	return gbus.Record{
		Message: gbus.Message{
			Topic: c.cfg.streamKeyToTopic(key),
			Value: value,
		},
		ID:        message.ID,
		Partition: 0,
		Offset:    parseStreamID(message.ID),
	}
}

// parseStreamID keeps the numeric part of "ms-seq" as an offset-like value, which is
// what a log line wants to show.
func parseStreamID(id string) int64 {
	for i := 0; i < len(id); i++ {
		if id[i] == '-' {
			parsed, err := strconv.ParseInt(id[:i], 10, 64)
			if err != nil {
				return 0
			}
			return parsed
		}
	}
	return 0
}

// claimPending reclaims entries idle longer than PendingIdle.
func (c *Consumer) claimPending(ctx context.Context, key, name string, dispatch func([]gbus.Record) gbus.Result) error {
	entries, _, err := c.client.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream:   key,
		Group:    c.cfg.Group,
		Consumer: name,
		MinIdle:  c.cfg.PendingIdle,
		Start:    "0-0",
		Count:    int64(c.cfg.PendingClaimCount),
	}).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil
		}
		return classify(err)
	}
	if len(entries) == 0 {
		return nil
	}

	records := make([]gbus.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, c.toRecord(entry, key))
	}
	dispatch(records)
	return nil
}

// deadLetter is this driver's own extension (the Java adapter has none): a rejected
// entry is copied to the configured dead-letter stream before it is acknowledged.
func (c *Consumer) deadLetter(ctx context.Context, record gbus.Record, reason string) error {
	if c.cfg.DeadLetterTopic == "" {
		return fmt.Errorf("%w: no dead letter topic configured", gbus.ErrPermanent)
	}
	return c.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: c.cfg.streamKey(c.cfg.DeadLetterTopic),
		ID:     "*",
		Values: []any{
			payloadField, base64.StdEncoding.EncodeToString(record.Value),
			"dlq-origin-topic", record.Topic,
			"dlq-origin-id", record.ID,
			"dlq-reason", reason,
		},
	}).Err()
}

// Close implements gbus.Consumer.
func (c *Consumer) Close(context.Context) error { return nil }

func (c *Consumer) retryDelay(result gbus.Result) time.Duration {
	if result.RetryAfter > 0 {
		return result.RetryAfter
	}
	return gbus.DefaultBackoff.Next(1)
}

// sleep waits unless the context ends first; it reports whether to continue.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func contains(haystack, needle string) bool {
	if len(needle) > len(haystack) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return gbus.Temporary(err)
	}
	return gbus.Temporary(err)
}

var (
	_ gbus.Producer      = (*Producer)(nil)
	_ gbus.Consumer      = (*Consumer)(nil)
	_ gbus.BatchConsumer = (*Consumer)(nil)
)
