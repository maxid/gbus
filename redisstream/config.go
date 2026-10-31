// Package redisstream implements gbus on Redis Streams.
//
// This mirrors the Java `RedisStreamMessageBusAdapter`, not a Pub/Sub fan-out: a
// stream keeps history, a consumer group tracks delivery, and an entry is
// acknowledged only after the handler succeeded. Sharing a stream with a Java
// process requires the same key layout and the same payload encoding, so both are
// fixed here rather than configurable per call.
package redisstream

import (
	"fmt"
	"strings"
	"time"

	"github.com/maxid/gbus"
	"github.com/maxid/gns"
)

// Defaults copied from PlatformMessageBusProperties.Redis.
const (
	// DefaultStreamKeyPrefix is `streamKeyPrefix`; the Java default is "bus:".
	DefaultStreamKeyPrefix = "bus:"
	// DefaultGroup is `consumerGroup`; the Java default is "default".
	DefaultGroup = "default"
	// DefaultPollBlock is `pollBlockMs`; the Java default is 1000ms.
	DefaultPollBlock = time.Second
	// DefaultClaimCount is `pendingClaimCount`.
	DefaultClaimCount = 100
	// payloadField is the entry field holding the Base64 payload.
	payloadField = "p"
	// fallbackSleep is the Java FALLBACK_SLEEP_MS used when polling is non-blocking.
	fallbackSleep = time.Second
	// readCount is the Java `count(100)`.
	readCount = 100
)

// Config configures the stream driver. The zero value is usable: every field has the
// Java default.
type Config struct {
	// StreamKeyPrefix precedes the topic to form the stream key.
	StreamKeyPrefix string
	// Namespace is the gns cache namespace. When set and StreamKeyPrefix is
	// empty, keys become "<namespace>:bus:<topic>"; an explicit prefix wins.
	Namespace string
	// Group is the consumer group created when missing.
	Group string
	// PollBlock is how long XREADGROUP blocks; a non-positive value polls without
	// blocking and sleeps between empty reads (the Java fallback).
	PollBlock time.Duration
	// PendingIdle enables claiming entries another consumer left pending for at least
	// this long. Zero disables it, as in Java.
	PendingIdle time.Duration
	// PendingClaimCount bounds one claim pass.
	PendingClaimCount int
	// Consumer names this consumer inside the group. Empty derives one from the
	// topic, group and worker index, matching `c-{topic}-{group}-{i}`.
	Consumer string
	// DeadLetterTopic receives rejected entries. Empty means a rejected entry is not
	// acknowledged, so it stays pending rather than disappearing.
	DeadLetterTopic string
	// Metrics receives neutral counters.
	Metrics gbus.Metrics
}

// Normalize fills the Java defaults and validates the result.
func (c *Config) Normalize() error {
	c.Namespace = strings.TrimSpace(c.Namespace)
	if c.StreamKeyPrefix == "" && c.Namespace != "" {
		c.StreamKeyPrefix = gns.Join(":", c.Namespace, "bus") + ":"
	}
	if c.StreamKeyPrefix == "" {
		c.StreamKeyPrefix = DefaultStreamKeyPrefix
	}
	if c.Group == "" {
		c.Group = DefaultGroup
	}
	if c.PollBlock == 0 {
		c.PollBlock = DefaultPollBlock
	}
	if c.PendingClaimCount <= 0 {
		c.PendingClaimCount = DefaultClaimCount
	}
	if c.PendingIdle < 0 {
		return fmt.Errorf("%w: pending idle must not be negative", gbus.ErrPermanent)
	}
	return nil
}

// UseNamespace sets the stream key prefix from a gns set:
// "<cache>:bus:". Call it before constructing a producer or consumer.
func (c *Config) UseNamespace(set gns.Set) {
	c.StreamKeyPrefix = set.RedisKey("bus") + ":"
}

// streamKey is the Java `streamKeyPrefix + topic`.
func (c Config) streamKey(topic string) string { return c.StreamKeyPrefix + topic }

// streamKeyToTopic is the Java `streamKeyToTopic`: strip the configured prefix.
func (c Config) streamKeyToTopic(key string) string {
	if len(key) >= len(c.StreamKeyPrefix) && key[:len(c.StreamKeyPrefix)] == c.StreamKeyPrefix {
		return key[len(c.StreamKeyPrefix):]
	}
	return key
}

// consumerName is `c-{topic}-{group}-{worker}` for single handlers and
// `c-batch-{topic}-{group}-{worker}` for batch ones, so two modes never steal each
// other's pending entries.
func consumerName(topic, group string, worker int, batch bool) string {
	prefix := "c-"
	if batch {
		prefix = "c-batch-"
	}
	return fmt.Sprintf("%s%s-%s-%d", prefix, topic, group, worker)
}
