// Package gbus is a thin message-bus abstraction.
//
// It hides driver types (Kafka client, Redis client) but not the reliability
// difference between them: Kafka gives at-least-once delivery with per-partition
// ordering, Redis Pub/Sub is an ephemeral fan-out with no replay. A consumer that
// needs durability must not be pointed at the ephemeral driver.
//
// The library does not own topic names, message schemas, idempotency keys or device
// presence. Those belong to the consuming contract.
package gbus

import (
	"fmt"
	"strings"
	"time"
)

// MaxKeyBytes and MaxHeaderBytes bound the routing data. They are deliberately
// small: a key is for partitioning, not for payload.
const (
	MaxKeyBytes    = 512
	MaxHeaderBytes = 4 << 10
	// DefaultMaxMessageBytes is the default ceiling for one Value.
	DefaultMaxMessageBytes = 1 << 20
)

// Header is one message header.
type Header struct {
	Key   string
	Value []byte
}

// Message is what a producer publishes.
//
// Key decides the partition, and therefore the ordering: two messages with the same
// key are delivered in publish order. Value is opaque to the bus.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// Record is one delivered message.
type Record struct {
	Message
	// ID is whatever correlation id the driver can offer. It is not globally
	// unique and must not be used as an idempotency key.
	ID        string
	Partition int32
	Offset    int64
	// Attempts counts deliveries in this process. It resets on restart, which is
	// why "retry at most N times" is not a durability guarantee.
	Attempts int
}

// PublishMetadata is what the broker acknowledged.
type PublishMetadata struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

// Validate rejects a message the bus cannot publish. It is called by every driver,
// so the rules do not drift between them.
func (m Message) Validate(maxBytes int64) error {
	if strings.TrimSpace(m.Topic) == "" {
		return fmt.Errorf("%w: topic is required", ErrPermanent)
	}
	if len(m.Key) > MaxKeyBytes {
		return fmt.Errorf("%w: key is %d bytes, limit %d", ErrPermanent, len(m.Key), MaxKeyBytes)
	}
	for _, header := range m.Headers {
		if strings.TrimSpace(header.Key) == "" {
			return fmt.Errorf("%w: header key is required", ErrPermanent)
		}
		if len(header.Key) > MaxKeyBytes {
			return fmt.Errorf("%w: header key %q is too long", ErrPermanent, header.Key)
		}
		if len(header.Value) > MaxHeaderBytes {
			return fmt.Errorf("%w: header %q value is %d bytes, limit %d", ErrPermanent, header.Key, len(header.Value), MaxHeaderBytes)
		}
	}
	if maxBytes > 0 && int64(len(m.Value)) > maxBytes {
		return fmt.Errorf("%w: value is %d bytes, limit %d", ErrMessageTooLarge, len(m.Value), maxBytes)
	}
	return nil
}

// Clone copies the byte slices so a driver may retain the message past the call
// without aliasing the caller's buffers. Drivers must clone before storing.
func (m Message) Clone() Message {
	out := Message{
		Topic:     m.Topic,
		Key:       cloneBytes(m.Key),
		Value:     cloneBytes(m.Value),
		Timestamp: m.Timestamp,
	}
	if len(m.Headers) > 0 {
		out.Headers = make([]Header, len(m.Headers))
		for i, header := range m.Headers {
			out.Headers[i] = Header{Key: header.Key, Value: cloneBytes(header.Value)}
		}
	}
	return out
}

// Header returns the first header with the given key.
func (m Message) Header(key string) ([]byte, bool) {
	for _, header := range m.Headers {
		if header.Key == key {
			return header.Value, true
		}
	}
	return nil, false
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
