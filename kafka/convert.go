// Package kafka implements gbus on Apache Kafka through franz-go.
//
// franz-go is pure Go, so the library builds without CGO. The adapter is the only
// place that knows Kafka types: business code keeps seeing gbus.Message/Record.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maxid/gbus"
)

// toRecord maps a Kafka record onto the driver-neutral one.
func toRecord(record *kgo.Record, attempts int) gbus.Record {
	headers := make([]gbus.Header, 0, len(record.Headers))
	for _, header := range record.Headers {
		headers = append(headers, gbus.Header{Key: header.Key, Value: header.Value})
	}
	return gbus.Record{
		Message: gbus.Message{
			Topic:     record.Topic,
			Key:       record.Key,
			Value:     record.Value,
			Headers:   headers,
			Timestamp: record.Timestamp,
		},
		Partition: record.Partition,
		Offset:    record.Offset,
		Attempts:  attempts,
	}
}

// fromMessage maps a gbus message onto a Kafka record.
func fromMessage(message gbus.Message) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(message.Headers))
	for _, header := range message.Headers {
		headers = append(headers, kgo.RecordHeader{Key: header.Key, Value: header.Value})
	}
	record := &kgo.Record{
		Topic:     message.Topic,
		Key:       message.Key,
		Value:     message.Value,
		Headers:   headers,
		Timestamp: message.Timestamp,
	}
	if record.Timestamp.IsZero() {
		// The broker rule is "producer time"; leaving it zero would make the
		// timestamp useless for ordering diagnostics.
		record.Timestamp = time.Now()
	}
	return record
}

// classify maps a franz-go error onto the gbus classification. Callers never parse
// driver strings.
func classify(err error) error {
	if err == nil {
		return nil
	}
	// Kafka error codes carry their own retryability; the code decides, not the text.
	var brokerErr *kerr.Error
	if errors.As(err, &brokerErr) {
		if brokerErr.Retriable {
			return gbus.Temporary(err)
		}
		return gbus.Permanent(err)
	}

	// Connection-level shapes (EOF, syscall errors) and a cancelled/deadlined call
	// are retryable; a deadline means the outcome is unknown, so the caller must
	// retry with a business idempotency key.
	if kgo.IsRetryableBrokerErr(err) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return gbus.Temporary(err)
	}
	return gbus.Permanent(err)
}

// errUnsupported is returned for configurations the driver cannot honour.
func errUnsupported(reason string) error {
	return fmt.Errorf("%w: %s", gbus.ErrUnsupportedSemantics, reason)
}
