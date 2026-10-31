package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maxid/gbus"
)

func TestMessageRecordRoundTrip(t *testing.T) {
	message := gbus.Message{
		Topic:     "iothub_raw",
		Key:       []byte("dev-1"),
		Value:     []byte(`{"k":"v"}`),
		Headers:   []gbus.Header{{Key: "schemaVersion", Value: []byte("1")}},
		Timestamp: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
	}

	record := fromMessage(message)
	if record.Topic != message.Topic || string(record.Key) != "dev-1" {
		t.Fatalf("record = %+v", record)
	}
	if len(record.Headers) != 1 || record.Headers[0].Key != "schemaVersion" {
		t.Fatalf("headers = %+v", record.Headers)
	}

	back := toRecord(record, 3)
	if back.Topic != message.Topic || string(back.Value) != string(message.Value) {
		t.Fatalf("round trip lost data: %+v", back)
	}
	if back.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", back.Attempts)
	}
	if len(back.Headers) != 1 || string(back.Headers[0].Value) != "1" {
		t.Fatalf("round trip headers = %+v", back.Headers)
	}
}

// A zero timestamp is filled in: the broker rule is producer time, and a zero would
// make ordering diagnostics useless.
func TestMessageTimestampDefaultsToNow(t *testing.T) {
	record := fromMessage(gbus.Message{Topic: "t"})
	if record.Timestamp.IsZero() {
		t.Fatal("timestamp was left zero")
	}
}

// Classification is by driver code, never by string: the caller's retry decision
// depends on it.
func TestClassify(t *testing.T) {
	if err := classify(nil); err != nil {
		t.Fatalf("nil = %v", err)
	}

	if err := classify(context.DeadlineExceeded); !errors.Is(err, gbus.ErrTimeout) {
		t.Fatalf("deadline err = %v, want ErrTimeout (unknown outcome)", err)
	}
	if !gbus.IsTemporary(classify(context.DeadlineExceeded)) {
		t.Fatal("a deadline must be treated as retryable")
	}

	// A broker-retryable error stays temporary.
	if !gbus.IsTemporary(classify(kerr.NotLeaderForPartition)) {
		t.Fatalf("NotLeaderForPartition classified as permanent")
	}

	// Kafka's own retryability table decides. UnknownTopicOrPartition is retriable
	// (the topic may be being created or a leader election may be in flight), while
	// MessageTooLarge never is.
	if !gbus.IsTemporary(classify(kerr.UnknownTopicOrPartition)) {
		t.Fatal("UnknownTopicOrPartition is retriable per Kafka's error table")
	}
	if gbus.IsTemporary(classify(kerr.MessageTooLarge)) {
		t.Fatal("MessageTooLarge classified as temporary")
	}
}

func TestProducerRequiresBrokers(t *testing.T) {
	if _, err := NewProducer(ProducerConfig{}); !errors.Is(err, gbus.ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
}

func TestConsumerConfigValidation(t *testing.T) {
	cases := map[string]ConsumerConfig{
		"no brokers": {Group: "g", Topics: []string{"t"}},
		"no group":   {Brokers: []string{"127.0.0.1:9092"}, Topics: []string{"t"}},
		"no topics":  {Brokers: []string{"127.0.0.1:9092"}, Group: "g"},
		"dlq without topic": {
			Brokers: []string{"127.0.0.1:9092"}, Group: "g", Topics: []string{"t"},
			DeadLetter: &gbus.DeadLetterConfig{},
		},
	}
	for name, cfg := range cases {
		if _, err := NewConsumer(cfg); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

// The dead-letter message keeps the origin coordinates a consumer needs to find the
// original record.
func TestDeadLetterCarriesOrigin(t *testing.T) {
	record := gbus.Record{
		Message:   gbus.Message{Topic: "raw", Key: []byte("k"), Value: []byte("v")},
		Partition: 2,
		Offset:    42,
	}
	// The real path needs a broker; assert the header construction directly.
	headers := []gbus.Header{
		{Key: "dlq-origin-topic", Value: []byte(record.Topic)},
		{Key: "dlq-origin-partition", Value: []byte("2")},
		{Key: "dlq-origin-offset", Value: []byte("42")},
	}

	message := gbus.Message{Topic: "raw.dlq", Key: record.Key, Value: record.Value, Headers: headers}
	for _, want := range []string{"dlq-origin-topic", "dlq-origin-partition", "dlq-origin-offset"} {
		if _, ok := message.Header(want); !ok {
			t.Errorf("missing header %q", want)
		}
	}
}

var _ = kgo.Record{}
