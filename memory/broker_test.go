package memory_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gbus"
	"github.com/maxid/gbus/memory"
)

func TestPublishThenConsume(t *testing.T) {
	broker := memory.New()
	ctx := context.Background()

	meta, err := broker.Publish(ctx, gbus.Message{Topic: "raw", Key: []byte("dev-1"), Value: []byte("hello")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if meta.Offset != 0 || meta.Topic != "raw" {
		t.Fatalf("metadata = %+v", meta)
	}

	consumer, err := broker.NewConsumer(gbus.ConsumerConfig{Group: "g1", Topics: []string{"raw"}})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	var received []gbus.Record
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Run(runCtx, gbus.HandlerFunc(func(_ context.Context, record gbus.Record) gbus.Result {
			received = append(received, record)
			cancel()
			return gbus.Result{Outcome: gbus.Ack}
		}))
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not deliver")
	}

	if len(received) != 1 || string(received[0].Value) != "hello" {
		t.Fatalf("received = %+v", received)
	}
}

// The commit position advances only after Ack: a consumer that stops mid-way leaves
// the unacknowledged record to be redelivered.
func TestCommitOnlyAfterAck(t *testing.T) {
	broker := memory.New()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := broker.Publish(ctx, gbus.Message{Topic: "raw", Value: []byte("v")}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	consumer, _ := broker.NewConsumer(gbus.ConsumerConfig{Group: "g1", Topics: []string{"raw"}})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	seen := 0
	go func() {
		_ = consumer.Run(runCtx, gbus.HandlerFunc(func(_ context.Context, record gbus.Record) gbus.Result {
			mu.Lock()
			seen++
			count := seen
			mu.Unlock()
			if count == 2 {
				cancel() // stop before acknowledging the second record
			}
			return gbus.Result{Outcome: gbus.Ack}
		}))
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if broker.CommitPosition("raw", "g1", 0) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Two records were acknowledged; the third is still pending.
	if got := broker.CommitPosition("raw", "g1", 0); got != 2 {
		t.Fatalf("commit position = %d, want 2", got)
	}
}

func TestRetryRedelivers(t *testing.T) {
	broker := memory.New()
	ctx := context.Background()

	if _, err := broker.Publish(ctx, gbus.Message{Topic: "raw", Value: []byte("v")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	consumer, _ := broker.NewConsumer(gbus.ConsumerConfig{
		Group:  "g1",
		Topics: []string{"raw"},
		Retry:  gbus.BackoffPolicy{Min: time.Millisecond, Max: 2 * time.Millisecond, Factor: 2},
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var attempts atomic.Int64
	go func() {
		_ = consumer.Run(runCtx, gbus.HandlerFunc(func(_ context.Context, record gbus.Record) gbus.Result {
			if attempts.Add(1) < 3 {
				return gbus.Result{Outcome: gbus.Retry, Reason: "not ready"}
			}
			cancel()
			return gbus.Result{Outcome: gbus.Ack}
		}))
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() < 3 {
		time.Sleep(5 * time.Millisecond)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3 (retry redelivers)", attempts.Load())
	}
	if got := broker.CommitPosition("raw", "g1", 0); got != 1 {
		t.Fatalf("commit position = %d, want 1 after the final ack", got)
	}
}

// A rejected record goes to the dead-letter topic before the position advances, and
// keeps its origin metadata.
func TestRejectGoesToDeadLetter(t *testing.T) {
	broker := memory.New()
	ctx := context.Background()

	if _, err := broker.Publish(ctx, gbus.Message{
		Topic: "raw", Key: []byte("dev-1"), Value: []byte("bad"), Headers: []gbus.Header{{Key: "h", Value: []byte("v")}},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	consumer, err := broker.NewConsumer(gbus.ConsumerConfig{
		Group:      "g1",
		Topics:     []string{"raw"},
		DeadLetter: &gbus.DeadLetterConfig{Topic: "raw.dlq"},
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = consumer.Run(runCtx, gbus.HandlerFunc(func(_ context.Context, record gbus.Record) gbus.Result {
			return gbus.Result{Outcome: gbus.Reject, Reason: "malformed"}
		}))
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(broker.Records("raw.dlq")) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	dlq := broker.Records("raw.dlq")
	if len(dlq) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(dlq))
	}
	if string(dlq[0].Value) != "bad" || string(dlq[0].Key) != "dev-1" {
		t.Fatalf("dead letter lost the payload: %+v", dlq[0])
	}
	for _, want := range []string{"dlq-origin-topic", "dlq-origin-offset", "dlq-reason"} {
		if _, ok := dlq[0].Header(want); !ok {
			t.Errorf("dead letter missing header %q", want)
		}
	}
	if got := broker.CommitPosition("raw", "g1", 0); got != 1 {
		t.Fatalf("commit position = %d, want 1 after dead lettering", got)
	}
}

// Without a dead-letter queue a rejection must not advance the position: dropping it
// silently is how records disappear.
func TestRejectWithoutDLQStops(t *testing.T) {
	broker := memory.New()
	ctx := context.Background()

	if _, err := broker.Publish(ctx, gbus.Message{Topic: "raw", Value: []byte("bad")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	consumer, _ := broker.NewConsumer(gbus.ConsumerConfig{Group: "g1", Topics: []string{"raw"}})

	err := consumer.Run(ctx, gbus.HandlerFunc(func(context.Context, gbus.Record) gbus.Result {
		return gbus.Result{Outcome: gbus.Reject, Reason: "malformed"}
	}))
	if err == nil {
		t.Fatal("reject without a dead letter queue must surface an error")
	}
	if got := broker.CommitPosition("raw", "g1", 0); got != 0 {
		t.Fatalf("commit position = %d, want 0", got)
	}
}

func TestPublishValidates(t *testing.T) {
	broker := memory.New()
	ctx := context.Background()

	cases := []struct {
		name string
		msg  gbus.Message
		want error
	}{
		{"no topic", gbus.Message{}, gbus.ErrPermanent},
		{"key too long", gbus.Message{Topic: "t", Key: make([]byte, gbus.MaxKeyBytes+1)}, gbus.ErrPermanent},
		{"header without key", gbus.Message{Topic: "t", Headers: []gbus.Header{{Value: []byte("v")}}}, gbus.ErrPermanent},
	}
	for _, tc := range cases {
		if _, err := broker.Publish(ctx, tc.msg); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}

	broker.MaxBytes = 8
	if _, err := broker.Publish(ctx, gbus.Message{Topic: "t", Value: make([]byte, 9)}); !errors.Is(err, gbus.ErrMessageTooLarge) {
		t.Fatalf("oversized err = %v, want ErrMessageTooLarge", err)
	}
}

// The producer must not alias the caller's buffers.
func TestPublishCopiesTheMessage(t *testing.T) {
	broker := memory.New()
	value := []byte("original")
	if _, err := broker.Publish(context.Background(), gbus.Message{Topic: "t", Value: value}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	value[0] = 'X'
	records := broker.Records("t")
	if string(records[0].Value) != "original" {
		t.Fatalf("stored value mutated: %q", records[0].Value)
	}
}

func TestClosedBrokerRejectsPublish(t *testing.T) {
	broker := memory.New()
	if err := broker.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := broker.Publish(context.Background(), gbus.Message{Topic: "t"}); !errors.Is(err, gbus.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestConsumerConfigValidation(t *testing.T) {
	if _, err := memory.New().NewConsumer(gbus.ConsumerConfig{}); err == nil {
		t.Fatal("missing group must fail")
	}
	if _, err := memory.New().NewConsumer(gbus.ConsumerConfig{Group: "g"}); err == nil {
		t.Fatal("missing topics must fail")
	}
	if _, err := memory.New().NewConsumer(gbus.ConsumerConfig{
		Group: "g", Topics: []string{"t"}, DeadLetter: &gbus.DeadLetterConfig{},
	}); err == nil {
		t.Fatal("dead letter without a topic must fail")
	}
}

func TestBackoffGrowsAndClamps(t *testing.T) {
	policy := gbus.BackoffPolicy{Min: 10 * time.Millisecond, Max: 40 * time.Millisecond, Factor: 2}
	cases := map[int]time.Duration{
		1: 10 * time.Millisecond,
		2: 20 * time.Millisecond,
		3: 40 * time.Millisecond,
		4: 40 * time.Millisecond, // clamped
	}
	for attempt, want := range cases {
		if got := policy.Next(attempt); got != want {
			t.Errorf("Next(%d) = %s, want %s", attempt, got, want)
		}
	}
}

func TestErrorClassification(t *testing.T) {
	if !gbus.IsTemporary(gbus.Temporary(errors.New("broker down"))) {
		t.Fatal("a wrapped transient failure must be temporary")
	}
	if gbus.IsTemporary(gbus.Permanent(errors.New("bad schema"))) {
		t.Fatal("a permanent failure must not be temporary")
	}
	if gbus.IsTemporary(gbus.ErrClosed) || gbus.IsTemporary(gbus.ErrMessageTooLarge) {
		t.Fatal("closed and too-large are never temporary")
	}
	if !gbus.IsTemporary(gbus.ErrQueueFull) || !gbus.IsTemporary(gbus.ErrTimeout) {
		t.Fatal("queue full and timeout are retryable")
	}
	if !gbus.IsPermanent(errors.New("anything else")) {
		t.Fatal("an unclassified error defaults to permanent")
	}
}
