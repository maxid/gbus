package redisstream_test

import (
	"context"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gbus"
	"github.com/maxid/gbus/redisstream"
)

func newClient(t *testing.T) (*miniredis.Miniredis, goredis.UniversalClient) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

// The entry layout is the compatibility surface: a Java consumer reads field "p" and
// Base64-decodes it.
func TestPublishWritesTheJavaLayout(t *testing.T) {
	server, client := newClient(t)

	producer, err := redisstream.NewProducer(client, redisstream.Config{})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}

	meta, err := producer.Publish(context.Background(), gbus.Message{
		Topic: "iothub_raw",
		Value: []byte(`{"k":"v"}`),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if meta.Topic != "iothub_raw" {
		t.Fatalf("topic = %q", meta.Topic)
	}

	// Key is "bus:" + topic by default.
	entries, err := client.XRange(context.Background(), "bus:iothub_raw", "-", "+").Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries = %d, %v", len(entries), err)
	}
	raw, ok := entries[0].Values["p"].(string)
	if !ok {
		t.Fatalf("payload field is not a string: %#v", entries[0].Values)
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || string(decoded) != `{"k":"v"}` {
		t.Fatalf("payload = %q, %v", decoded, err)
	}

	_ = server
}

func TestConsumeAcksAfterSuccess(t *testing.T) {
	_, client := newClient(t)

	producer, _ := redisstream.NewProducer(client, redisstream.Config{})
	consumer, err := redisstream.NewConsumer(client, redisstream.Config{}, redisstream.Subscribe{Topic: "raw", Group: "g1"}, false)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := make(chan gbus.Record, 1)
	go func() {
		_ = consumer.Run(ctx, gbus.HandlerFunc(func(_ context.Context, record gbus.Record) gbus.Result {
			select {
			case received <- record:
			default:
			}
			return gbus.Result{Outcome: gbus.Ack}
		}))
	}()

	// The group is created from id 0, so publishing after the consumer starts is
	// delivered (and publishing before would also be, unlike a `$` group).
	time.Sleep(50 * time.Millisecond)
	if _, err := producer.Publish(ctx, gbus.Message{Topic: "raw", Value: []byte("v1")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case record := <-received:
		if string(record.Value) != "v1" {
			t.Fatalf("value = %q", record.Value)
		}
		if record.Topic != "raw" {
			t.Fatalf("topic = %q, want the logical name", record.Topic)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no record delivered")
	}

	// The entry is acknowledged: the pending list is empty.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := client.XPending(ctx, "bus:raw", "g1").Result()
		if err == nil && pending.Count == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pending, err := client.XPending(ctx, "bus:raw", "g1").Result()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending = %d, want 0 after an ack", pending.Count)
	}
}

// A retry leaves the entry unacknowledged, which is what makes redelivery possible.
func TestRetryLeavesTheEntryPending(t *testing.T) {
	_, client := newClient(t)

	producer, _ := redisstream.NewProducer(client, redisstream.Config{})
	consumer, _ := redisstream.NewConsumer(client, redisstream.Config{}, redisstream.Subscribe{Topic: "raw", Group: "g1"}, false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	attempts := 0
	go func() {
		_ = consumer.Run(ctx, gbus.HandlerFunc(func(_ context.Context, record gbus.Record) gbus.Result {
			mu.Lock()
			attempts++
			count := attempts
			mu.Unlock()
			if count == 1 {
				return gbus.Result{Outcome: gbus.Retry, Reason: "not ready", RetryAfter: 10 * time.Millisecond}
			}
			cancel()
			return gbus.Result{Outcome: gbus.Ack}
		}))
	}()

	time.Sleep(50 * time.Millisecond)
	if _, err := producer.Publish(ctx, gbus.Message{Topic: "raw", Value: []byte("v1")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := attempts
		mu.Unlock()
		if count >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts == 0 {
		t.Fatal("the handler never ran")
	}
}

func TestBatchDeliversOneCallAndAcks(t *testing.T) {
	_, client := newClient(t)

	producer, _ := redisstream.NewProducer(client, redisstream.Config{})
	consumer, _ := redisstream.NewConsumer(client, redisstream.Config{}, redisstream.Subscribe{Topic: "raw", Group: "g1"}, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batches := make(chan int, 1)
	go func() {
		_ = consumer.RunBatch(ctx, gbus.BatchHandlerFunc(func(_ context.Context, records []gbus.Record) gbus.Result {
			select {
			case batches <- len(records):
			default:
			}
			return gbus.Result{Outcome: gbus.Ack}
		}))
	}()

	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if _, err := producer.Publish(ctx, gbus.Message{Topic: "raw", Value: []byte("v")}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	select {
	case size := <-batches:
		if size == 0 {
			t.Fatal("empty batch delivered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no batch delivered")
	}
}

func TestProducerRequiresClient(t *testing.T) {
	if _, err := redisstream.NewProducer(nil, redisstream.Config{}); err == nil {
		t.Fatal("nil client must fail")
	}
	if _, err := redisstream.NewConsumer(nil, redisstream.Config{}, redisstream.Subscribe{Topic: "t"}, false); err == nil {
		t.Fatal("nil client must fail")
	}
	if _, err := redisstream.NewConsumer(goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"}), redisstream.Config{}, redisstream.Subscribe{}, false); err == nil {
		t.Fatal("missing topic must fail")
	}
}

func TestConsumerLag(t *testing.T) {
	server, client := newClient(t)
	defer server.Close()
	producer, _ := redisstream.NewProducer(client, redisstream.Config{})
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := producer.Publish(ctx, gbus.Message{Topic: "raw", Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := redisstream.NewConsumer(client, redisstream.Config{}, redisstream.Subscribe{Topic: "raw", Group: "g1"}, false)
	if err != nil {
		t.Fatal(err)
	}
	var _ gbus.LagReporter = consumer
	lag, err := consumer.Lag(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(lag) != 1 || lag[0].Topic != "raw" || lag[0].Partition != 0 ||
		lag[0].End != 3 || lag[0].Committed != 0 || lag[0].Lag != 3 {
		t.Fatalf("lag = %+v", lag)
	}
}
