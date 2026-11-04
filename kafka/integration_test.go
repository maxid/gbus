//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maxid/gbus"
	"github.com/maxid/gbus/kafka"
)

// The tests need a real cluster: a Kafka driver that only passes unit tests proves
// the mapping, not the commit semantics.
//
//	GBUS_TEST_KAFKA_BROKERS=127.0.0.1:9092 go test -tags=integration ./kafka/
func brokers(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("GBUS_TEST_KAFKA_BROKERS")
	if raw == "" {
		t.Skip("GBUS_TEST_KAFKA_BROKERS is not set")
	}
	return []string{raw}
}

func topicName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestPublishThenConsume(t *testing.T) {
	brokers := brokers(t)
	topic := topicName("gbus.test")

	producer, err := kafka.NewProducer(kafka.ProducerConfig{Brokers: brokers, ClientID: "gbus-test"})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	meta, err := producer.Publish(ctx, gbus.Message{
		Topic: topic, Key: []byte("dev-1"), Value: []byte("hello"),
		Headers: []gbus.Header{{Key: "schemaVersion", Value: []byte("1")}},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if meta.Topic != topic {
		t.Fatalf("metadata = %+v", meta)
	}

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers, ClientID: "gbus-test", Group: topic, Topics: []string{topic}, StartFrom: "earliest",
	})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close(context.Background())

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	received := make(chan gbus.Record, 1)
	go func() {
		_ = consumer.Run(runCtx, gbus.HandlerFunc(func(_ context.Context, record gbus.Record) gbus.Result {
			select {
			case received <- record:
			default:
			}
			stop()
			return gbus.Result{Outcome: gbus.Ack}
		}))
	}()

	select {
	case record := <-received:
		if string(record.Value) != "hello" {
			t.Fatalf("value = %q", record.Value)
		}
		if value, ok := record.Header("schemaVersion"); !ok || string(value) != "1" {
			t.Fatalf("headers = %+v", record.Headers)
		}
	case <-ctx.Done():
		t.Fatal("no record received")
	}
}

func TestConsumerLag(t *testing.T) {
	brokers := brokers(t)
	topic := strings.TrimSpace(os.Getenv("GBUS_TEST_KAFKA_TOPIC"))
	if topic == "" {
		t.Skip("GBUS_TEST_KAFKA_TOPIC is not set; pre-create a topic to test lag")
	}
	group := topicName("gbus-lag-group")
	producer, err := kafka.NewProducer(kafka.ProducerConfig{Brokers: brokers, ClientID: "gbus-lag-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if _, err := producer.Publish(ctx, gbus.Message{Topic: topic, Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers, ClientID: "gbus-lag-test", Group: group, Topics: []string{topic}, StartFrom: "earliest",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close(context.Background())
	var _ gbus.LagReporter = consumer
	lag, err := consumer.Lag(ctx)
	if err != nil {
		t.Fatal(err)
	}
	total := int64(0)
	for _, partition := range lag {
		if partition.Topic != topic {
			t.Fatalf("topic = %q", partition.Topic)
		}
		total += partition.Lag
	}
	if total == 0 {
		t.Fatalf("initial lag = %+v", lag)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Run(runCtx, gbus.HandlerFunc(func(_ context.Context, record gbus.Record) gbus.Result {
			return gbus.Result{Outcome: gbus.Ack}
		}))
	}()
	deadline := time.Now().Add(15 * time.Second)
	for {
		lag, err = consumer.Lag(ctx)
		if err != nil {
			t.Fatal(err)
		}
		total = 0
		for _, partition := range lag {
			total += partition.Lag
		}
		if total == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
	if total != 0 {
		t.Fatalf("lag after ack = %+v", lag)
	}
}
