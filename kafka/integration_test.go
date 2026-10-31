//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"os"
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
