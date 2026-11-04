//go:build integration

package redisstream_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gbus"
	"github.com/maxid/gbus/redisstream"
)

func redisClient(t *testing.T) goredis.UniversalClient {
	t.Helper()
	addr := os.Getenv("GBUS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("GBUS_TEST_REDIS_ADDR is not set")
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("GBUS_TEST_REDIS_PASSWORD")})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestConsumerLagRedis(t *testing.T) {
	client := redisClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	topic := fmt.Sprintf("gbus-lag-%d", time.Now().UnixNano())
	producer, err := redisstream.NewProducer(client, redisstream.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := producer.Publish(ctx, gbus.Message{Topic: topic, Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := redisstream.NewConsumer(client, redisstream.Config{}, redisstream.Subscribe{Topic: topic, Group: "g1"}, false)
	if err != nil {
		t.Fatal(err)
	}
	var _ gbus.LagReporter = consumer
	lag, err := consumer.Lag(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(lag) != 1 || lag[0].Topic != topic || lag[0].End != 3 || lag[0].Committed != 0 || lag[0].Lag != 3 {
		t.Fatalf("lag = %+v", lag)
	}
}
