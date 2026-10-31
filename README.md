English | [简体中文](README.zh-CN.md)

# gbus

A small message-bus abstraction for Go with Kafka and Redis backends.

> Status: **experimental scaffold** (v0.0.0-dev).

## Driver choices

- Kafka: Confluent official Go client (`confluent-kafka-go`); optional pure-Go fallback for CGO-free builds.
- Redis: official `go-redis` client.

## Planned features

- Producer/consumer abstractions with explicit subscription registration
- Kafka adapter
- Redis pub/sub adapter
- Batch handlers, ack/retry semantics

## Kafka driver

```go
producer, _ := kafka.NewProducer(kafka.ProducerConfig{Brokers: []string{"kafka:9092"}})
consumer, _ := kafka.NewConsumer(kafka.ConsumerConfig{
    Brokers: []string{"kafka:9092"}, Group: "iot-app", Topics: []string{"iothub_raw"},
})
```

`ProduceSync` backs `Publish`, so a nil error means the broker has the record. The
consumer commits **explicitly** (`DisableAutoCommit`): an offset advances only after
`Ack`, or after a rejected record reached the dead-letter topic. `Retry` pauses the
partition instead of hot-looping.

Error classification uses Kafka's own error table (`kerr.Error.Retriable`), not the
message text — which is why `UNKNOWN_TOPIC_OR_PARTITION` counts as retryable (the
topic may be being created) while `MESSAGE_TOO_LARGE` does not.

The driver's broker test needs a cluster:

```bash
GBUS_TEST_KAFKA_BROKERS=127.0.0.1:9092 go test -tags=integration ./kafka/
```

## Install

```bash
go get github.com/maxid/gbus
```

## License

MIT. See [LICENSE](LICENSE).
