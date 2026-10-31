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

## Namespaces

Several environments usually share one cluster, so topic and consumer-group names carry
a namespace prefix. Business code keeps the logical name:

```go
producer, _ := kafka.NewProducer(kafka.ProducerConfig{
    Brokers:   []string{"kafka:9092"},
    Namespace: "xdmp-dev",   // separator defaults to "."
})
producer.Publish(ctx, gbus.Message{Topic: "iothub_raw", ...})   // -> xdmp-dev.iothub_raw
```

The prefix is applied at the driver boundary and **stripped again on delivery**, so a
handler sees `iothub_raw`. The **consumer group is namespaced too** — two environments
sharing a group id would steal each other's partitions. An invalid prefix (space, slash,
colon) fails at construction, not on the first publish.

## Redis Streams driver

The platform's Redis adapter is **Streams, not Pub/Sub** — a stream keeps history and a
consumer group tracks delivery. The layout matches the Java adapter byte for byte:

| Thing | Value |
|---|---|
| stream key | `bus:` + topic (`StreamKeyPrefix`) |
| payload field | `p`, **Base64** encoded |
| group | `default` (`Group`), created from id `0` with `MKSTREAM` |
| read | `XREADGROUP count 100`, blocking `PollBlock` (default 1s) |
| ack | only after the handler returns `Ack` |
| claim | `PendingIdle` (default off) + `PendingClaimCount` (100) |
| headers / key | **not stored** — the Java layout has no place for them |

```go
producer, _ := redisstream.NewProducer(client, redisstream.Config{})
consumer, _ := redisstream.NewConsumer(client, redisstream.Config{},
    redisstream.Subscribe{Topic: "iothub_raw", Group: "iot-app"}, false)
```

The zero config keeps the Java-compatible `bus:` prefix. When several
applications share Redis, set `Namespace` or use `UseNamespace(set)`:

```go
cfg := redisstream.Config{}
cfg.UseNamespace(set) // app:bus:<topic>
```

An explicit `StreamKeyPrefix` wins over `Namespace`, which keeps a Java/Go
dual-run on the same stream possible.

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

franz-go negotiates protocol versions automatically, so one binary works against Kafka
**0.8 through 3.x/4.x**; no per-cluster version pinning is required.

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
