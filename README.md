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

## Install

```bash
go get github.com/maxid/gbus
```

## License

MIT. See [LICENSE](LICENSE).
