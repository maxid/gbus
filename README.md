# gbus

A small message-bus abstraction for Go, equivalent in intent to `xdmp-bus` (Kafka default; Redis pub/sub support).

> Status: **experimental scaffold** (v0.0.0-dev).

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
