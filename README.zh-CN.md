[English](README.md) | 简体中文

# gbus

Go 的小型消息总线抽象，支持 Kafka 与 Redis 后端。

> 状态：**实验性脚手架**（v0.0.0-dev）。

## 驱动选择

- Kafka：Confluent 官方 Go 客户端（`confluent-kafka-go`）；为无 CGO 构建提供可选的纯 Go 回退。
- Redis：官方 `go-redis` 客户端。

## 计划中的特性

- 生产者/消费者抽象，显式订阅注册
- Kafka 适配器
- Redis pub/sub 适配器
- 批量处理器、ack/重试语义

## 安装

```bash
go get github.com/maxid/gbus
```

## 许可证

MIT。见 [LICENSE](LICENSE)。
