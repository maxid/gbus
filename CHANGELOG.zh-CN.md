[English](CHANGELOG.md) | 简体中文

# 变更日志

本文件记录本项目的重要变更。

## [Unreleased]

- 初始脚手架。
- `redisstream.Config.Namespace` 派生 `<namespace>:bus:` stream key；零值仍保留默认 `bus:` 布局；gbus 不再依赖 gns。
