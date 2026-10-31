# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

- Initial scaffold.
- `redisstream.Config.Namespace` and `UseNamespace(gns.Set)` derive
  `<cache>:bus:` stream keys while the zero value keeps the Java `bus:` layout.
