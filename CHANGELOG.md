# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

- Initial scaffold.
- `redisstream.Config.Namespace` derives `<namespace>:bus:` stream keys while
  the zero value keeps the default `bus:` layout; gbus no longer depends on gns.
