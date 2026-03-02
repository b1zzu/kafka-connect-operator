# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-03-02

### Added

- Offset management (export, import, reset) with status timestamps ([#62])
- Log4j2 hot-reload to apply logging level changes without pod restarts ([#61])
- Manual restart support for connectors via annotations ([#59])
- Configurable logging levels per connector and cluster ([#58])
- JSON console logging using Log4j2 layout template ([#55], [#56])
- Kafka Connect Datagen image for testing ([#50])
- Startup probe for cluster pods ([#48])
- Observed generation in status for proper reconciliation tracking ([#41])
- Connector and task status fields to CRD status ([#38])
- Connector state control (pause/resume/stop) via spec fields ([#37])
- Unit tests for cluster and connector reconcilers ([#36])
- Affinity and tolerations support for cluster pods ([#35])
- PodDisruptionBudget support for cluster pods ([#34])
- Auto-restart of failed connectors and tasks ([#33])
- KAFKA_HEAP_OPTS set to use 75% of container memory ([#33])

### Changed

- Moved connector state control from annotations to spec fields ([#37])
- Refactored cluster controller to reduce duplicate code ([#40])
- Updated Go dependencies to v0.35.2 ([#39])
- Distribution install.yaml pushed as part of releases ([#42], [#54])
- Updated reference section and quick-start guide ([#45])

### Fixed

- Startup probe configuration ([#48])

[Unreleased]: https://github.com/b1zzu/kafka-connect-operator/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/b1zzu/kafka-connect-operator/compare/v0.2.0...v0.3.0

[#33]: https://github.com/b1zzu/kafka-connect-operator/pull/33
[#34]: https://github.com/b1zzu/kafka-connect-operator/pull/34
[#35]: https://github.com/b1zzu/kafka-connect-operator/pull/35
[#36]: https://github.com/b1zzu/kafka-connect-operator/pull/36
[#37]: https://github.com/b1zzu/kafka-connect-operator/pull/37
[#38]: https://github.com/b1zzu/kafka-connect-operator/pull/38
[#39]: https://github.com/b1zzu/kafka-connect-operator/pull/39
[#40]: https://github.com/b1zzu/kafka-connect-operator/pull/40
[#41]: https://github.com/b1zzu/kafka-connect-operator/pull/41
[#42]: https://github.com/b1zzu/kafka-connect-operator/pull/42
[#45]: https://github.com/b1zzu/kafka-connect-operator/pull/45
[#48]: https://github.com/b1zzu/kafka-connect-operator/pull/48
[#50]: https://github.com/b1zzu/kafka-connect-operator/pull/50
[#54]: https://github.com/b1zzu/kafka-connect-operator/pull/54
[#55]: https://github.com/b1zzu/kafka-connect-operator/pull/55
[#56]: https://github.com/b1zzu/kafka-connect-operator/pull/56
[#58]: https://github.com/b1zzu/kafka-connect-operator/pull/58
[#59]: https://github.com/b1zzu/kafka-connect-operator/pull/59
[#61]: https://github.com/b1zzu/kafka-connect-operator/pull/61
[#62]: https://github.com/b1zzu/kafka-connect-operator/pull/62
