# Changelog

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased (0.3.0)

### Features

- Added offset management (export, import, reset) with status timestamps (#62)
- Apply logging level changes without pod restarts using Log4j2 hot-reload (#61)
- Added manual restart support for connectors via annotations (#59)
- Made logging levels configurable per connector and cluster (#58)
- Preconfigured cluster to log to console in JSON format using Log4j2 layout template (#55, #56)
- Added kafka-connect-datagen image for testing (#50)
- Added startup probe configuration (#48)
- Added observed generation to status for proper reconciliation tracking (#41)
- Moved connector state control (pause/resume/stop) from annotations to spec fields (#37)
- Added connector and task status fields to CRD status (#38)
- Added affinity and tolerations support for cluster pods (#35)
- Added PodDisruptionBudget support for cluster pods (#34)
- Set KAFKA_HEAP_OPTS to use 75% of container memory (#33)
- Added auto-restart of failed connectors and tasks (#33)
