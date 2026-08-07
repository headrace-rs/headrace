## 0.1.10 (2026-08-07)

### Features

- snapshot window state on demand
- serve window state over gRPC
- add the inspect client subcommand
- add hand-rolled landing page

## 0.1.9 (2026-08-07)

### Features

- add multi-input plumbing for join
- implement the join core
- validate inputs at build time

## 0.1.8 (2026-08-07)

### Features

- add optional name to window and map

## 0.1.7 (2026-08-06)

### Features

- add map expression transform
- separate on_invalid from on_missing

## 0.1.6 (2026-08-06)

### Features

- add sliding windows

## 0.1.5 (2026-08-06)

### Features

- window lateness and idle-timeout fields
- event-time windows with watermarks
- idle_timeout flushes quiet windows

## 0.1.4 (2026-08-05)

### Features

- helm chart for the otlp deployment

## 0.1.3 (2026-08-05)

### Features

- normalize cumulative sums to delta

## 0.1.2 (2026-08-05)

### Features

- add otlp source and sink
- grpc receiver and exporter in core
- enable the otlp feature
- graceful receiver shutdown

## 0.1.1 (2026-08-04)

### Features

- scaffold project
- structured logging with --log-format json

### Fixes

- use the real release-plz action tag v0.5
