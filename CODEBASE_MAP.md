# Codebase Navigation Map

## Project Overview
- **Repository**: `KAnggara75/HeartBeat`
- **Module Root**: `github.com/KAnggara75/HeartBeat`
- **Language & Runtime**: Go (1.24+) [CONFIRMED: `go.mod`]
- **Architecture Pattern**: Modular Layered Service with Scheduler Daemon and Auxiliary HTTP Management Server [CONFIRMED]

---

## `cmd/heartbeat`
- **Responsibility**: Application entry point, CLI flag parsing (`--config`, `--once`, `--version`, `--scc-url`, `--scc-auth`, `--scc-insecure`), signal traps, daemon lifecycle control, and graceful shutdown orchestration.
- **Entry / Key Files**:
  - [`cmd/heartbeat/main.go`](file:///Users/i/work/KAnggara75/hb/cmd/heartbeat/main.go) — Main bootstrap, flag parsing, service coordinator wiring, and OS signal listener (`SIGINT`/`SIGTERM`).
- **Dependencies**: Standard library (`context`, `flag`, `os`, `os/signal`, `syscall`, `time`), internal packages (`config`, `heartbeat`, `logger`, `scheduler`, `server`).
- **Consumers**: OS binary execution, Docker entrypoint, CI/CD runners.
- **Key Notes**: Supports dual runtime modes: continuous daemon mode (default) and one-shot execution mode (`--once`), with configuration loading from local YAML or remote Spring Cloud Config.

---

## `internal/config`
- **Responsibility**: Configuration loading from local YAML files or remote Spring Cloud Config Server via `scc2go`, environment variable expansion, indexed slice normalization, default values injection, and schema validation.
- **Entry / Key Files**:
  - [`internal/config/config.go`](file:///Users/i/work/KAnggara75/hb/internal/config/config.go) — YAML/JSON/Viper struct definitions, Spring Cloud Config fetcher integration, `${VAR:-default}` regex expansion, and integrity validation.
  - [`internal/config/config_test.go`](file:///Users/i/work/KAnggara75/hb/internal/config/config_test.go) — Unit tests verifying YAML deserialization, Spring Cloud Config mock server loading, and error boundaries.
- **Dependencies**: `github.com/KAnggara75/scc2go`, `github.com/spf13/viper`, `gopkg.in/yaml.v3`, standard library (`encoding/json`, `os`, `regexp`, `strconv`, `time`).
- **Consumers**: [`cmd/heartbeat/main.go`](file:///Users/i/work/KAnggara75/hb/cmd/heartbeat/main.go).
- **External Integrations**: Spring Cloud Config Server (via HTTP/HTTPS REST).
- **Key Notes**: Reconstructs flattened indexed properties (`prop[0].subprop`) from Spring Cloud Config into clean Go slices. Enforces alias uniqueness across both Supabase and Kafka instances.

---

## `internal/heartbeat`
- **Responsibility**: Core keep-alive ping execution, multi-database Supabase HTTP interactions, multi-cluster Kafka produce/consume cycles, and concurrent dispatching.
- **Entry / Key Files**:
  - [`internal/heartbeat/manager.go`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat/manager.go) — Coordinates concurrent target executions via goroutines, tracks last execution states, and calculates run summaries.
  - [`internal/heartbeat/supabase.go`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat/supabase.go) — Supabase PostgREST client performing HTTP `POST` inserts into heartbeat tables and optional `DELETE` retention purges.
  - [`internal/heartbeat/kafka.go`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat/kafka.go) — Kafka client implementing mTLS (CA, client cert/key) and SASL (`SCRAM-SHA-512`, `SCRAM-SHA-256`, `PLAIN`) produce/consume keep-alive logic.
  - [`internal/heartbeat/supabase_test.go`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat/supabase_test.go) — Unit test suite mocking Supabase HTTP endpoints and verifying headers/payloads.
  - [`internal/heartbeat/kafka_test.go`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat/kafka_test.go) — Unit test suite validating security protocol configuration builders.
- **Dependencies**: `github.com/segmentio/kafka-go`, standard library (`crypto/tls`, `crypto/x509`, `encoding/json`, `net/http`, `net/url`, `sync`, `time`).
- **Consumers**: [`internal/scheduler`](file:///Users/i/work/KAnggara75/hb/internal/scheduler), [`internal/server`](file:///Users/i/work/KAnggara75/hb/internal/server), [`cmd/heartbeat/main.go`](file:///Users/i/work/KAnggara75/hb/cmd/heartbeat/main.go).
- **External Integrations**:
  - Supabase REST API (`/rest/v1/<table_name>`)
  - Apache Kafka / Aiven Cloud Kafka brokers
- **Key Notes**: Kafka reader executes with short context deadlines so absence of ongoing topic traffic does not block the daemon cycle.

---

## `internal/scheduler`
- **Responsibility**: Periodic job scheduling with second precision, managing cron expressions and interval timers.
- **Entry / Key Files**:
  - [`internal/scheduler/scheduler.go`](file:///Users/i/work/KAnggara75/hb/internal/scheduler/scheduler.go) — Wraps `robfig/cron/v3`, parses per-target intervals or cron specs, and handles startup runs.
- **Dependencies**: `github.com/robfig/cron/v3`, [`internal/config`](file:///Users/i/work/KAnggara75/hb/internal/config), [`internal/heartbeat`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat).
- **Consumers**: [`cmd/heartbeat/main.go`](file:///Users/i/work/KAnggara75/hb/cmd/heartbeat/main.go).
- **Key Notes**: Transparently handles standard 5-field cron syntax by padding to 6-field second precision if necessary.

---

## `internal/server`
- **Responsibility**: Lightweight HTTP control and observability server.
- **Entry / Key Files**:
  - [`internal/server/server.go`](file:///Users/i/work/KAnggara75/hb/internal/server/server.go) — HTTP handlers for `/healthz`, `/livez`, `/status`, and `/trigger`.
- **Dependencies**: Standard library (`net/http`, `encoding/json`, `context`, `time`), [`internal/heartbeat`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat).
- **Consumers**: External monitoring (Uptime Kuma, Prometheus blackbox, Docker healthcheck, manual triggers).
- **Key Notes**: Supports asynchronous triggers and synchronous trigger evaluation (`/trigger?sync=true`).

---

## `internal/logger`
- **Responsibility**: Global structured logging initialization.
- **Entry / Key Files**:
  - [`internal/logger/logger.go`](file:///Users/i/work/KAnggara75/hb/internal/logger/logger.go) — Factory configuring `log/slog` handlers (Text or JSON) and minimum log levels.
- **Dependencies**: Standard library `log/slog`.
- **Consumers**: [`cmd/heartbeat/main.go`](file:///Users/i/work/KAnggara75/hb/cmd/heartbeat/main.go).

---

## `scripts`
- **Responsibility**: Database migration and DDL setup scripts.
- **Entry / Key Files**:
  - [`scripts/supabase_schema.sql`](file:///Users/i/work/KAnggara75/hb/scripts/supabase_schema.sql) — DDL creating `heartbeats` table, indexes, and permissive RLS policies.
- **Consumers**: Developers setting up target Supabase projects.
