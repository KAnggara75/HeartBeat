# Architecture Decision Records (ADRs)

## ADR-001: Standalone Go Daemon with Dual Operating Modes

- **Status**: Accepted
- **Date**: 2026-09-16
- **Source**: Developer interview & codebase implementation ([`cmd/heartbeat/main.go`](file:///Users/i/work/KAnggara75/hb/cmd/heartbeat/main.go))
- **Context**: The keep-alive service needs to execute reliably across diverse environments (local VPS, Docker containers, Kubernetes, or serverless scheduled runners like GitHub Actions/Linux cron).
- **Decision**: Implement a unified Go application supporting two execution modes: continuous background daemon mode (managed by an internal cron scheduler) and an immediate exit one-shot mode triggered via the `--once` flag.
- **Consequences**:
  - *Positive*: Zero code duplication between daemon and batch jobs; simplifies containerization and CI/CD cron executions.
  - *Negative*: Binary includes scheduling libraries (`robfig/cron/v3`) even when invoked strictly in one-shot mode.

---

## ADR-002: Pure Go Kafka Engine (`segmentio/kafka-go`)

- **Status**: Accepted
- **Date**: 2026-09-16
- **Source**: Codebase evidence ([`internal/heartbeat/kafka.go`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat/kafka.go), [`go.mod`](file:///Users/i/work/KAnggara75/hb/go.mod))
- **Context**: Connecting to Aiven Kafka requires robust support for mutual TLS (mTLS) with custom client certificates and SASL mechanisms. Standard CGO-based libraries (`confluent-kafka-go`) require system-installed `librdkafka`, complicating multi-platform builds and Alpine Docker images.
- **Decision**: Standardize on `github.com/segmentio/kafka-go`, a 100% pure Go implementation.
- **Consequences**:
  - *Positive*: Enables `CGO_ENABLED=0` static binary compilation; frictionless multi-stage Alpine Docker builds without external C dependencies; native `crypto/tls` integration for Aiven certificates.
  - *Negative*: Slightly higher memory footprint under extremely high-throughput streaming compared to C-optimized bindings, which is negligible for periodic heartbeat workloads.

---

## ADR-003: PostgREST HTTP REST API for Supabase Keep-Alive

- **Status**: Accepted
- **Date**: 2026-09-16
- **Source**: Codebase evidence ([`internal/heartbeat/supabase.go`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat/supabase.go))
- **Context**: Supabase free-tier projects automatically pause after 7 days of inactivity. Activity can be signaled either through direct PostgreSQL connections (port 5432/6543) or through the PostgREST HTTP REST API. Direct DB connections require connection pooling management and open database ports which are often restricted by corporate firewalls.
- **Decision**: Utilize standard HTTP `POST` requests to Supabase PostgREST (`/rest/v1/<table_name>`) with Bearer API token authentication, paired with automatic retention cleanup (`DELETE`).
- **Consequences**:
  - *Positive*: Uses standard HTTP port 443; lightweight implementation using Go `net/http` without heavy database drivers; activity explicitly registers on Supabase Cloud API activity metrics.
  - *Negative*: Requires target Supabase database to have the `heartbeats` table provisioned and appropriate RLS policies configured.

---

## ADR-004: Dual Produce-Consume Cycle for Kafka Health Verification

- **Status**: Accepted
- **Date**: 2026-09-16
- **Source**: Codebase evidence ([`internal/heartbeat/kafka.go`](file:///Users/i/work/KAnggara75/hb/internal/heartbeat/kafka.go))
- **Context**: In cloud Kafka providers like Aiven, merely connecting or producing data might not exercise consumer group offsets or partition reader coordination, which some platforms use to evaluate subscriber liveliness.
- **Decision**: Perform both a Produce operation (writing a timestamped heartbeat payload) and a Consumer read operation within a short deadline.
- **Consequences**:
  - *Positive*: Keeps both producer connections and consumer group offsets warm and active.
  - *Negative*: Requires consumer permissions and read access on the specified topic.

---

## ADR-005: Centralized Configuration via scc2go (Spring Cloud Config)

- **Status**: Accepted
- **Date**: 2026-09-16
- **Source**: Developer request & codebase implementation ([`internal/config/config.go`](file:///Users/i/work/KAnggara75/hb/internal/config/config.go), [`go.mod`](file:///Users/i/work/KAnggara75/hb/go.mod))
- **Context**: In enterprise and cloud environments, configuration secrets, target aliases, and intervals are frequently managed centrally within a Spring Cloud Config Server rather than baked into static local YAML files or mounted container volumes.
- **Decision**: Integrate [`github.com/KAnggara75/scc2go`](https://github.com/KAnggara75/scc2go) to fetch configurations remotely from Spring Cloud Config via HTTP/HTTPS with basic/token authentication and TLS toggle. Map settings into the unified `Config` model with automated normalization for indexed slice properties (`key[N].prop`).
- **Consequences**:
  - *Positive*: Enables native integration with Spring Cloud Config microservice infrastructure; zero manual file management in ephemeral container environments; seamless fallback to local YAML when `SCC_URL` is omitted.
  - *Negative*: Introduces `scc2go`, `viper`, and `resty` dependencies.

