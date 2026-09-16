# Project Context & Grounded Memory

## 1. Project Purpose & High-Level Mission
HeartBeat (`github.com/KAnggara75/HeartBeat`) is an automated infrastructure keep-alive service. Its primary mission is to prevent developer and production cloud resources—specifically **Supabase PostgreSQL databases** and **Aiven Apache Kafka clusters**—from entering inactive, paused, or suspended states on free or low-tier plans by generating consistent, configurable synthetic activity.

---

## 2. System Boundaries
- **In-Scope**:
  - Multi-database Supabase pinging via PostgREST HTTP REST API (`/rest/v1/<table_name>`).
  - Automatic retention cleanup (`DELETE`) to maintain a lean database footprint.
  - Multi-cluster Kafka produce and consume operations with mTLS and SASL support.
  - Scheduling daemon with flexible per-target intervals and cron expressions.
  - Local HTTP server providing liveness probes (`/healthz`, `/livez`), JSON status reporting (`/status`), and manual triggers (`/trigger`).
  - Containerization (Dockerfile, Docker Compose) and one-shot execution mode (`--once`).
- **Out-of-Scope**:
  - Direct database DDL migrations or schema management inside external databases (delegated to manual SQL script execution).
  - Web UI / Dashboard for configuring targets (configuration is code/file-driven via `config.yaml`).
  - Long-term time-series metrics storage (intended to be scraped by external tools like Prometheus).

---

## 3. Main Actors & Personas
- **HeartBeat Daemon**: Background autonomous agent executing periodic ping and cleanup jobs.
- **System Administrator / DevOps**: Operator deploying the binary/container and providing credentials via `config.yaml` or environment variables.
- **External Observability Probe**: Monitoring agents (e.g., Uptime Kuma, Prometheus) polling `/healthz` or `/status`.

---

## 4. Domain Concepts & Ubiquitous Glossary
- **Keep-Alive**: Synthetic traffic sent to a cloud resource at intervals shorter than the cloud provider's inactivity threshold (e.g., Supabase 7-day inactivity pause limit).
- **Target Alias**: A unique identifier assigned to each monitored resource instance (e.g., `supabase-prod`, `aiven-staging`) used for logging, metrics, and deduplication.
- **Retention Days**: The duration in days after which synthetic heartbeat rows in Supabase are safely deleted to prevent uncontrolled table growth.
- **One-Shot Mode**: An execution pattern where the binary performs all checks once and immediately terminates with a pass/fail exit code.

---

## 5. External Systems & Integration Points
- **Supabase Cloud**:
  - Integration: PostgREST REST API over HTTPS (`/rest/v1/<table_name>`).
  - Authentication: Supabase `apikey` and `Authorization: Bearer <key>` headers.
- **Aiven for Apache Kafka**:
  - Integration: Native Kafka TCP protocol.
  - Authentication: Mutual TLS (Root CA + Client Certificate + Client Key) or SASL (`SCRAM-SHA-512`, `SCRAM-SHA-256`, `PLAIN`).
- **Spring Cloud Config Server**:
  - Integration: HTTP/HTTPS REST API via [`github.com/KAnggara75/scc2go`](https://github.com/KAnggara75/scc2go).
  - Authentication: Optional Basic / Bearer token via `SCC_AUTH` or `--scc-auth`.

---

## 6. Runtime Environment & Constraints
- **Runtime**: Linux / macOS (ARM64, AMD64), Go 1.24+.
- **Resource Footprint**: Minimal (~15-30 MB RAM idle in Alpine container).
- **Network Requirements**: Egress HTTPS (port 443) for Supabase; egress TCP (custom broker ports, typically 10000-30000) for Kafka brokers.
- **Inbound Ports**: Optional HTTP server on port 8080.

---

## 7. Coding Conventions & Standards
- **Standard Library First**: Prioritize Go standard library packages (`log/slog`, `net/http`, `crypto/tls`, `time`, `context`).
- **Pure Go Architecture**: Zero CGO dependencies (`CGO_ENABLED=0`) to ensure frictionless cross-compilation and Alpine compatibility.
- **Conventional Commits**: Commits strictly follow Conventional Commits 1.0.0 (`feat`, `fix`, `chore`, `docs`, `ci`, `build`, `test`).
- **Secret Safety**: No plaintext credentials committed to version control; enforced via `.gitignore` and `${ENV_VAR}` substitution.

---

## 8. Known Limitations
- `UNKNOWN`: Target SLA and automated notification channel preferences (Telegram, Discord, Slack) not yet configured in source code.
- `UNKNOWN`: Direct PostgreSQL TCP pooler connection (port 5432/6543) not yet implemented as fallback if PostgREST is disabled.
