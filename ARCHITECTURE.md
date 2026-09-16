# Technical Architecture

## 1. System Overview & Component Diagram

HeartBeat is designed as a standalone, lightweight, cloud-agnostic daemon in Go that continuously or periodically exercises cloud data infrastructure (Supabase PostgreSQL via PostgREST and Aiven/Apache Kafka clusters) to prevent inactivity-based suspension, auto-pausing, or cold shutdowns.

```mermaid
graph TD
    subgraph Host / Container Environment
        CLI["CLI Entrypoint<br/>cmd/heartbeat/main.go"]
        CONFIG["Config Loader<br/>internal/config"]
        LOGGER["Logger (slog)<br/>internal/logger"]
        SCHED["Scheduler Engine<br/>internal/scheduler"]
        SRV["HTTP Server (:8080)<br/>internal/server"]
        MGR["Heartbeat Manager<br/>internal/heartbeat"]
        SB_SVC["Supabase Service<br/>internal/heartbeat/supabase.go"]
        KF_SVC["Kafka Service<br/>internal/heartbeat/kafka.go"]
    end

    subgraph External Cloud & Infrastructure
        SCC[("Spring Cloud Config Server<br/>scc2go REST")]
        SB1[("Supabase Instance 1<br/>PostgREST REST API")]
        SB2[("Supabase Instance N<br/>PostgREST REST API")]
        KF1[["Kafka Cluster 1<br/>Aiven mTLS / SSL"]]
        KF2[["Kafka Cluster N<br/>SASL_SSL SCRAM-512"]]
    end

    CONFIG -.->|Remote Fetch via scc2go| SCC
    CLI -->|Loads YAML or SCC| CONFIG
    CLI -->|Initializes| LOGGER
    CLI -->|Bootstraps| MGR
    CLI -->|Starts Scheduler| SCHED
    CLI -->|Exposes Endpoints| SRV

    SCHED -->|Triggers Periodic Jobs| MGR
    SRV -->|Manual /trigger & /status| MGR

    MGR -->|Goroutine Pool| SB_SVC
    MGR -->|Goroutine Pool| KF_SVC

    SB_SVC -->|HTTP POST /rest/v1/heartbeats| SB1
    SB_SVC -->|HTTP POST /rest/v1/heartbeats| SB2
    KF_SVC -->|Produce & Consume| KF1
    KF_SVC -->|Produce & Consume| KF2
```

---

## 2. Request & Execution Flow

### 2.1 Daemon Lifecycle
1. **Bootstrap**: `main.go` parses flags (`-config`, `-once`, `-version`, `-scc-url`, `-scc-auth`, `-scc-insecure`) and loads configuration either remotely from Spring Cloud Config via `scc2go` or locally from `config.yaml` with environment variable resolution (`${VAR:-default}`).
2. **Initialization**: Configures global structured logger (`slog`), instantiates `heartbeat.Manager`, and builds per-target execution specifications.
3. **Execution Routing**:
   - If `--once` is set: Invokes `Manager.RunAll(ctx)`, outputs execution results, and exits with code 0 (success) or 1 (failures detected).
   - If daemon mode: Starts `robfig/cron/v3` scheduler, optional HTTP server on port 8080, and registers OS signal listener (`os.Interrupt`, `syscall.SIGTERM`).
4. **Shutdown**: Upon receiving interrupt signals, the scheduler is drained, HTTP server shuts down with 5-second context timeout, and connections are closed gracefully.

### 2.2 Supabase Heartbeat Sequence
```mermaid
sequenceDiagram
    participant Sched as Scheduler / Manager
    participant SbSvc as SupabaseService
    participant PostgREST as Supabase API (/rest/v1)
    participant PG as PostgreSQL Storage

    Sched->>SbSvc: Ping(ctx, &SupabaseConfig)
    Note over SbSvc: Construct JSON payload (alias, status, timestamp, custom metadata)
    SbSvc->>PostgREST: POST /rest/v1/<table_name><br/>Headers: apikey, Bearer Auth, Prefer: return=minimal
    PostgREST->>PG: INSERT INTO heartbeats ...
    PG-->>PostgREST: 201 Created / 200 OK
    PostgREST-->>SbSvc: HTTP 201 Created
    opt Cleanup Enabled (Retention > 0 days)
        SbSvc->>PostgREST: DELETE /rest/v1/<table_name>?alias=eq.<alias>&created_at=lt.<cutoff>
        PostgREST->>PG: DELETE FROM heartbeats WHERE ...
        PostgREST-->>SbSvc: HTTP 204 No Content
    end
    SbSvc-->>Sched: HeartbeatResult (Success, Latency, Message)
```

### 2.3 Kafka Heartbeat Sequence (Aiven Keep-Alive)
```mermaid
sequenceDiagram
    participant Sched as Scheduler / Manager
    participant KfSvc as KafkaService
    participant Broker as Kafka Broker (Aiven)

    Sched->>KfSvc: Ping(ctx, &KafkaConfig)
    Note over KfSvc: Configure TLS (Root CA + Client Cert/Key) or SASL (SCRAM/PLAIN)
    opt Produce Enabled
        KfSvc->>Broker: kafka.Writer.WriteMessages(ctx, Message{Key, Payload})
        Broker-->>KfSvc: Ack (RequireOne)
    end
    opt Consume Enabled
        KfSvc->>Broker: kafka.Reader.ReadMessage(ctxWithTimeout)
        Broker-->>KfSvc: Return Latest Message or Timeout
        Note over KfSvc: Active reader interaction marks consumer group active
    end
    KfSvc-->>Sched: HeartbeatResult (Success, Latency, Message)
```

---

## 3. Concurrency & Resource Management

- **Goroutine Fan-out & Fan-in**:
  `Manager.RunAll` spawns dedicated goroutines for each configured Supabase database and Kafka cluster, using a `sync.WaitGroup` to await completion and a mutex-protected slice to gather results.
- **Thread-Safe State Caching**:
  `Manager` maintains a `sync.RWMutex` guarding `lastResults map[string]*HeartbeatResult` and `lastRun time.Time`, ensuring safe concurrent reads by the `/status` HTTP endpoint during active heartbeat sweeps.
- **Context Timeouts & Deadlines**:
  - Supabase HTTP operations utilize `context.WithTimeout` (default 15s or configurable via `timeout`).
  - Kafka consumer interactions run within a scoped context deadline (default 8s or configurable via `consume.timeout`), preventing blocking when no new messages arrive on the topic.
- **Pure Go Kafka Engine**:
  Utilizes `segmentio/kafka-go`, eliminating CGO and dynamic linking to `librdkafka`, allowing minimal Alpine/scratch container distribution.

---

## 4. Error Handling & Fault Tolerance

- **Isolated Failure Domains**: Failure of a single Supabase instance or Kafka cluster does not halt or fail sibling checks during a round.
- **Structured Error Return**: Every check produces a unified `HeartbeatResult` struct capturing success status, latency, error messages, and timestamps.
- **Multi-Status HTTP Reporting**: When one or more targets fail, `/status` responds with HTTP 207 (Multi-Status), enabling external monitors to identify partial degradation.
- **Exit Code Propagation**: In `--once` mode, any failed target triggers an exit code of `1` so orchestrators (GitHub Actions, Kubernetes CronJobs) immediately detect failures.

---

## 5. Observability & Telemetry

- **Structured Logging (`log/slog`)**: Standard library structured logger emitting either human-readable colored text or production-grade JSON logs (`app.log_format: "json"`).
- **HTTP Health Endpoints**:
  - `GET /healthz` & `GET /livez`: Fast 200 OK liveness probes.
  - `GET /status`: Detailed JSON payload with per-target latency, timestamps, and messages.
  - `POST /trigger`: On-demand manual trigger supporting synchronous (`?sync=true`) and asynchronous execution.

---

## 6. Data & Domain Boundaries

- **Keep-Alive Payloads**: Ephemeral records consisting of `id` (UUID), `alias` (string), `source` (string), `status` ("alive"), `created_at` (TIMESTAMPTZ), and optional arbitrary key-value metadata.
- **Storage Conservation**: Retention purges prevent infinite table growth in target databases without requiring database triggers or manual vacuuming.
