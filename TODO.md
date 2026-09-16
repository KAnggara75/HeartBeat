# Project TODO & Technical Debt

## 1. Immediate Tasks
_Urgent tasks affecting reliability, security, or immediate execution._
- [ ] Establish initial automated test workflow in GitHub Actions (`.github/workflows/ci.yml`).
- [ ] Configure production alert notifications (e.g. Discord, Telegram, or Slack webhook) when a heartbeat check consistently fails.

## 2. Existing Code Annotations (TODO / FIXME)
_Annotations found directly in source code._
- _None currently present in codebase._

## 3. Technical Debt & Structural Improvements
_Long-term architectural and refactoring initiatives._
- [ ] **Exponential Backoff & Retries**: Introduce retry policies for transient network timeouts on Supabase HTTP endpoints and Kafka broker handshakes.
- [ ] **Prometheus Metrics Exporter**: Expose a `/metrics` Prometheus endpoint alongside `/status` to track ping counts, latencies (histogram), and failure rates natively in Grafana.
- [ ] **Direct PostgreSQL Pooler Support**: Add optional direct `database/sql` driver connection for Supabase as an alternative to the PostgREST REST API.
- [ ] **Dynamic Configuration Hot-Reload**: Implement `SIGHUP` or fsnotify file watching to reload `config.yaml` without terminating active daemon processes.
- [ ] **Kafka Topic Auto-Creation**: Add optional topic creation check via Kafka Admin API if the configured heartbeat topic does not yet exist.
