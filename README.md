# HeartBeat 💓

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](Dockerfile)

**HeartBeat** adalah *lightweight background daemon* & CLI tool berbasis Go yang dirancang untuk menjaga layanan *cloud* (seperti **Supabase** dan **Aiven Kafka**) tetap aktif dan mencegah penangguhan (*suspend / pause*) akibat *inactivity* pada *free tier* / *dev tier*.

---

## ✨ Fitur Utama

- 🗄️ **Multi-Database Supabase Support**:
  - Konfigurasi banyak database sekaligus dengan penamaan **alias** (misal: `supabase-prod`, `supabase-staging`).
  - Melakukan operasi `INSERT` berkala via Supabase REST API (PostgREST) untuk mereset *inactivity timer*.
  - Dilengkapi fitur **auto retention cleanup** (otomatis menghapus log detak jantung yang lebih tua dari *X* hari agar tidak menghabiskan kuota *storage*).

- ⚡ **Multi-Cluster Kafka Support (Aiven Cloud, Upstash, Self-Hosted)**:
  - Konfigurasi banyak cluster Kafka dengan sistem **alias**.
  - **Dukungan Penuh Keamanan Aiven**:
    - **mTLS (Mutual TLS)**: CA Certificate, Client Certificate, dan Client Key (format file atau injeksi env string PEM).
    - **SASL / SCRAM**: `SCRAM-SHA-512`, `SCRAM-SHA-256`, dan `PLAIN`.
  - **Full Activity**: Melakukan **Publish** (Produce pesan detak jantung) dan **Consume** (membaca pesan / mengaktifkan consumer group) agar metrik broker & topic tetap hangat.

- ⏱️ **Penjadwalan Fleksibel**:
  - Interval independen per-target (contoh: `interval: "4h"` atau `cron: "0 */6 * * *"`).
  - Interval global *fallback* (contoh: `default_interval: "6h"`).
  - Eksekusi otomatis saat *startup* (`run_on_startup: true`).

- 🔄 **Mode Ganda**:
  - **Background Daemon**: Berjalan 24/7 dengan *graceful shutdown* (`SIGINT`/`SIGTERM`).
  - **One-Shot Mode (`--once`)**: Eksekusi satu siklus lalu keluar (ideal untuk GitHub Actions scheduler, Linux cron job, atau K8s Job).

- 🩺 **HTTP Health & Monitoring Server**:
  - `GET /healthz` & `GET /livez`: Liveness probe untuk Docker / Kubernetes / Uptime Kuma.
  - `GET /status`: Menampilkan ringkasan status, latensi, dan riwayat pemeriksaan setiap target secara *real-time*.
  - `POST /trigger`: Memicu eksekusi *heartbeat* secara manual via HTTP request.

- ☁️ **Centralized Config via Spring Cloud Config ([`scc2go`](https://github.com/KAnggara75/scc2go))**:
  - Konfigurasi dapat diambil langsung secara dinamis dari **Spring Cloud Config Server** via pustaka [`scc2go`](https://github.com/KAnggara75/scc2go).
  - Mendukung autentikasi *Basic / Bearer token* dan opsi *bypass TLS certificate*.
  - Otomatis melakukan normalisasi format properti berindeks (`supabase[0].alias`, `kafka[0].brokers[0]`).
  - Mendukung *fallback* otomatis ke `config.yaml` lokal jika URL Spring Cloud Config tidak disetel.

- 🔐 **Konfigurasi Ramah Lingkungan (*12-Factor App*)**:
  - Mendukung substitusi variabel lingkungan dalam YAML: `${VAR_NAME}` atau `${VAR_NAME:-default_value}`.

---

## 📁 Struktur Direktori

```text
hb/
├── cmd/
│   └── heartbeat/
│       └── main.go               # Entrypoint aplikasi & CLI flags
├── internal/
│   ├── config/                   # Parser konfigurasi & env expansion
│   ├── heartbeat/                # Logika ping Supabase & Kafka
│   ├── logger/                   # Structured logging (slog)
│   ├── scheduler/                # Cron & interval scheduler
│   └── server/                   # HTTP monitoring server
├── scripts/
│   └── supabase_schema.sql       # Script SQL inisialisasi tabel Supabase
├── certs/
│   └── .gitkeep                  # Direktori sertifikat SSL/TLS Kafka
├── config.example.yaml           # Contoh konfigurasi lengkap
├── docker-compose.yml            # Konfigurasi Docker Compose
├── Dockerfile                    # Multi-stage lightweight Docker image
├── Makefile                      # Shortcut perintah build, run, test
├── LICENSE                       # Lisensi MIT
└── README.md
```

---

## 🚀 Panduan Memulai Cepat

### 1. Persiapan Supabase

Jalankan script SQL berikut di **Supabase SQL Editor** Anda untuk membuat tabel log `heartbeats`:

File tersedia di [`scripts/supabase_schema.sql`](scripts/supabase_schema.sql):

```sql
CREATE TABLE IF NOT EXISTS public.heartbeats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alias TEXT NOT NULL,
    source TEXT DEFAULT 'heartbeat-daemon',
    status TEXT DEFAULT 'alive',
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT TIMEZONE('utc', NOW()) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_heartbeats_alias ON public.heartbeats (alias);
CREATE INDEX IF NOT EXISTS idx_heartbeats_created_at ON public.heartbeats (created_at DESC);

ALTER TABLE public.heartbeats ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow insert heartbeats" ON public.heartbeats FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow read heartbeats" ON public.heartbeats FOR SELECT USING (true);
CREATE POLICY "Allow delete heartbeats" ON public.heartbeats FOR DELETE USING (true);
```

### 2. Persiapan Sertifikat Aiven Kafka (Jika Menggunakan mTLS)

Jika cluster Aiven Kafka Anda menggunakan **mTLS** (default Aiven):
1. Buka dashboard Aiven console -> Service Anda.
2. Download **CA Certificate** (`ca.pem`), **Access Certificate** (`service.cert`), dan **Access Key** (`service.key`).
3. Simpan file-file tersebut ke folder `./certs/`:
   - `./certs/aiven_ca.pem`
   - `./certs/aiven_service.cert`
   - `./certs/aiven_service.key`

*(Catatan: Anda juga bisa menyematkan string sertifikat langsung via environment variable `ca_cert_pem`, `client_cert_pem`, `client_key_pem`)*.

### 3. Konfigurasi `config.yaml`

Salin template konfigurasi:

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml` sesuai kebutuhan Anda:

```yaml
app:
  name: "HeartBeat"
  log_level: "info"
  server:
    enabled: true
    port: 8080

scheduler:
  default_interval: "6h"
  run_on_startup: true

supabase:
  - alias: "my-supabase-prod"
    enabled: true
    url: "https://abcdefghijklmn.supabase.co"
    api_key: "eyJhbGciOi..."
    table_name: "heartbeats"
    interval: "6h"
    cleanup:
      enabled: true
      retention_days: 7

kafka:
  - alias: "my-aiven-kafka"
    enabled: true
    brokers:
      - "kafka-xxxx.aivencloud.com:12345"
    topic: "heartbeat-ping"
    interval: "4h"
    security:
      protocol: "SSL"
      ssl:
        ca_cert_file: "./certs/aiven_ca.pem"
        client_cert_file: "./certs/aiven_service.cert"
        client_key_file: "./certs/aiven_service.key"
    produce:
      enabled: true
    consume:
      enabled: true
      group_id: "hb-consumer-group"
```

---

## 💻 Menjalankan Aplikasi

### Mode 1: Menjalankan Langsung (Go Binary)

```bash
# Build binary
make build

# Jalankan daemon
./bin/heartbeat -config config.yaml

# Atau langsung dengan go run:
make run
```

### Mode 2: Mode One-Shot (Sekali Jalan)

Sangat cocok jika ingin dijalankan via cron job Linux atau GitHub Actions:

```bash
./bin/heartbeat -config config.yaml -once
```

### Mode 3: Docker & Docker Compose

```bash
# Menggunakan Docker Compose (Background daemon)
docker compose up -d

# Cek logs
docker compose logs -f

# Hentikan
docker compose down
```

### Mode 4: Menggunakan Spring Cloud Config Server ([`scc2go`](https://github.com/KAnggara75/scc2go))

HeartBeat dapat mengambil konfigurasi terpusat secara dinamis dari Spring Cloud Config Server dengan menentukan `SCC_URL` (wajib disediakan via environment variable atau CLI flag jika tanpa file lokal):

```bash
# Set SCC_URL dan APP_AUTH_SECRET:
export SCC_URL="https://conflect.example.com/heartbeat/prd"
export APP_AUTH_SECRET="your-secret-token"

# Jalankan daemon:
./bin/heartbeat

# Atau jalankan one-shot mode:
./bin/heartbeat -once

# Atau tentukan via CLI flags:
./bin/heartbeat -scc-url "https://conflect.example.com/heartbeat/prd" \
                -scc-auth "Bearer your-secret-token"
```

---

## 📡 HTTP API Reference

Saat `app.server.enabled: true`, endpoint HTTP berikut aktif (default port `8080`):

| Endpoint | Method | Deskripsi |
| :--- | :---: | :--- |
| `/healthz` | `GET` | Health check probe sederhana (mengembalikan HTTP 200 `{"status":"ok"}`). |
| `/livez` | `GET` | Alias untuk `/healthz`. |
| `/status` | `GET` | Menampilkan data JSON status terakhir, latensi, dan pesan dari seluruh database & cluster. |
| `/trigger` | `POST` | Memicu eksekusi pemeriksaan heartbeat saat itu juga (asinkron). Tambahkan `?sync=true` untuk menunggu hasil. |

Contoh respon `/status`:

```json
{
  "last_run": "2026-09-16T08:45:00Z",
  "status": "running",
  "targets": {
    "supabase:my-supabase-prod": {
      "target_type": "supabase",
      "alias": "my-supabase-prod",
      "success": true,
      "latency": 45123890,
      "message": "Inserted heartbeat record into table 'heartbeats'",
      "timestamp": "2026-09-16T08:45:00.045Z"
    },
    "kafka:my-aiven-kafka": {
      "target_type": "kafka",
      "alias": "my-aiven-kafka",
      "success": true,
      "latency": 92345678,
      "message": "produced to topic 'heartbeat-ping' | consumed from group 'hb-consumer-group'",
      "timestamp": "2026-09-16T08:45:00.092Z"
    }
  }
}
```

---

## ⚙️ Ringkasan Parameter Konfigurasi

### Supabase Settings
| Field | Tipe | Default | Keterangan |
| :--- | :---: | :---: | :--- |
| `alias` | `string` | **Wajib** | Identifier unik database. |
| `enabled` | `bool` | `true` | Toggle aktif/nonaktif target ini. |
| `url` | `string` | **Wajib** | URL proyek Supabase (contoh: `https://xyz.supabase.co`). |
| `api_key` | `string` | **Wajib** | Anon key atau Service Role key. |
| `table_name` | `string` | `heartbeats` | Nama tabel tujuan insert. |
| `interval` | `string` | `6h` | Frekuensi ping (contoh: `2h`, `6h`, `12h`). |
| `cron` | `string` | - | Ekspresi cron 5-field (contoh: `0 */4 * * *`). |
| `cleanup.enabled` | `bool` | `false` | Hapus otomatis data lawas. |
| `cleanup.retention_days`| `int` | `7` | Hapus log yang lebih tua dari N hari. |

### Kafka Settings
| Field | Tipe | Default | Keterangan |
| :--- | :---: | :---: | :--- |
| `alias` | `string` | **Wajib** | Identifier unik cluster Kafka. |
| `enabled` | `bool` | `true` | Toggle aktif/nonaktif cluster ini. |
| `brokers` | `[]string`| **Wajib** | Daftar host broker (contoh: `host:port`). |
| `topic` | `string` | `heartbeat-ping`| Topic tujuan publish/consume. |
| `security.protocol` | `string` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_SSL`, atau `SASL_PLAINTEXT`. |
| `security.ssl.*` | `object` | - | Path file atau string PEM untuk `ca_cert`, `client_cert`, `client_key`. |
| `security.sasl.*` | `object` | - | `mechanism` (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`), `username`, `password`. |
| `produce.enabled` | `bool` | `true` | Kirim payload ping ke topic. |
| `consume.enabled` | `bool` | `true` | Baca pesan/verifikasi consumer group. |

---

## 🧪 Pengujian Unit

Jalankan rangkaian test otomatis:

```bash
make test
```

---

## 📄 Lisensi

Proyek ini dilisensikan di bawah [MIT License](LICENSE) - dibuat oleh **Call Vin ([@KAnggara75](https://github.com/KAnggara75))**.
