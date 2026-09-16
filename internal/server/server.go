package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/heartbeat"
)

type Server struct {
	httpServer *http.Server
	manager    *heartbeat.Manager
	port       int
}

func NewServer(port int, manager *heartbeat.Manager) *Server {
	s := &Server{
		manager: manager,
		port:    port,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/livez", s.handleHealthz)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/trigger", s.handleTrigger)

	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return s
}

func (s *Server) Start() error {
	slog.Info("Starting HTTP health & status server", slog.Int("port", s.port))
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	results, lastRun := s.manager.GetStatus()
	allHealthy := true
	total := len(results)
	healthyCount := 0
	for _, res := range results {
		if res.Success {
			healthyCount++
		} else {
			allHealthy = false
		}
	}

	overallStatus := "Healthy"
	badgeColor := "#10b981" // emerald
	if total == 0 {
		overallStatus = "Initializing"
		badgeColor = "#6b7280" // gray
	} else if !allHealthy {
		overallStatus = "Degraded"
		badgeColor = "#ef4444" // red
	}

	lastRunFormatted := "Never"
	if !lastRun.IsZero() {
		lastRunFormatted = lastRun.Format("2006-01-02 15:04:05 MST")
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HeartBeat Status Monitor</title>
    <style>
        :root {
            --bg: #0f172a;
            --card-bg: #1e293b;
            --border: #334155;
            --text-main: #f8fafc;
            --text-sub: #94a3b8;
            --accent: #38bdf8;
            --success: #10b981;
            --danger: #ef4444;
            --badge-bg: #0f172a;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background-color: var(--bg);
            color: var(--text-main);
            padding: 2rem 1rem;
            display: flex;
            justify-content: center;
        }
        .container {
            width: 100%%;
            max-width: 900px;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border);
            padding-bottom: 1.5rem;
            margin-bottom: 2rem;
            flex-wrap: wrap;
            gap: 1rem;
        }
        .title-group {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        .heart-icon {
            color: #ec4899;
            animation: pulse 1.5s infinite;
            display: inline-block;
        }
        @keyframes pulse {
            0%% { transform: scale(1); }
            50%% { transform: scale(1.15); }
            100%% { transform: scale(1); }
        }
        h1 { font-size: 1.75rem; font-weight: 700; letter-spacing: -0.025em; }
        .badge {
            font-size: 0.85rem;
            font-weight: 600;
            padding: 0.35rem 0.85rem;
            border-radius: 9999px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }
        .card {
            background-color: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 0.75rem;
            padding: 1.25rem;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
        }
        .card-label { font-size: 0.8rem; color: var(--text-sub); text-transform: uppercase; margin-bottom: 0.5rem; }
        .card-value { font-size: 1.25rem; font-weight: 600; }
        .targets-section h2 {
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .table-wrap {
            overflow-x: auto;
            border: 1px solid var(--border);
            border-radius: 0.75rem;
            background-color: var(--card-bg);
        }
        table {
            width: 100%%;
            border-collapse: collapse;
            text-align: left;
            font-size: 0.9rem;
        }
        th, td {
            padding: 0.85rem 1.15rem;
            border-bottom: 1px solid var(--border);
        }
        th {
            background-color: rgba(15, 23, 42, 0.6);
            color: var(--text-sub);
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.05em;
        }
        tr:last-child td { border-bottom: none; }
        tr:hover td { background-color: rgba(255, 255, 255, 0.02); }
        .status-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            font-weight: 600;
            font-size: 0.8rem;
        }
        .dot {
            width: 8px;
            height: 8px;
            border-radius: 50%%;
        }
        .dot-healthy { background-color: var(--success); box-shadow: 0 0 8px var(--success); }
        .dot-failed { background-color: var(--danger); box-shadow: 0 0 8px var(--danger); }
        .actions {
            margin-top: 1.5rem;
            display: flex;
            justify-content: flex-end;
            gap: 0.75rem;
        }
        button {
            background: linear-gradient(135deg, #0284c7 0%%, #0369a1 100%%);
            color: white;
            border: none;
            padding: 0.6rem 1.25rem;
            border-radius: 0.5rem;
            font-weight: 600;
            font-size: 0.875rem;
            cursor: pointer;
            transition: opacity 0.2s;
        }
        button:hover { opacity: 0.9; }
        button:disabled { opacity: 0.5; cursor: not-allowed; }
        .footer {
            margin-top: 2rem;
            text-align: center;
            font-size: 0.8rem;
            color: var(--text-sub);
        }
    </style>
    <script>
        function triggerHeartbeat() {
            const btn = document.getElementById("trigger-btn");
            btn.disabled = true;
            btn.innerText = "Triggering...";
            fetch("/trigger?sync=true", { method: "POST" })
                .then(() => { setTimeout(() => window.location.reload(), 1000); })
                .catch(err => {
                    alert("Trigger failed: " + err);
                    btn.disabled = false;
                    btn.innerText = "Trigger Heartbeat Now";
                });
        }
        // Auto refresh page every 15 seconds
        setTimeout(() => window.location.reload(), 15000);
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="title-group">
                <span class="heart-icon">💓</span>
                <h1>HeartBeat Monitor</h1>
            </div>
            <div>
                <span class="badge" style="background-color: %s20; color: %s; border: 1px solid %s;">
                    %s
                </span>
            </div>
        </div>

        <div class="summary-grid">
            <div class="card">
                <div class="card-label">Last Ping Time</div>
                <div class="card-value" style="font-size: 1rem;">%s</div>
            </div>
            <div class="card">
                <div class="card-label">Healthy Targets</div>
                <div class="card-value">%d / %d</div>
            </div>
            <div class="card">
                <div class="card-label">Auto Refresh</div>
                <div class="card-value" style="font-size: 1rem; color: var(--accent);">Every 15s</div>
            </div>
        </div>

        <div class="targets-section">
            <h2>Configured Targets</h2>
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>Target</th>
                            <th>Type</th>
                            <th>Status</th>
                            <th>Latency</th>
                            <th>Message / Info</th>
                            <th>Checked At</th>
                        </tr>
                    </thead>
                    <tbody>`,
		badgeColor, badgeColor, badgeColor, overallStatus,
		lastRunFormatted, healthyCount, total,
	)

	if len(results) == 0 {
		fmt.Fprintf(w, `<tr><td colspan="6" style="text-align: center; color: var(--text-sub); padding: 2rem;">No heartbeat cycles recorded yet. (Check startup or trigger manually)</td></tr>`)
	} else {
		for _, res := range results {
			statusPill := `<span class="status-pill"><span class="dot dot-healthy"></span> Healthy</span>`
			if !res.Success {
				statusPill = `<span class="status-pill"><span class="dot dot-failed"></span> Failed</span>`
			}
			msg := res.Message
			if msg == "" {
				msg = "-"
			}
			timeStr := res.Timestamp.Format("15:04:05 MST")

			fmt.Fprintf(w, `
                <tr>
                    <td style="font-weight: 600;">%s</td>
                    <td><span style="font-family: monospace; font-size: 0.8rem; background: rgba(255,255,255,0.05); padding: 0.2rem 0.5rem; border-radius: 4px;">%s</span></td>
                    <td>%s</td>
                    <td style="color: var(--accent);">%v</td>
                    <td style="max-width: 250px; word-break: break-word;">%s</td>
                    <td style="color: var(--text-sub);">%s</td>
                </tr>`,
				res.Alias, res.TargetType, statusPill, res.Latency.Round(time.Millisecond), msg, timeStr,
			)
		}
	}

	fmt.Fprintf(w, `
                    </tbody>
                </table>
            </div>
        </div>

        <div class="actions">
            <button id="trigger-btn" onclick="triggerHeartbeat()">Trigger Heartbeat Now</button>
        </div>

        <div class="footer">
            HeartBeat Service • Continuous Keep-Alive Daemon
        </div>
    </div>
</body>
</html>`)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	results, lastRun := s.manager.GetStatus()
	w.Header().Set("Content-Type", "application/json")

	allHealthy := true
	for _, res := range results {
		if !res.Success {
			allHealthy = false
			break
		}
	}

	statusCode := http.StatusOK
	if len(results) > 0 && !allHealthy {
		statusCode = http.StatusMultiStatus
	}

	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "running",
		"last_run": lastRun.UTC().Format(time.RFC3339),
		"targets":  results,
	})
}

func (s *Server) handleTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	isSync := r.URL.Query().Get("sync") == "true"
	if isSync {
		results := s.manager.RunAll(r.Context())
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(results)
		return
	}

	go s.manager.RunAll(context.Background())

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"message": "Heartbeat execution triggered in background",
	})
}
