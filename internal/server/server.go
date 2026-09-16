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
	slog.Info("Starting HTTP health server", slog.Int("port", s.port))
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
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
