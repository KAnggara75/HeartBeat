package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KAnggara75/HeartBeat/internal/config"
	"github.com/KAnggara75/HeartBeat/internal/heartbeat"
)

func TestServerEndpoints(t *testing.T) {
	cfg := &config.Config{}
	mgr := heartbeat.NewManager(cfg)
	srv := NewServer(8080, mgr)

	// 1. Test Index Page (HTML Status Dashboard)
	t.Run("GET / returns HTML dashboard", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		srv.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", rec.Code)
		}
		contentType := rec.Header().Get("Content-Type")
		if !strings.Contains(contentType, "text/html") {
			t.Fatalf("expected Content-Type text/html, got %s", contentType)
		}
		body := rec.Body.String()
		if !strings.Contains(body, "HeartBeat Monitor") {
			t.Fatalf("expected body to contain 'HeartBeat Monitor', got: %s", body)
		}
		if !strings.Contains(body, "Trigger Heartbeat Now") {
			t.Fatalf("expected body to contain trigger button, got: %s", body)
		}
	})

	// 2. Test Healthz
	t.Run("GET /healthz returns status ok", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rec := httptest.NewRecorder()

		srv.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
			t.Fatalf("expected body to contain 'status: ok', got: %s", rec.Body.String())
		}
	})

	// 3. Test Status API
	t.Run("GET /status returns json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		rec := httptest.NewRecorder()

		srv.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), `"status":"running"`) {
			t.Fatalf("expected body to contain 'status: running', got: %s", rec.Body.String())
		}
	})
}
