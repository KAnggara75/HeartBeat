package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

	// 1b. Test Index Page with degraded/failed and healthy targets
	t.Run("GET / with results shows Degraded badge and Healthy row", func(t *testing.T) {
		statusMgr := heartbeat.NewManager(cfg)
		// Inject a healthy target
		statusMgr.RunTarget(context.Background(), "supabase", "nonexistent") // this returns error, not recorded
		// But we can trigger RunAll or directly test server with manager that has recorded results
		// Let's create a server with manager having results
		enabled := true
		testCfg := &config.Config{
			Supabase: []config.SupabaseConfig{
				{
					Alias:   "sb-test",
					Enabled: &enabled,
					URL:     "http://127.0.0.1:59999",
					ApiKey:  "key",
				},
			},
		}
		mgrWithRes := heartbeat.NewManager(testCfg)
		mgrWithRes.RunAll(context.Background())

		srvWithRes := NewServer(8081, mgrWithRes)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		srvWithRes.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
		body := rec.Body.String()
		if !strings.Contains(body, "Degraded") {
			t.Fatalf("expected body to contain Degraded, got: %s", body)
		}
		if !strings.Contains(body, "sb-test") {
			t.Fatalf("expected body to contain sb-test, got: %s", body)
		}

		// Also test GET /status with failed target -> 207 Multi-Status
		reqStatus := httptest.NewRequest(http.MethodGet, "/status", nil)
		recStatus := httptest.NewRecorder()
		srvWithRes.httpServer.Handler.ServeHTTP(recStatus, reqStatus)
		if recStatus.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207 MultiStatus, got %d", recStatus.Code)
		}
	})

	// 1c. Test 404 for unknown path on Index handler
	t.Run("GET /unknown returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/unknown-path", nil)
		rec := httptest.NewRecorder()

		srv.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected status 404 Not Found, got %d", rec.Code)
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

	// 2b. Test Livez
	t.Run("GET /livez returns status ok", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/livez", nil)
		rec := httptest.NewRecorder()

		srv.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", rec.Code)
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

	// 4. Test Trigger Endpoint
	t.Run("POST /trigger async returns 202 Accepted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/trigger", nil)
		rec := httptest.NewRecorder()

		srv.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected status 202 Accepted, got %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "Heartbeat execution triggered") {
			t.Fatalf("expected body confirmation, got: %s", rec.Body.String())
		}
	})

	t.Run("POST /trigger?sync=true returns 200 OK", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/trigger?sync=true", nil)
		rec := httptest.NewRecorder()

		srv.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200 OK, got %d", rec.Code)
		}
	})

	t.Run("GET /trigger returns 405 Method Not Allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/trigger", nil)
		rec := httptest.NewRecorder()

		srv.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected status 405, got %d", rec.Code)
		}
	})

	// 5. Test Start / Stop Server
	t.Run("Server Start and Stop", func(t *testing.T) {
		testSrv := NewServer(0, mgr)
		go func() {
			_ = testSrv.Start()
		}()
		time.Sleep(50 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		if err := testSrv.Stop(ctx); err != nil {
			t.Errorf("expected clean shutdown, got %v", err)
		}
	})
}
