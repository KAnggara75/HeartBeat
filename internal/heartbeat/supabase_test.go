package heartbeat

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KAnggara75/HeartBeat/internal/config"
)

func TestSupabasePingSuccess(t *testing.T) {
	var receivedApiKey string
	var receivedAuth string
	var receivedBody map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedApiKey = r.Header.Get("apikey")
		receivedAuth = r.Header.Get("Authorization")

		if r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&receivedBody)
			w.WriteHeader(http.StatusCreated)
			return
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	service := NewSupabaseService()
	cfg := &config.SupabaseConfig{
		Alias:     "test-supabase",
		URL:       server.URL,
		ApiKey:    "secret-api-key-123",
		TableName: "heartbeats",
		Cleanup: config.SupabaseCleanupConfig{
			Enabled:       true,
			RetentionDays: 7,
		},
	}

	result := service.Ping(context.Background(), cfg)

	if !result.Success {
		t.Fatalf("expected success, got error: %s", result.Message)
	}
	if receivedApiKey != "secret-api-key-123" {
		t.Errorf("expected apikey header 'secret-api-key-123', got '%s'", receivedApiKey)
	}
	if receivedAuth != "Bearer secret-api-key-123" {
		t.Errorf("expected Authorization header 'Bearer secret-api-key-123', got '%s'", receivedAuth)
	}
	if receivedBody["alias"] != "test-supabase" {
		t.Errorf("expected payload alias 'test-supabase', got '%v'", receivedBody["alias"])
	}
	if receivedBody["status"] != "alive" {
		t.Errorf("expected payload status 'alive', got '%v'", receivedBody["status"])
	}
}

func TestSupabasePingFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error": "permission denied"}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	service := NewSupabaseService()
	cfg := &config.SupabaseConfig{
		Alias:     "test-failing",
		URL:       server.URL,
		ApiKey:    "invalid-key",
		TableName: "heartbeats",
	}

	result := service.Ping(context.Background(), cfg)

	if result.Success {
		t.Fatalf("expected failure, got success: %s", result.Message)
	}
}

func TestSupabasePingConflictAndCleanupWarning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			// Return Conflict 409
			w.WriteHeader(http.StatusConflict)
			return
		}
		if r.Method == http.MethodDelete {
			// Return 500 on delete to trigger warning branch
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("cleanup error"))
			return
		}
	}))
	defer server.Close()

	service := NewSupabaseService()
	cfg := &config.SupabaseConfig{
		Alias:     "test-conflict",
		URL:       server.URL,
		ApiKey:    "key",
		TableName: "heartbeats",
		Cleanup: config.SupabaseCleanupConfig{
			Enabled:       true,
			RetentionDays: 1,
		},
	}

	result := service.Ping(context.Background(), cfg)
	if !result.Success {
		t.Fatalf("expected success on 409 conflict, got: %s", result.Message)
	}
}

func TestSupabasePingPostgresFailure(t *testing.T) {
	service := NewSupabaseService()
	cfg := &config.SupabaseConfig{
		Alias:   "test-pg-fail",
		Host:    "postgresql://invaliduser:invalidpass@127.0.0.1:59997/invalid_db",
		Timeout: "100ms",
	}

	result := service.Ping(context.Background(), cfg)
	if result.Success {
		t.Fatalf("expected failure connecting to invalid postgres, got success")
	}
}
