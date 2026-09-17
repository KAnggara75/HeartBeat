package heartbeat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KAnggara75/HeartBeat/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

	// Also test postgres:// prefix
	cfg2 := &config.SupabaseConfig{
		Alias:   "test-pg-fail2",
		URL:     "postgres://invaliduser:invalidpass@127.0.0.1:59997/invalid_db",
		Timeout: "100ms",
	}
	result2 := service.Ping(context.Background(), cfg2)
	if result2.Success {
		t.Fatalf("expected failure connecting to invalid postgres://, got success")
	}
}

func TestSupabasePingPayloadMarshalError(t *testing.T) {
	service := NewSupabaseService()
	cfg := &config.SupabaseConfig{
		Alias: "test-bad-payload",
		URL:   "http://localhost:1234",
		Payload: map[string]interface{}{
			"bad": make(chan int), // channel cannot be JSON marshaled
		},
	}

	result := service.Ping(context.Background(), cfg)
	if result.Success {
		t.Fatalf("expected failure when payload cannot be serialized, got success")
	}
}

func TestSupabasePingInvalidURL(t *testing.T) {
	service := NewSupabaseService()
	cfg := &config.SupabaseConfig{
		Alias: "test-bad-url",
		URL:   "://invalid-url",
	}

	result := service.Ping(context.Background(), cfg)
	if result.Success {
		t.Fatalf("expected failure for invalid URL, got success")
	}
}

func TestSupabaseCleanupNetworkError(t *testing.T) {
	// Start a server that accepts POST but closes connection or shuts down before cleanup
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	serverURL := server.URL

	service := NewSupabaseService()
	cfg := &config.SupabaseConfig{
		Alias:     "test-cleanup-net-err",
		URL:       serverURL,
		ApiKey:    "key",
		TableName: "heartbeats",
		Cleanup: config.SupabaseCleanupConfig{
			Enabled:       true,
			RetentionDays: 1,
		},
	}

	// Close the server immediately after creating it so cleanup request fails with network error
	server.Close()
	service.cleanupOldRecords(context.Background(), cfg, serverURL)
}

// mockRow implements pgx.Row
type mockRow struct {
	scanFunc func(dest ...any) error
}

func (m *mockRow) Scan(dest ...any) error {
	if m.scanFunc != nil {
		return m.scanFunc(dest...)
	}
	return nil
}

// mockPgxConn implements pgxConnCloser
type mockPgxConn struct {
	queryRowFunc func(ctx context.Context, sql string, args ...any) pgx.Row
	execFunc     func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	closeFunc    func(ctx context.Context) error
}

func (m *mockPgxConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if m.queryRowFunc != nil {
		return m.queryRowFunc(ctx, sql, args...)
	}
	return &mockRow{}
}

func (m *mockPgxConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if m.execFunc != nil {
		return m.execFunc(ctx, sql, arguments...)
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (m *mockPgxConn) Close(ctx context.Context) error {
	if m.closeFunc != nil {
		return m.closeFunc(ctx)
	}
	return nil
}

func TestEnsureTableSchema_Branches(t *testing.T) {
	service := NewSupabaseService()
	ctx := context.Background()

	// 1. Table already exists
	t.Run("Table already exists", func(t *testing.T) {
		conn := &mockPgxConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{
					scanFunc: func(dest ...any) error {
						if len(dest) > 0 {
							if p, ok := dest[0].(*bool); ok {
								*p = true
							}
						}
						return nil
					},
				}
			},
		}
		err := service.ensureTableSchema(ctx, conn, "heartbeats")
		if err != nil {
			t.Errorf("expected nil error when table exists, got: %v", err)
		}
	})

	// 2. Query error checking existence
	t.Run("Error checking existence", func(t *testing.T) {
		conn := &mockPgxConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{
					scanFunc: func(dest ...any) error {
						return fmt.Errorf("connection terminated")
					},
				}
			},
		}
		err := service.ensureTableSchema(ctx, conn, "heartbeats")
		if err == nil {
			t.Errorf("expected error when query fails, got nil")
		}
	})

	// 3. Table does not exist -> creates schema successfully
	t.Run("Table does not exist - creates successfully", func(t *testing.T) {
		execCalled := false
		conn := &mockPgxConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{
					scanFunc: func(dest ...any) error {
						if len(dest) > 0 {
							if p, ok := dest[0].(*bool); ok {
								*p = false
							}
						}
						return nil
					},
				}
			},
			execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
				execCalled = true
				return pgconn.NewCommandTag("CREATE TABLE"), nil
			},
		}
		err := service.ensureTableSchema(ctx, conn, "heartbeats")
		if err != nil {
			t.Errorf("expected nil error on successful schema creation, got: %v", err)
		}
		if !execCalled {
			t.Errorf("expected Exec to be called to create schema")
		}
	})

	// 4. Table does not exist -> exec returns "already exists" error
	t.Run("Schema exec returns already exists", func(t *testing.T) {
		conn := &mockPgxConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{
					scanFunc: func(dest ...any) error {
						if len(dest) > 0 {
							if p, ok := dest[0].(*bool); ok {
								*p = false
							}
						}
						return nil
					},
				}
			},
			execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, fmt.Errorf("relation already exists")
			},
		}
		err := service.ensureTableSchema(ctx, conn, "heartbeats")
		if err != nil {
			t.Errorf("expected nil error when already exists error occurs, got: %v", err)
		}
	})

	// 5. Table does not exist -> exec returns other error
	t.Run("Schema exec returns critical error", func(t *testing.T) {
		conn := &mockPgxConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{
					scanFunc: func(dest ...any) error {
						if len(dest) > 0 {
							if p, ok := dest[0].(*bool); ok {
								*p = false
							}
						}
						return nil
					},
				}
			},
			execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, fmt.Errorf("syntax error near WHERE")
			},
		}
		err := service.ensureTableSchema(ctx, conn, "heartbeats")
		if err == nil {
			t.Errorf("expected error on exec failure, got nil")
		}
	})
}

func TestPingPostgres_Mocked(t *testing.T) {
	ctx := context.Background()

	// 1. Success with Cleanup
	t.Run("PingPostgres success with cleanup", func(t *testing.T) {
		var executedSQLs []string
		mockConn := &mockPgxConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{
					scanFunc: func(dest ...any) error {
						if len(dest) > 0 {
							if p, ok := dest[0].(*bool); ok {
								*p = true
							}
						}
						return nil
					},
				}
			},
			execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
				executedSQLs = append(executedSQLs, sql)
				return pgconn.NewCommandTag("INSERT 0 1"), nil
			},
		}

		svc := &SupabaseService{
			client: &http.Client{},
			pgxConnector: func(ctx context.Context, connStr string) (pgxConnCloser, error) {
				return mockConn, nil
			},
		}

		cfg := &config.SupabaseConfig{
			Alias:     "sb-pg-mock",
			Host:      "postgres://mock:mock@localhost:5432/db",
			TableName: "heartbeats",
			Cleanup: config.SupabaseCleanupConfig{
				Enabled:       true,
				RetentionDays: 2,
			},
		}

		res := svc.Ping(ctx, cfg)
		if !res.Success {
			t.Fatalf("expected success, got: %s", res.Message)
		}
		if len(executedSQLs) != 2 {
			t.Errorf("expected 2 SQL queries (INSERT and DELETE cleanup), got %d: %v", len(executedSQLs), executedSQLs)
		}
	})

	// 2. Insert error: duplicate key triggers UPDATE branch
	t.Run("PingPostgres duplicate key updates existing record", func(t *testing.T) {
		firstInsert := true
		updateCalled := false
		mockConn := &mockPgxConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{
					scanFunc: func(dest ...any) error {
						if len(dest) > 0 {
							if p, ok := dest[0].(*bool); ok {
								*p = true
							}
						}
						return nil
					},
				}
			},
			execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
				if firstInsert {
					firstInsert = false
					return pgconn.CommandTag{}, fmt.Errorf("duplicate key value violates unique constraint")
				}
				updateCalled = true
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
		}

		svc := &SupabaseService{
			client: &http.Client{},
			pgxConnector: func(ctx context.Context, connStr string) (pgxConnCloser, error) {
				return mockConn, nil
			},
		}

		cfg := &config.SupabaseConfig{
			Alias: "sb-duplicate",
			Host:  "postgres://mock:mock@localhost:5432/db",
		}

		res := svc.Ping(ctx, cfg)
		if !res.Success {
			t.Fatalf("expected success on duplicate key fallback, got: %s", res.Message)
		}
		if !updateCalled {
			t.Errorf("expected UPDATE query to be executed after duplicate key error")
		}
	})

	// 3. Insert error: unhandled database error
	t.Run("PingPostgres unhandled insert error", func(t *testing.T) {
		mockConn := &mockPgxConn{
			queryRowFunc: func(ctx context.Context, sql string, args ...any) pgx.Row {
				return &mockRow{
					scanFunc: func(dest ...any) error {
						if len(dest) > 0 {
							if p, ok := dest[0].(*bool); ok {
								*p = true
							}
						}
						return nil
					},
				}
			},
			execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, fmt.Errorf("permission denied for table heartbeats")
			},
		}

		svc := &SupabaseService{
			client: &http.Client{},
			pgxConnector: func(ctx context.Context, connStr string) (pgxConnCloser, error) {
				return mockConn, nil
			},
		}

		cfg := &config.SupabaseConfig{
			Alias: "sb-perm-err",
			Host:  "postgres://mock:mock@localhost:5432/db",
		}

		res := svc.Ping(ctx, cfg)
		if res.Success {
			t.Fatalf("expected failure on permission denied, got success")
		}
	})
}
