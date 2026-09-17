package heartbeat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type pgxConnExecutor interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
}

type pgxConnCloser interface {
	pgxConnExecutor
	Close(ctx context.Context) error
}

type SupabaseService struct {
	client       *http.Client
	pgxConnector func(ctx context.Context, connStr string) (pgxConnCloser, error)
}

func defaultPgxConnector(ctx context.Context, connStr string) (pgxConnCloser, error) {
	return pgx.Connect(ctx, connStr)
}

func NewSupabaseService() *SupabaseService {
	return &SupabaseService{
		client:       &http.Client{},
		pgxConnector: defaultPgxConnector,
	}
}

type HeartbeatResult struct {
	TargetType string        `json:"target_type"` // "supabase" or "kafka"
	Alias      string        `json:"alias"`
	Success    bool          `json:"success"`
	Latency    time.Duration `json:"latency"`
	Message    string        `json:"message"`
	Timestamp  time.Time     `json:"timestamp"`
}

// Ping performs the insert heartbeat and optional cleanup for a Supabase database (via REST API or direct PostgreSQL)
func (s *SupabaseService) Ping(ctx context.Context, cfg *config.SupabaseConfig) *HeartbeatResult {
	start := time.Now()

	target := cfg.Host
	if target == "" {
		target = cfg.URL
	}

	if strings.HasPrefix(target, "postgres://") || strings.HasPrefix(target, "postgresql://") {
		return s.pingPostgres(ctx, cfg, target)
	}

	timeout := config.ParseDuration(cfg.Timeout, 15*time.Second)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	baseURL := strings.TrimRight(cfg.URL, "/")
	endpoint := fmt.Sprintf("%s/rest/v1/%s", baseURL, url.PathEscape(cfg.TableName))

	// Construct payload
	payload := make(map[string]interface{})
	for k, v := range cfg.Payload {
		payload[k] = v
	}
	payload["alias"] = cfg.Alias
	payload["status"] = "alive"
	if _, ok := payload["source"]; !ok {
		payload["source"] = "heartbeat-daemon"
	}
	payload["created_at"] = time.Now().UTC().Format(time.RFC3339)

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return &HeartbeatResult{
			TargetType: "supabase",
			Alias:      cfg.Alias,
			Success:    false,
			Latency:    time.Since(start),
			Message:    fmt.Sprintf("failed to serialize payload: %v", err),
			Timestamp:  time.Now(),
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return &HeartbeatResult{
			TargetType: "supabase",
			Alias:      cfg.Alias,
			Success:    false,
			Latency:    time.Since(start),
			Message:    fmt.Sprintf("failed to create HTTP request: %v", err),
			Timestamp:  time.Now(),
		}
	}

	req.Header.Set("apikey", cfg.ApiKey)
	req.Header.Set("Authorization", "Bearer "+cfg.ApiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "resolution=merge-duplicates,return=minimal")

	resp, err := s.client.Do(req)
	if err != nil {
		return &HeartbeatResult{
			TargetType: "supabase",
			Alias:      cfg.Alias,
			Success:    false,
			Latency:    time.Since(start),
			Message:    fmt.Sprintf("request error: %v", err),
			Timestamp:  time.Now(),
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		slog.Debug("Supabase record already exists, conflict merged", slog.String("alias", cfg.Alias))
	} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return &HeartbeatResult{
			TargetType: "supabase",
			Alias:      cfg.Alias,
			Success:    false,
			Latency:    time.Since(start),
			Message:    fmt.Sprintf("unexpected HTTP %d: %s", resp.StatusCode, string(respBody)),
			Timestamp:  time.Now(),
		}
	}

	slog.Info("Supabase heartbeat insert succeeded",
		slog.String("alias", cfg.Alias),
		slog.String("table", cfg.TableName),
		slog.Duration("latency", time.Since(start)),
	)

	// Perform optional cleanup if enabled
	if cfg.Cleanup.Enabled && cfg.Cleanup.RetentionDays > 0 {
		s.cleanupOldRecords(ctx, cfg, baseURL)
	}

	return &HeartbeatResult{
		TargetType: "supabase",
		Alias:      cfg.Alias,
		Success:    true,
		Latency:    time.Since(start),
		Message:    fmt.Sprintf("Inserted heartbeat record into table '%s'", cfg.TableName),
		Timestamp:  time.Now(),
	}
}

func (s *SupabaseService) pingPostgres(ctx context.Context, cfg *config.SupabaseConfig, connStr string) *HeartbeatResult {
	start := time.Now()
	timeout := config.ParseDuration(cfg.Timeout, 15*time.Second)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	connector := s.pgxConnector
	if connector == nil {
		connector = defaultPgxConnector
	}
	conn, err := connector(ctx, connStr)
	if err != nil {
		return &HeartbeatResult{
			TargetType: "supabase",
			Alias:      cfg.Alias,
			Success:    false,
			Latency:    time.Since(start),
			Message:    fmt.Sprintf("PostgreSQL connection error: %v", err),
			Timestamp:  time.Now(),
		}
	}
	defer conn.Close(ctx)

	tableName := cfg.TableName
	if tableName == "" {
		tableName = "heartbeats"
	}

	// 1. Ensure table and schema exist
	if err := s.ensureTableSchema(ctx, conn, tableName); err != nil {
		slog.Warn("Notice checking/creating supabase schema", slog.String("alias", cfg.Alias), slog.Any("error", err))
	}

	// 2. Insert heartbeat
	insertSQL := fmt.Sprintf(`INSERT INTO public.%s (alias, source, status, notes, created_at)
		VALUES ($1, $2, $3, $4, NOW());`, tableName)
	_, err = conn.Exec(ctx, insertSQL, cfg.Alias, "heartbeat-daemon", "alive", "Automated keep-alive ping")
	if err != nil {
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "duplicate key") || strings.Contains(errStr, "unique constraint") || strings.Contains(errStr, "already exists") {
			slog.Debug("Record already exists in table, updating timestamp", slog.String("alias", cfg.Alias))
			updateSQL := fmt.Sprintf(`UPDATE public.%s SET status = $2, source = $3, notes = $4, created_at = NOW() WHERE alias = $1;`, tableName)
			_, _ = conn.Exec(ctx, updateSQL, cfg.Alias, "alive", "heartbeat-daemon", "Automated keep-alive ping")
		} else {
			return &HeartbeatResult{
				TargetType: "supabase",
				Alias:      cfg.Alias,
				Success:    false,
				Latency:    time.Since(start),
				Message:    fmt.Sprintf("PostgreSQL insert error: %v", err),
				Timestamp:  time.Now(),
			}
		}
	}

	// 3. Cleanup old records if enabled (> 24 hours / retention duration)
	if cfg.Cleanup.Enabled {
		retentionDur := cfg.Cleanup.RetentionDuration()
		seconds := int(retentionDur.Seconds())
		if seconds > 0 {
			delSQL := fmt.Sprintf(`DELETE FROM public.%s WHERE alias = $1 AND created_at < NOW() - make_interval(secs => %d);`,
				tableName, seconds)
			_, _ = conn.Exec(ctx, delSQL, cfg.Alias)
		}
	}

	slog.Info("PostgreSQL heartbeat insert succeeded",
		slog.String("alias", cfg.Alias),
		slog.String("table", tableName),
		slog.Duration("latency", time.Since(start)),
	)

	return &HeartbeatResult{
		TargetType: "supabase",
		Alias:      cfg.Alias,
		Success:    true,
		Latency:    time.Since(start),
		Message:    fmt.Sprintf("Inserted heartbeat record into PostgreSQL table '%s'", tableName),
		Timestamp:  time.Now(),
	}
}

func (s *SupabaseService) cleanupOldRecords(ctx context.Context, cfg *config.SupabaseConfig, baseURL string) {
	retentionDur := cfg.Cleanup.RetentionDuration()
	cutoff := time.Now().UTC().Add(-retentionDur).Format(time.RFC3339)
	delURL := fmt.Sprintf("%s/rest/v1/%s?alias=eq.%s&created_at=lt.%s",
		baseURL,
		url.PathEscape(cfg.TableName),
		url.QueryEscape(cfg.Alias),
		url.QueryEscape(cutoff),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, delURL, nil)
	if err != nil {
		slog.Warn("Failed to create cleanup request", slog.String("alias", cfg.Alias), slog.Any("error", err))
		return
	}

	req.Header.Set("apikey", cfg.ApiKey)
	req.Header.Set("Authorization", "Bearer "+cfg.ApiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		slog.Warn("Cleanup request failed", slog.String("alias", cfg.Alias), slog.Any("error", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		slog.Debug("Supabase cleanup completed",
			slog.String("alias", cfg.Alias),
			slog.String("table", cfg.TableName),
			slog.Int("retention_days", cfg.Cleanup.RetentionDays),
		)
	} else {
		body, _ := io.ReadAll(resp.Body)
		slog.Warn("Supabase cleanup returned non-2xx status",
			slog.String("alias", cfg.Alias),
			slog.Int("status", resp.StatusCode),
			slog.String("response", string(body)),
		)
	}
}

func (s *SupabaseService) ensureTableSchema(ctx context.Context, conn pgxConnExecutor, tableName string) error {
	var exists bool
	checkSQL := `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_name = $1
	);`
	err := conn.QueryRow(ctx, checkSQL, tableName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("checking table existence: %w", err)
	}

	if exists {
		return nil
	}

	slog.Info("Heartbeat table does not exist, applying schema from scripts/supabase_schema.sql",
		slog.String("table", tableName),
	)

	schemaSQL := defaultSupabaseSchemaSQL
	if data, err := os.ReadFile("scripts/supabase_schema.sql"); err == nil && len(data) > 0 {
		schemaSQL = string(data)
	}

	_, err = conn.Exec(ctx, schemaSQL)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil
		}
		return fmt.Errorf("executing schema initialization script: %w", err)
	}

	slog.Info("Successfully initialized Supabase schema and policies", slog.String("table", tableName))
	return nil
}

const defaultSupabaseSchemaSQL = `
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

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'heartbeats' AND policyname = 'Allow insert heartbeats'
    ) THEN
        CREATE POLICY "Allow insert heartbeats" ON public.heartbeats FOR INSERT WITH CHECK (true);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'heartbeats' AND policyname = 'Allow read heartbeats'
    ) THEN
        CREATE POLICY "Allow read heartbeats" ON public.heartbeats FOR SELECT USING (true);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE tablename = 'heartbeats' AND policyname = 'Allow delete heartbeats'
    ) THEN
        CREATE POLICY "Allow delete heartbeats" ON public.heartbeats FOR DELETE USING (true);
    END IF;
END
$$;
`

