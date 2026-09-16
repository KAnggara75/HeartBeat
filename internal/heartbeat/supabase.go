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
	"strings"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/config"
)

type SupabaseService struct {
	client *http.Client
}

func NewSupabaseService() *SupabaseService {
	return &SupabaseService{
		client: &http.Client{},
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

// Ping performs the insert heartbeat and optional cleanup for a Supabase database
func (s *SupabaseService) Ping(ctx context.Context, cfg *config.SupabaseConfig) *HeartbeatResult {
	start := time.Now()
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
	req.Header.Set("Prefer", "return=minimal")

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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
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

func (s *SupabaseService) cleanupOldRecords(ctx context.Context, cfg *config.SupabaseConfig, baseURL string) {
	cutoff := time.Now().UTC().AddDate(0, 0, -cfg.Cleanup.RetentionDays).Format(time.RFC3339)
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
