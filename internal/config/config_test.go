package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	yamlContent := `
app:
  name: "test-heartbeat"
  log_level: "debug"
  log_format: "json"
  server:
    enabled: true
    port: 9090

scheduler:
  default_interval: "2h"
  run_on_startup: true

supabase:
  - alias: "db-alpha"
    url: "https://${TEST_SB_HOST:-default.supabase.co}"
    api_key: "test-key"
    table_name: "custom_heartbeats"
    cleanup:
      enabled: true
      retention_days: 14

kafka:
  - alias: "kafka-alpha"
    brokers:
      - "localhost:9092"
    topic: "test-topic"
    security:
      protocol: "PLAINTEXT"
`
	tmpDir := t.TempDir()
	cfgFile := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(cfgFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	cfg, err := LoadConfig(cfgFile)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.App.Name != "test-heartbeat" {
		t.Errorf("expected App.Name test-heartbeat, got %s", cfg.App.Name)
	}
	if cfg.App.Server.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.App.Server.Port)
	}
	if len(cfg.Supabase) != 1 {
		t.Fatalf("expected 1 supabase config, got %d", len(cfg.Supabase))
	}
	if cfg.Supabase[0].URL != "https://default.supabase.co" {
		t.Errorf("expected URL https://default.supabase.co, got %s", cfg.Supabase[0].URL)
	}
	if cfg.Supabase[0].Cleanup.RetentionDays != 14 {
		t.Errorf("expected RetentionDays 14, got %d", cfg.Supabase[0].Cleanup.RetentionDays)
	}
	if len(cfg.Kafka) != 1 {
		t.Fatalf("expected 1 kafka config, got %d", len(cfg.Kafka))
	}
	if cfg.Kafka[0].Alias != "kafka-alpha" {
		t.Errorf("expected kafka alias kafka-alpha, got %s", cfg.Kafka[0].Alias)
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr bool
	}{
		{
			name: "missing supabase url",
			yaml: `
supabase:
  - alias: "sb1"
    api_key: "abc"
`,
			wantErr: true,
		},
		{
			name: "duplicate kafka alias",
			yaml: `
kafka:
  - alias: "k1"
    brokers: ["b1"]
  - alias: "k1"
    brokers: ["b2"]
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			f := filepath.Join(tmpDir, "cfg.yaml")
			_ = os.WriteFile(f, []byte(tt.yaml), 0644)

			_, err := LoadConfig(f)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
