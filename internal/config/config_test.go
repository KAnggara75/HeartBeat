package config

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
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

func TestLoadConfigFromSCC(t *testing.T) {
	sccResponse := `{
		"name": "heartbeat",
		"profiles": ["default"],
		"propertySources": [
			{
				"name": "test-source",
				"source": {
					"app.name": "scc-heartbeat",
					"app.log_level": "debug",
					"app.server.port": 8888,
					"supabase[0].alias": "scc-supabase",
					"supabase[0].url": "https://scc.supabase.co",
					"supabase[0].api_key": "scc-key",
					"supabase[0].table_name": "heartbeats",
					"kafka[0].alias": "scc-kafka",
					"kafka[0].brokers[0]": "scc-kafka:9092"
				}
			}
		]
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sccResponse))
	}))
	defer ts.Close()

	viper.Reset()
	cfg, err := LoadConfigWithSCC("", SCCParams{
		URL: ts.URL,
	})
	if err != nil {
		t.Fatalf("LoadConfigWithSCC failed: %v", err)
	}

	if cfg.App.Name != "scc-heartbeat" {
		t.Errorf("expected App.Name 'scc-heartbeat', got '%s'", cfg.App.Name)
	}
	if cfg.App.Server.Port != 8888 {
		t.Errorf("expected Server.Port 8888, got %d", cfg.App.Server.Port)
	}
	if len(cfg.Supabase) != 1 || cfg.Supabase[0].Alias != "scc-supabase" {
		t.Fatalf("expected supabase alias 'scc-supabase', got %+v", cfg.Supabase)
	}
	if len(cfg.Kafka) != 1 || cfg.Kafka[0].Alias != "scc-kafka" {
		t.Fatalf("expected kafka alias 'scc-kafka', got %+v", cfg.Kafka)
	}
}

func TestLoadConfigMissingSCCURL(t *testing.T) {
	// Ensure SCC_URL is cleared
	t.Setenv("SCC_URL", "")

	_, err := LoadConfigWithSCC("non_existent_config.yaml", SCCParams{})
	if err == nil {
		t.Fatal("expected error when SCC_URL is empty and local file does not exist, got nil")
	}
}

func TestResolveDefaultInterval(t *testing.T) {
	t.Setenv("HEALH_INTERVAL", "")
	t.Setenv("HEALTH_INTERVAL", "")
	if got := resolveDefaultInterval(); got != "5m" {
		t.Errorf("expected default 5m, got %s", got)
	}

	t.Setenv("HEALH_INTERVAL", "10m")
	if got := resolveDefaultInterval(); got != "10m" {
		t.Errorf("expected 10m from HEALH_INTERVAL, got %s", got)
	}

	t.Setenv("HEALH_INTERVAL", "")
	t.Setenv("HEALTH_INTERVAL", "2m")
	if got := resolveDefaultInterval(); got != "2m" {
		t.Errorf("expected 2m from HEALTH_INTERVAL, got %s", got)
	}
}

func TestRetentionDuration(t *testing.T) {
	c1 := SupabaseCleanupConfig{Retention: "12h"}
	if c1.RetentionDuration() != 12*time.Hour {
		t.Errorf("expected 12h, got %v", c1.RetentionDuration())
	}

	c2 := SupabaseCleanupConfig{RetentionHours: 48}
	if c2.RetentionDuration() != 48*time.Hour {
		t.Errorf("expected 48h, got %v", c2.RetentionDuration())
	}

	c3 := SupabaseCleanupConfig{RetentionDays: 3}
	if c3.RetentionDuration() != 72*time.Hour {
		t.Errorf("expected 72h, got %v", c3.RetentionDuration())
	}

	cDefault := SupabaseCleanupConfig{}
	if cDefault.RetentionDuration() != 24*time.Hour {
		t.Errorf("expected 24h default, got %v", cDefault.RetentionDuration())
	}
}

func TestIsEnabledHelpers(t *testing.T) {
	bTrue := true
	bFalse := false

	// KafkaConfig
	kc := KafkaConfig{}
	if !kc.IsEnabled() {
		t.Errorf("expected nil enabled to default to true")
	}
	kc.Enabled = &bFalse
	if kc.IsEnabled() {
		t.Errorf("expected false")
	}

	// KafkaProduceConfig
	pc := KafkaProduceConfig{}
	if !pc.IsEnabled() {
		t.Errorf("expected nil produce enabled to default to true")
	}
	pc.Enabled = &bFalse
	if pc.IsEnabled() {
		t.Errorf("expected false")
	}

	// KafkaConsumeConfig
	cc := KafkaConsumeConfig{}
	if !cc.IsEnabled() {
		t.Errorf("expected nil consume enabled to default to true")
	}
	cc.Enabled = &bTrue
	if !cc.IsEnabled() {
		t.Errorf("expected true")
	}
}

func TestParseDuration(t *testing.T) {
	fallback := 10 * time.Minute
	if got := ParseDuration("", fallback); got != fallback {
		t.Errorf("expected fallback %v, got %v", fallback, got)
	}
	if got := ParseDuration("invalid-duration", fallback); got != fallback {
		t.Errorf("expected fallback %v on error, got %v", fallback, got)
	}
	if got := ParseDuration("15s", fallback); got != 15*time.Second {
		t.Errorf("expected 15s, got %v", got)
	}
}


