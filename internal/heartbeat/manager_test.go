package heartbeat

import (
	"context"
	"testing"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/config"
)

func TestManagerLifecycleAndTargets(t *testing.T) {
	enabled := true
	disabled := false
	cfg := &config.Config{
		Supabase: []config.SupabaseConfig{
			{
				Alias:   "sb-active",
				Enabled: &enabled,
				URL:     "http://127.0.0.1:59999", // Unreachable port for fast network fail
				ApiKey:  "test-key",
			},
			{
				Alias:   "sb-disabled",
				Enabled: &disabled,
			},
		},
		Kafka: []config.KafkaConfig{
			{
				Alias:   "kf-active",
				Enabled: &enabled,
				Brokers: []string{"127.0.0.1:59998"},
				Topic:   "hb-topic",
				Produce: config.KafkaProduceConfig{Enabled: &enabled},
			},
			{
				Alias:   "kf-disabled",
				Enabled: &disabled,
			},
		},
	}

	mgr := NewManager(cfg)

	// Test GetStatus initial state
	status, lastRun := mgr.GetStatus()
	if len(status) != 0 {
		t.Errorf("expected 0 initial status results, got %d", len(status))
	}
	if !lastRun.IsZero() {
		t.Errorf("expected zero lastRun time initially")
	}

	// Test recordResult directly
	res := &HeartbeatResult{
		TargetType: "supabase",
		Alias:      "test-target",
		Success:    true,
		Latency:    10 * time.Millisecond,
		Message:    "OK",
		Timestamp:  time.Now(),
	}
	mgr.recordResult(res)

	status, _ = mgr.GetStatus()
	if len(status) != 1 || status["supabase:test-target"] == nil {
		t.Errorf("expected recorded result in status map")
	}

	// Test RunTarget supabase alias not found
	_, err := mgr.RunTarget(context.Background(), "supabase", "nonexistent")
	if err == nil {
		t.Errorf("expected error for nonexistent supabase alias")
	}

	// Test RunTarget kafka alias not found
	_, err = mgr.RunTarget(context.Background(), "kafka", "nonexistent")
	if err == nil {
		t.Errorf("expected error for nonexistent kafka alias")
	}

	// Test RunTarget unknown target type
	_, err = mgr.RunTarget(context.Background(), "redis", "something")
	if err == nil {
		t.Errorf("expected error for unknown target type")
	}

	// Test RunTarget for configured supabase
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	resSb, err := mgr.RunTarget(ctx, "supabase", "sb-active")
	if err != nil {
		t.Errorf("unexpected error invoking RunTarget supabase: %v", err)
	}
	if resSb == nil {
		t.Fatalf("expected non-nil result from RunTarget supabase")
	}

	// Test RunTarget for configured kafka
	resKf, err := mgr.RunTarget(ctx, "kafka", "kf-active")
	if err != nil {
		t.Errorf("unexpected error invoking RunTarget kafka: %v", err)
	}
	if resKf == nil {
		t.Fatalf("expected non-nil result from RunTarget kafka")
	}

	// Test RunAll (which also exercises logSummary)
	allResults := mgr.RunAll(ctx)
	if len(allResults) != 2 {
		t.Errorf("expected 2 active targets in RunAll, got %d", len(allResults))
	}

	_, lastRunAfter := mgr.GetStatus()
	if lastRunAfter.IsZero() {
		t.Errorf("expected non-zero lastRun after RunAll")
	}
}
