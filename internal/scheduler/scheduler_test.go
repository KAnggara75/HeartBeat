package scheduler

import (
	"testing"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/config"
	"github.com/KAnggara75/HeartBeat/internal/heartbeat"
)

func TestDetermineScheduleSpec(t *testing.T) {
	tests := []struct {
		name            string
		cronExpr        string
		intervalStr     string
		defaultInterval string
		want            string
	}{
		{
			name:     "5-field cron expression converts to 6-field with 0 prefix",
			cronExpr: "0 * * * *",
			want:     "0 0 * * * *",
		},
		{
			name:     "6-field cron expression preserved",
			cronExpr: "30 0 * * * *",
			want:     "30 0 * * * *",
		},
		{
			name:        "interval string used when cron is empty",
			cronExpr:    "",
			intervalStr: "10m",
			want:        "@every 10m0s",
		},
		{
			name:            "default interval used when both cron and interval empty",
			cronExpr:        "",
			intervalStr:     "",
			defaultInterval: "15m",
			want:            "@every 15m0s",
		},
		{
			name:            "invalid duration falls back to 6h",
			cronExpr:        "",
			intervalStr:     "invalid-duration",
			defaultInterval: "also-invalid",
			want:            "@every 6h0m0s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := determineScheduleSpec(tt.cronExpr, tt.intervalStr, tt.defaultInterval)
			if got != tt.want {
				t.Errorf("determineScheduleSpec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScheduler_StartStop(t *testing.T) {
	enabled := true
	disabled := false
	cfg := &config.Config{
		Scheduler: config.SchedulerConfig{
			DefaultInterval: "1s",
			RunOnStartup:    true,
		},
		Supabase: []config.SupabaseConfig{
			{
				Alias:    "sb1",
				Enabled:  &enabled,
				Interval: "1s",
			},
			{
				Alias:   "sb-disabled",
				Enabled: &disabled,
			},
		},
		Kafka: []config.KafkaConfig{
			{
				Alias:    "kf1",
				Enabled:  &enabled,
				Cron:     "*/5 * * * * *", // 6 fields valid
			},
			{
				Alias:   "kf-disabled",
				Enabled: &disabled,
			},
		},
	}

	mgr := heartbeat.NewManager(cfg)
	sched := NewScheduler(cfg, mgr)

	err := sched.Start()
	if err != nil {
		t.Fatalf("sched.Start() failed: %v", err)
	}

	// Give a short sleep for the startup goroutine
	time.Sleep(50 * time.Millisecond)

	sched.Stop()
}

func TestScheduler_InvalidSpec(t *testing.T) {
	enabled := true
	t.Run("invalid supabase cron", func(t *testing.T) {
		cfg := &config.Config{
			Supabase: []config.SupabaseConfig{
				{
					Alias:   "bad-sb",
					Enabled: &enabled,
					Cron:    "invalid cron format with lots of parts that cron parser rejects",
				},
			},
		}
		mgr := heartbeat.NewManager(cfg)
		sched := NewScheduler(cfg, mgr)
		if err := sched.Start(); err == nil {
			t.Errorf("expected error for bad supabase cron, got nil")
			sched.Stop()
		}
	})

	t.Run("invalid kafka cron", func(t *testing.T) {
		cfg := &config.Config{
			Kafka: []config.KafkaConfig{
				{
					Alias:   "bad-kf",
					Enabled: &enabled,
					Cron:    "invalid cron format with lots of parts that cron parser rejects",
				},
			},
		}
		mgr := heartbeat.NewManager(cfg)
		sched := NewScheduler(cfg, mgr)
		if err := sched.Start(); err == nil {
			t.Errorf("expected error for bad kafka cron, got nil")
			sched.Stop()
		}
	})
}
