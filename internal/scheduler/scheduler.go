package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/config"
	"github.com/KAnggara75/HeartBeat/internal/heartbeat"
	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	cron    *cron.Cron
	manager *heartbeat.Manager
	cfg     *config.Config
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewScheduler(cfg *config.Config, manager *heartbeat.Manager) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		cron:    cron.New(cron.WithSeconds()),
		manager: manager,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start registers all jobs and begins scheduled execution
func (s *Scheduler) Start() error {
	defaultInterval := s.cfg.Scheduler.DefaultInterval
	if defaultInterval == "" {
		defaultInterval = "5m"
	}

	// Schedule Supabase instances
	for _, sb := range s.cfg.Supabase {
		if !sb.IsEnabled() {
			continue
		}
		alias := sb.Alias
		spec := determineScheduleSpec(sb.Cron, sb.Interval, defaultInterval)

		_, err := s.cron.AddFunc(spec, func() {
			slog.Info("Running scheduled heartbeat for Supabase", slog.String("alias", alias))
			s.manager.RunTarget(s.ctx, "supabase", alias)
		})
		if err != nil {
			return fmt.Errorf("invalid schedule spec '%s' for supabase [%s]: %w", spec, alias, err)
		}
		slog.Info("Registered Supabase heartbeat schedule", slog.String("alias", alias), slog.String("schedule", spec))
	}

	// Schedule Kafka instances
	for _, kf := range s.cfg.Kafka {
		if !kf.IsEnabled() {
			continue
		}
		alias := kf.Alias
		spec := determineScheduleSpec(kf.Cron, kf.Interval, defaultInterval)

		_, err := s.cron.AddFunc(spec, func() {
			slog.Info("Running scheduled heartbeat for Kafka", slog.String("alias", alias))
			s.manager.RunTarget(s.ctx, "kafka", alias)
		})
		if err != nil {
			return fmt.Errorf("invalid schedule spec '%s' for kafka [%s]: %w", spec, alias, err)
		}
		slog.Info("Registered Kafka heartbeat schedule", slog.String("alias", alias), slog.String("schedule", spec))
	}

	s.cron.Start()

	// Initial run if configured
	if s.cfg.Scheduler.RunOnStartup {
		go func() {
			slog.Info("Running initial startup heartbeats...")
			// Give a moment for network / server startup
			time.Sleep(1 * time.Second)
			s.manager.RunAll(s.ctx)
		}()
	}

	return nil
}

// Stop stops the scheduler and waits for in-flight jobs
func (s *Scheduler) Stop() {
	s.cancel()
	ctx := s.cron.Stop()
	<-ctx.Done()
	slog.Info("Scheduler stopped gracefully")
}

func determineScheduleSpec(cronExpr, intervalStr, defaultInterval string) string {
	if cronExpr != "" {
		// If standard 5-field cron expression, prefix with '0' for second-precision cron
		fields := strings.Fields(cronExpr)
		if len(fields) == 5 {
			return "0 " + cronExpr
		}
		return cronExpr
	}

	useInterval := intervalStr
	if useInterval == "" {
		useInterval = defaultInterval
	}

	// Validate duration
	d, err := time.ParseDuration(useInterval)
	if err != nil {
		d = 6 * time.Hour
	}

	return fmt.Sprintf("@every %s", d.String())
}
