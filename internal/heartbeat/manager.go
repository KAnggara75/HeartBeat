package heartbeat

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/config"
)

type Manager struct {
	cfg             *config.Config
	supabaseService *SupabaseService
	kafkaService    *KafkaService

	mu          sync.RWMutex
	lastResults map[string]*HeartbeatResult
	lastRun     time.Time
}

func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		cfg:             cfg,
		supabaseService: NewSupabaseService(),
		kafkaService:    NewKafkaService(),
		lastResults:     make(map[string]*HeartbeatResult),
	}
}

// RunAll executes heartbeats across all configured Supabase and Kafka instances concurrently
func (m *Manager) RunAll(ctx context.Context) []*HeartbeatResult {
	m.mu.Lock()
	m.lastRun = time.Now()
	m.mu.Unlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []*HeartbeatResult

	// 1. Supabase Heartbeats
	for _, sb := range m.cfg.Supabase {
		if !sb.IsEnabled() {
			continue
		}
		sbCfg := sb
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := m.supabaseService.Ping(ctx, &sbCfg)
			mu.Lock()
			results = append(results, res)
			mu.Unlock()
			m.recordResult(res)
		}()
	}

	// 2. Kafka Heartbeats
	for _, kf := range m.cfg.Kafka {
		if !kf.IsEnabled() {
			continue
		}
		kfCfg := kf
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := m.kafkaService.Ping(ctx, &kfCfg)
			mu.Lock()
			results = append(results, res)
			mu.Unlock()
			m.recordResult(res)
		}()
	}

	wg.Wait()
	m.logSummary(results)
	return results
}

// RunTarget runs a single target by type and alias
func (m *Manager) RunTarget(ctx context.Context, targetType, alias string) (*HeartbeatResult, error) {
	switch targetType {
	case "supabase":
		for _, sb := range m.cfg.Supabase {
			if sb.Alias == alias {
				res := m.supabaseService.Ping(ctx, &sb)
				m.recordResult(res)
				return res, nil
			}
		}
		return nil, fmt.Errorf("supabase alias not found: %s", alias)

	case "kafka":
		for _, kf := range m.cfg.Kafka {
			if kf.Alias == alias {
				res := m.kafkaService.Ping(ctx, &kf)
				m.recordResult(res)
				return res, nil
			}
		}
		return nil, fmt.Errorf("kafka alias not found: %s", alias)

	default:
		return nil, fmt.Errorf("unknown target type: %s", targetType)
	}
}

func (m *Manager) recordResult(res *HeartbeatResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%s", res.TargetType, res.Alias)
	m.lastResults[key] = res
}

func (m *Manager) GetStatus() (map[string]*HeartbeatResult, time.Time) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	copied := make(map[string]*HeartbeatResult, len(m.lastResults))
	for k, v := range m.lastResults {
		copied[k] = v
	}
	return copied, m.lastRun
}

func (m *Manager) logSummary(results []*HeartbeatResult) {
	successCount := 0
	failedCount := 0

	for _, res := range results {
		if res.Success {
			successCount++
		} else {
			failedCount++
			slog.Error("Heartbeat target failed",
				slog.String("type", res.TargetType),
				slog.String("alias", res.Alias),
				slog.String("error", res.Message),
				slog.Duration("latency", res.Latency),
			)
		}
	}

	slog.Info("Heartbeat execution round completed",
		slog.Int("total", len(results)),
		slog.Int("success", successCount),
		slog.Int("failed", failedCount),
	)
}
