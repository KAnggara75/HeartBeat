package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/config"
	"github.com/KAnggara75/HeartBeat/internal/heartbeat"
	"github.com/KAnggara75/HeartBeat/internal/logger"
	"github.com/KAnggara75/HeartBeat/internal/scheduler"
	"github.com/KAnggara75/HeartBeat/internal/server"
)

var (
	Version   = "1.0.0"
	BuildDate = "2026-09-16"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to YAML configuration file")
	shortConfig := flag.String("c", "", "Path to YAML configuration file (shorthand)")
	runOnce := flag.Bool("once", false, "Run heartbeats once and exit (useful for cron/CI)")
	showVersion := flag.Bool("version", false, "Print version and exit")
	shortVersion := flag.Bool("v", false, "Print version (shorthand)")
	sccURL := flag.String("scc-url", os.Getenv("SCC_URL"), "Spring Cloud Config Server URL (or via SCC_URL env)")
	sccAuth := flag.String("scc-auth", os.Getenv("SCC_AUTH"), "Authorization header for Spring Cloud Config (or via SCC_AUTH env)")
	sccInsecure := flag.Bool("scc-insecure", os.Getenv("SCC_INSECURE") == "true" || os.Getenv("SCC_DISABLE_TLS") == "true", "Disable TLS verification for Spring Cloud Config")
	flag.Parse()

	if *showVersion || *shortVersion {
		fmt.Printf("HeartBeat v%s (built %s)\n", Version, BuildDate)
		os.Exit(0)
	}

	cfgFile := *configPath
	if *shortConfig != "" {
		cfgFile = *shortConfig
	}

	cfg, err := config.LoadConfigWithSCC(cfgFile, config.ResolveSCCParams(config.SCCParams{
		URL:        *sccURL,
		Auth:       *sccAuth,
		DisableTLS: *sccInsecure,
	}))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	isJSON := cfg.App.LogFormat == "json"
	l := logger.InitLogger(cfg.App.LogLevel, isJSON)
	l.Info("Starting HeartBeat service",
		slog.String("version", Version),
		slog.String("name", cfg.App.Name),
		slog.Int("supabase_instances", len(cfg.Supabase)),
		slog.Int("kafka_clusters", len(cfg.Kafka)),
	)

	manager := heartbeat.NewManager(cfg)

	// Mode 1: Run once and exit
	if *runOnce {
		l.Info("Running in one-shot mode (--once)")
		results := manager.RunAll(context.Background())
		hasFailed := false
		for _, r := range results {
			if !r.Success {
				hasFailed = true
				break
			}
		}
		if hasFailed {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Mode 2: Background Daemon
	sched := scheduler.NewScheduler(cfg, manager)
	if err := sched.Start(); err != nil {
		l.Error("Failed to start scheduler", slog.Any("error", err))
		os.Exit(1)
	}

	// Optional HTTP Health & Control Server
	var srv *server.Server
	if cfg.App.Server.Enabled {
		srv = server.NewServer(cfg.App.Server.Port, manager)
		go func() {
			if err := srv.Start(); err != nil {
				l.Error("HTTP server error", slog.Any("error", err))
			}
		}()
	}

	// Graceful Shutdown Handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	l.Info("Shutdown signal received", slog.String("signal", sig.String()))

	sched.Stop()

	if srv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Stop(shutdownCtx); err != nil {
			l.Warn("HTTP server forced to shutdown", slog.Any("error", err))
		}
	}

	l.Info("HeartBeat service stopped gracefully")
}
