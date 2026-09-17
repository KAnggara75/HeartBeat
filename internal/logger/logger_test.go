package logger

import (
	"log/slog"
	"testing"
)

func TestInitLogger(t *testing.T) {
	tests := []struct {
		name       string
		levelStr   string
		jsonFormat bool
		wantLevel  slog.Level
	}{
		{
			name:       "debug level text",
			levelStr:   "debug",
			jsonFormat: false,
			wantLevel:  slog.LevelDebug,
		},
		{
			name:       "warn level text",
			levelStr:   "warn",
			jsonFormat: false,
			wantLevel:  slog.LevelWarn,
		},
		{
			name:       "warning level json",
			levelStr:   "warning",
			jsonFormat: true,
			wantLevel:  slog.LevelWarn,
		},
		{
			name:       "error level json",
			levelStr:   "error",
			jsonFormat: true,
			wantLevel:  slog.LevelError,
		},
		{
			name:       "default info level text",
			levelStr:   "unknown",
			jsonFormat: false,
			wantLevel:  slog.LevelInfo,
		},
		{
			name:       "info level uppercase with spaces",
			levelStr:   " INFO ",
			jsonFormat: false,
			wantLevel:  slog.LevelInfo,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := InitLogger(tt.levelStr, tt.jsonFormat)
			if l == nil {
				t.Fatalf("InitLogger returned nil")
			}
			if !l.Enabled(nil, tt.wantLevel) {
				t.Errorf("expected level %v to be enabled", tt.wantLevel)
			}
		})
	}
}
