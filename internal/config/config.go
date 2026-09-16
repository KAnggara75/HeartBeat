package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App       AppConfig        `yaml:"app"`
	Scheduler SchedulerConfig  `yaml:"scheduler"`
	Supabase  []SupabaseConfig `yaml:"supabase"`
	Kafka     []KafkaConfig    `yaml:"kafka"`
}

type AppConfig struct {
	Name      string       `yaml:"name"`
	LogLevel  string       `yaml:"log_level"`
	LogFormat string       `yaml:"log_format"` // text or json
	Server    ServerConfig `yaml:"server"`
}

type ServerConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

type SchedulerConfig struct {
	DefaultInterval string `yaml:"default_interval"`
	RunOnStartup    bool   `yaml:"run_on_startup"`
}

type SupabaseConfig struct {
	Alias        string                 `yaml:"alias"`
	Enabled      *bool                  `yaml:"enabled"`
	URL          string                 `yaml:"url"`
	ApiKey       string                 `yaml:"api_key"`
	TableName    string                 `yaml:"table_name"`
	Interval     string                 `yaml:"interval"`
	Cron         string                 `yaml:"cron"`
	Payload      map[string]interface{} `yaml:"payload"`
	Timeout      string                 `yaml:"timeout"`
	Cleanup      SupabaseCleanupConfig  `yaml:"cleanup"`
}

func (s *SupabaseConfig) IsEnabled() bool {
	if s.Enabled == nil {
		return true
	}
	return *s.Enabled
}

type SupabaseCleanupConfig struct {
	Enabled       bool `yaml:"enabled"`
	RetentionDays int  `yaml:"retention_days"`
}

type KafkaConfig struct {
	Alias    string              `yaml:"alias"`
	Enabled  *bool               `yaml:"enabled"`
	Brokers  []string            `yaml:"brokers"`
	Topic    string              `yaml:"topic"`
	Interval string              `yaml:"interval"`
	Cron     string              `yaml:"cron"`
	Security KafkaSecurityConfig `yaml:"security"`
	Produce  KafkaProduceConfig  `yaml:"produce"`
	Consume  KafkaConsumeConfig  `yaml:"consume"`
}

func (k *KafkaConfig) IsEnabled() bool {
	if k.Enabled == nil {
		return true
	}
	return *k.Enabled
}

type KafkaSecurityConfig struct {
	Protocol string          `yaml:"protocol"` // PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT
	SSL      KafkaSSLConfig  `yaml:"ssl"`
	SASL     KafkaSASLConfig `yaml:"sasl"`
}

type KafkaSSLConfig struct {
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
	CACertFile         string `yaml:"ca_cert_file"`
	CACertPEM          string `yaml:"ca_cert_pem"`
	ClientCertFile     string `yaml:"client_cert_file"`
	ClientCertPEM      string `yaml:"client_cert_pem"`
	ClientKeyFile      string `yaml:"client_key_file"`
	ClientKeyPEM       string `yaml:"client_key_pem"`
}

type KafkaSASLConfig struct {
	Mechanism string `yaml:"mechanism"` // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

type KafkaProduceConfig struct {
	Enabled *bool  `yaml:"enabled"`
	Key     string `yaml:"key"`
	Message string `yaml:"message"`
}

func (p *KafkaProduceConfig) IsEnabled() bool {
	if p.Enabled == nil {
		return true
	}
	return *p.Enabled
}

type KafkaConsumeConfig struct {
	Enabled     *bool  `yaml:"enabled"`
	GroupID     string `yaml:"group_id"`
	Timeout     string `yaml:"timeout"`
	MaxMessages int    `yaml:"max_messages"`
}

func (c *KafkaConsumeConfig) IsEnabled() bool {
	if c.Enabled == nil {
		return true
	}
	return *c.Enabled
}

// LoadConfig loads configuration from a YAML file with env expansion
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file %s: %w", path, err)
	}

	expandedData := expandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expandedData), &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML config: %w", err)
	}

	setDefaults(&cfg)
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func setDefaults(cfg *Config) {
	if cfg.App.Name == "" {
		cfg.App.Name = "HeartBeat"
	}
	if cfg.App.LogLevel == "" {
		cfg.App.LogLevel = "info"
	}
	if cfg.App.LogFormat == "" {
		cfg.App.LogFormat = "text"
	}
	if cfg.App.Server.Port == 0 {
		cfg.App.Server.Port = 8080
	}

	if cfg.Scheduler.DefaultInterval == "" {
		cfg.Scheduler.DefaultInterval = "6h"
	}

	for i := range cfg.Supabase {
		if cfg.Supabase[i].TableName == "" {
			cfg.Supabase[i].TableName = "heartbeats"
		}
		if cfg.Supabase[i].Timeout == "" {
			cfg.Supabase[i].Timeout = "15s"
		}
		if cfg.Supabase[i].Cleanup.RetentionDays <= 0 {
			cfg.Supabase[i].Cleanup.RetentionDays = 7
		}
	}

	for i := range cfg.Kafka {
		if cfg.Kafka[i].Topic == "" {
			cfg.Kafka[i].Topic = "heartbeat-ping"
		}
		if cfg.Kafka[i].Security.Protocol == "" {
			cfg.Kafka[i].Security.Protocol = "PLAINTEXT"
		}
		if cfg.Kafka[i].Consume.GroupID == "" {
			cfg.Kafka[i].Consume.GroupID = fmt.Sprintf("hb-%s-group", cfg.Kafka[i].Alias)
		}
		if cfg.Kafka[i].Consume.Timeout == "" {
			cfg.Kafka[i].Consume.Timeout = "10s"
		}
		if cfg.Kafka[i].Consume.MaxMessages <= 0 {
			cfg.Kafka[i].Consume.MaxMessages = 1
		}
	}
}

func validateConfig(cfg *Config) error {
	aliases := make(map[string]bool)

	for _, sb := range cfg.Supabase {
		if sb.Alias == "" {
			return fmt.Errorf("supabase entry missing 'alias'")
		}
		if aliases["sb:"+sb.Alias] {
			return fmt.Errorf("duplicate supabase alias: %s", sb.Alias)
		}
		aliases["sb:"+sb.Alias] = true

		if sb.IsEnabled() {
			if sb.URL == "" {
				return fmt.Errorf("supabase [%s] missing 'url'", sb.Alias)
			}
			if sb.ApiKey == "" {
				return fmt.Errorf("supabase [%s] missing 'api_key'", sb.Alias)
			}
		}
	}

	for _, kf := range cfg.Kafka {
		if kf.Alias == "" {
			return fmt.Errorf("kafka entry missing 'alias'")
		}
		if aliases["kf:"+kf.Alias] {
			return fmt.Errorf("duplicate kafka alias: %s", kf.Alias)
		}
		aliases["kf:"+kf.Alias] = true

		if kf.IsEnabled() {
			if len(kf.Brokers) == 0 {
				return fmt.Errorf("kafka [%s] must define at least one broker", kf.Alias)
			}
		}
	}

	return nil
}

var envRegex = regexp.MustCompile(`\$\{([^}:]+)(?::-([^}]*))?\}`)

// expandEnv replaces ${VAR} or ${VAR:-default} with OS environment variable values
func expandEnv(content string) string {
	return envRegex.ReplaceAllStringFunc(content, func(m string) string {
		sub := envRegex.FindStringSubmatch(m)
		if len(sub) > 1 {
			varName := sub[1]
			val, found := os.LookupEnv(varName)
			if found && val != "" {
				return val
			}
			if len(sub) > 2 {
				return sub[2] // default value
			}
		}
		return ""
	})
}

// ParseDuration parses duration string with fallback
func ParseDuration(d string, fallback time.Duration) time.Duration {
	if d == "" {
		return fallback
	}
	val, err := time.ParseDuration(d)
	if err != nil {
		return fallback
	}
	return val
}
