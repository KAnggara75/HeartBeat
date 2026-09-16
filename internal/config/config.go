package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/KAnggara75/scc2go"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

type Config struct {
	App       AppConfig        `json:"app" yaml:"app" mapstructure:"app"`
	Scheduler SchedulerConfig  `json:"scheduler" yaml:"scheduler" mapstructure:"scheduler"`
	Supabase  []SupabaseConfig `json:"supabase" yaml:"supabase" mapstructure:"supabase"`
	Kafka     []KafkaConfig    `json:"kafka" yaml:"kafka" mapstructure:"kafka"`
}

type AppConfig struct {
	Name      string       `json:"name" yaml:"name" mapstructure:"name"`
	LogLevel  string       `json:"log_level" yaml:"log_level" mapstructure:"log_level"`
	LogFormat string       `json:"log_format" yaml:"log_format" mapstructure:"log_format"` // text or json
	Server    ServerConfig `json:"server" yaml:"server" mapstructure:"server"`
}

type ServerConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Port    int  `json:"port" yaml:"port" mapstructure:"port"`
}

type SchedulerConfig struct {
	DefaultInterval string `json:"default_interval" yaml:"default_interval" mapstructure:"default_interval"`
	RunOnStartup    bool   `json:"run_on_startup" yaml:"run_on_startup" mapstructure:"run_on_startup"`
}

type SupabaseConfig struct {
	Alias     string                 `json:"alias" yaml:"alias" mapstructure:"alias"`
	Enabled   *bool                  `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Host      string                 `json:"host" yaml:"host" mapstructure:"host"` // PostgreSQL connection string
	URL       string                 `json:"url" yaml:"url" mapstructure:"url"`
	ApiKey    string                 `json:"api_key" yaml:"api_key" mapstructure:"api_key"`
	TableName string                 `json:"table_name" yaml:"table_name" mapstructure:"table_name"`
	Interval  string                 `json:"interval" yaml:"interval" mapstructure:"interval"`
	Cron      string                 `json:"cron" yaml:"cron" mapstructure:"cron"`
	Payload   map[string]interface{} `json:"payload" yaml:"payload" mapstructure:"payload"`
	Timeout   string                 `json:"timeout" yaml:"timeout" mapstructure:"timeout"`
	Cleanup   SupabaseCleanupConfig  `json:"cleanup" yaml:"cleanup" mapstructure:"cleanup"`
}

func (s *SupabaseConfig) IsEnabled() bool {
	if s.Enabled == nil {
		return true
	}
	return *s.Enabled
}

type SupabaseCleanupConfig struct {
	Enabled        bool   `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Retention      string `json:"retention" yaml:"retention" mapstructure:"retention"` // e.g. "24h"
	RetentionHours int    `json:"retention_hours" yaml:"retention_hours" mapstructure:"retention_hours"`
	RetentionDays  int    `json:"retention_days" yaml:"retention_days" mapstructure:"retention_days"`
}

func (c *SupabaseCleanupConfig) RetentionDuration() time.Duration {
	if c.Retention != "" {
		if d, err := time.ParseDuration(c.Retention); err == nil && d > 0 {
			return d
		}
	}
	if c.RetentionHours > 0 {
		return time.Duration(c.RetentionHours) * time.Hour
	}
	if c.RetentionDays > 0 {
		return time.Duration(c.RetentionDays) * 24 * time.Hour
	}
	return 24 * time.Hour
}

type KafkaConfig struct {
	Alias    string              `json:"alias" yaml:"alias" mapstructure:"alias"`
	Enabled  *bool               `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Brokers  []string            `json:"brokers" yaml:"brokers" mapstructure:"brokers"`
	Topic    string              `json:"topic" yaml:"topic" mapstructure:"topic"`
	Interval string              `json:"interval" yaml:"interval" mapstructure:"interval"`
	Cron     string              `json:"cron" yaml:"cron" mapstructure:"cron"`
	Security KafkaSecurityConfig `json:"security" yaml:"security" mapstructure:"security"`
	Produce  KafkaProduceConfig  `json:"produce" yaml:"produce" mapstructure:"produce"`
	Consume  KafkaConsumeConfig  `json:"consume" yaml:"consume" mapstructure:"consume"`
}

func (k *KafkaConfig) IsEnabled() bool {
	if k.Enabled == nil {
		return true
	}
	return *k.Enabled
}

type KafkaSecurityConfig struct {
	Protocol string          `json:"protocol" yaml:"protocol" mapstructure:"protocol"` // PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT
	SSL      KafkaSSLConfig  `json:"ssl" yaml:"ssl" mapstructure:"ssl"`
	SASL     KafkaSASLConfig `json:"sasl" yaml:"sasl" mapstructure:"sasl"`
}

type KafkaSSLConfig struct {
	InsecureSkipVerify bool   `json:"insecure_skip_verify" yaml:"insecure_skip_verify" mapstructure:"insecure_skip_verify"`
	CACertFile         string `json:"ca_cert_file" yaml:"ca_cert_file" mapstructure:"ca_cert_file"`
	CACertPEM          string `json:"ca_cert_pem" yaml:"ca_cert_pem" mapstructure:"ca_cert_pem"`
	ClientCertFile     string `json:"client_cert_file" yaml:"client_cert_file" mapstructure:"client_cert_file"`
	ClientCertPEM      string `json:"client_cert_pem" yaml:"client_cert_pem" mapstructure:"client_cert_pem"`
	ClientKeyFile      string `json:"client_key_file" yaml:"client_key_file" mapstructure:"client_key_file"`
	ClientKeyPEM       string `json:"client_key_pem" yaml:"client_key_pem" mapstructure:"client_key_pem"`
}

type KafkaSASLConfig struct {
	Mechanism string `json:"mechanism" yaml:"mechanism" mapstructure:"mechanism"` // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	Username  string `json:"username" yaml:"username" mapstructure:"username"`
	Password  string `json:"password" yaml:"password" mapstructure:"password"`
}

type KafkaProduceConfig struct {
	Enabled *bool  `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Key     string `json:"key" yaml:"key" mapstructure:"key"`
	Message string `json:"message" yaml:"message" mapstructure:"message"`
}

func (p *KafkaProduceConfig) IsEnabled() bool {
	if p.Enabled == nil {
		return true
	}
	return *p.Enabled
}

type KafkaConsumeConfig struct {
	Enabled     *bool  `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	GroupID     string `json:"group_id" yaml:"group_id" mapstructure:"group_id"`
	Timeout     string `json:"timeout" yaml:"timeout" mapstructure:"timeout"`
	MaxMessages int    `json:"max_messages" yaml:"max_messages" mapstructure:"max_messages"`
}

func (c *KafkaConsumeConfig) IsEnabled() bool {
	if c.Enabled == nil {
		return true
	}
	return *c.Enabled
}

// SCCParams defines Spring Cloud Config parameters for scc2go
type SCCParams struct {
	URL        string
	Auth       string
	DisableTLS bool
}

// ResolveSCCParams resolves SCC URL and Auth with environment fallback
func ResolveSCCParams(params SCCParams) SCCParams {
	if params.URL == "" {
		params.URL = os.Getenv("SCC_URL")
	}

	if params.Auth == "" {
		params.Auth = os.Getenv("SCC_AUTH")
	}
	if params.Auth == "" {
		secret := os.Getenv("APP_AUTH_SECRET")
		if secret != "" {
			if strings.HasPrefix(secret, "Bearer ") || strings.HasPrefix(secret, "Basic ") {
				params.Auth = secret
			} else {
				params.Auth = "Bearer " + secret
			}
		}
	}

	if !params.DisableTLS {
		params.DisableTLS = os.Getenv("SCC_DISABLE_TLS") == "true" || os.Getenv("SCC_INSECURE") == "true"
	}

	return params
}

// LoadConfig loads configuration from local YAML file if path exists, or falls back to Spring Cloud Config
func LoadConfig(path string) (*Config, error) {
	scc := ResolveSCCParams(SCCParams{})
	// If path exists on disk and no explicit SCC_URL is set, prioritize local file
	if path != "" && os.Getenv("SCC_URL") == "" {
		if _, err := os.Stat(path); err == nil {
			scc.URL = ""
		}
	}
	return LoadConfigWithSCC(path, scc)
}

// LoadConfigWithSCC loads configuration using scc2go if SCC URL is provided, otherwise reads YAML file
func LoadConfigWithSCC(path string, scc SCCParams) (*Config, error) {
	var cfg Config

	hasLocalFile := false
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			hasLocalFile = true
		}
	}

	if !hasLocalFile && scc.URL == "" {
		return nil, fmt.Errorf("SCC_URL is required: Spring Cloud Config URL must be specified via --scc-url or SCC_URL environment variable when local config file is not found (%s)", path)
	}

	// 1. If Spring Cloud Config URL is provided, attempt scc2go fetch
	if scc.URL != "" {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		scc2go.GetEnv(scc.URL, scc.Auth, scc.DisableTLS)

		normalized := normalizeConfigMap(viper.AllSettings())
		rawBytes, err := json.Marshal(normalized)
		if err == nil {
			_ = json.Unmarshal(rawBytes, &cfg)
		}

		// Also extract custom Spring Cloud Config structures: db.<alias> and kafka.<alias>
		extractSCCCustomProperties(&cfg)
	}

	// 2. If a local YAML path is provided and exists, load and merge from YAML
	if hasLocalFile {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading config file %s: %w", path, err)
		}

		expandedData := expandEnv(string(data))
		if err := yaml.Unmarshal([]byte(expandedData), &cfg); err != nil {
			return nil, fmt.Errorf("parsing YAML config: %w", err)
		}
	}

	if len(cfg.Supabase) == 0 && len(cfg.Kafka) == 0 {
		return nil, fmt.Errorf("no target configurations found from Spring Cloud Config (%s) or local file (%s)", scc.URL, path)
	}

	setDefaults(&cfg)
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// extractSCCCustomProperties maps db.<alias> and kafka.<alias> from Viper into Config
func extractSCCCustomProperties(cfg *Config) {
	// 1. Map log.level
	if logLevel := viper.GetString("log.level"); logLevel != "" && cfg.App.LogLevel == "" {
		cfg.App.LogLevel = logLevel
	}

	// 2. Map db.<alias>.*
	if dbMap := viper.GetStringMap("db"); len(dbMap) > 0 {
		for alias, val := range dbMap {
			if props, ok := val.(map[string]interface{}); ok {
				host, _ := props["host"].(string)
				if host != "" {
					exists := false
					for _, sb := range cfg.Supabase {
						if sb.Alias == alias {
							exists = true
							break
						}
					}
					if !exists {
						enabled := true
						cfg.Supabase = append(cfg.Supabase, SupabaseConfig{
							Alias:     alias,
							Enabled:   &enabled,
							Host:      host,
							URL:       host,
							TableName: "heartbeats",
							Interval:  resolveDefaultInterval(),
							Cleanup: SupabaseCleanupConfig{
								Enabled:        true,
								Retention:      "24h",
								RetentionHours: 24,
							},
						})
					}
				}
			}
		}
	}

	// 3. Map kafka.<alias>.*
	if kafkaMap := viper.GetStringMap("kafka"); len(kafkaMap) > 0 {
		for alias, val := range kafkaMap {
			if props, ok := val.(map[string]interface{}); ok {
				var bootstrapServers string
				if bs, ok := props["bootstrap.servers"].(string); ok {
					bootstrapServers = bs
				} else if bsMap, ok := props["bootstrap"].(map[string]interface{}); ok {
					bootstrapServers, _ = bsMap["servers"].(string)
				}

				if bootstrapServers != "" {
					exists := false
					for _, kf := range cfg.Kafka {
						if kf.Alias == alias {
							exists = true
							break
						}
					}
					if !exists {
						enabled := true
						var brokers []string
						for _, b := range strings.Split(bootstrapServers, ",") {
							if trimmed := strings.TrimSpace(b); trimmed != "" {
								brokers = append(brokers, trimmed)
							}
						}

						topic, _ := props["topic"].(string)
						if topic == "" {
							topic = "heartbeat-ping"
						}

						groupID, _ := props["group_id"].(string)
						if groupID == "" {
							groupID = fmt.Sprintf("hb-%s-group", alias)
						}

						protocol := getStringFromMap(props, "security.protocol", "security", "protocol")
						if protocol == "" {
							protocol = "SASL_SSL"
						}

						saslMech := getStringFromMap(props, "sasl.mechanism", "sasl", "mechanism")
						saslUser := getStringFromMap(props, "sasl.username", "sasl", "username")
						saslPass := getStringFromMap(props, "sasl.password", "sasl", "password")
						caLoc := getStringFromMap(props, "ssl.ca.location", "ssl", "ca", "location")

						produceEnabled := true
						consumeEnabled := true

						cfg.Kafka = append(cfg.Kafka, KafkaConfig{
							Alias:    alias,
							Enabled:  &enabled,
							Brokers:  brokers,
							Topic:    topic,
							Interval: resolveDefaultInterval(),
							Security: KafkaSecurityConfig{
								Protocol: protocol,
								SSL: KafkaSSLConfig{
									CACertFile: caLoc,
								},
								SASL: KafkaSASLConfig{
									Mechanism: saslMech,
									Username:  saslUser,
									Password:  saslPass,
								},
							},
							Produce: KafkaProduceConfig{
								Enabled: &produceEnabled,
							},
							Consume: KafkaConsumeConfig{
								Enabled: &consumeEnabled,
								GroupID: groupID,
							},
						})
					}
				}
			}
		}
	}
}

func getStringFromMap(m map[string]interface{}, directKey string, nestedKeys ...string) string {
	if val, ok := m[directKey].(string); ok && val != "" {
		return val
	}

	curr := m
	for i, k := range nestedKeys {
		if i == len(nestedKeys)-1 {
			if str, ok := curr[k].(string); ok {
				return str
			}
		} else {
			if next, ok := curr[k].(map[string]interface{}); ok {
				curr = next
			} else {
				break
			}
		}
	}

	return ""
}

var indexedKeyRegex = regexp.MustCompile(`^([a-zA-Z0-9_\-]+)\[([0-9]+)\]$`)

func normalizeConfigMap(input map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	indexedSlices := make(map[string]map[int]interface{})

	for k, v := range input {
		if nestedMap, ok := v.(map[string]interface{}); ok {
			v = normalizeConfigMap(nestedMap)
		}

		if match := indexedKeyRegex.FindStringSubmatch(k); len(match) == 3 {
			sliceName := match[1]
			idx, _ := strconv.Atoi(match[2])

			if indexedSlices[sliceName] == nil {
				indexedSlices[sliceName] = make(map[int]interface{})
			}
			indexedSlices[sliceName][idx] = v
		} else {
			result[k] = v
		}
	}

	for sliceName, idxMap := range indexedSlices {
		maxIdx := -1
		for idx := range idxMap {
			if idx > maxIdx {
				maxIdx = idx
			}
		}

		slice := make([]interface{}, maxIdx+1)
		for idx, val := range idxMap {
			slice[idx] = val
		}
		result[sliceName] = slice
	}

	return result
}

// resolveDefaultInterval returns interval from HEALH_INTERVAL (or HEALTH_INTERVAL) env, defaulting to "5m"
func resolveDefaultInterval() string {
	if val := os.Getenv("HEALH_INTERVAL"); val != "" {
		return val
	}
	if val := os.Getenv("HEALTH_INTERVAL"); val != "" {
		return val
	}
	return "5m"
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
	cfg.App.Server.Enabled = true

	defaultInterval := resolveDefaultInterval()

	if cfg.Scheduler.DefaultInterval == "" {
		cfg.Scheduler.DefaultInterval = defaultInterval
	}

	for i := range cfg.Supabase {
		if cfg.Supabase[i].Interval == "" {
			cfg.Supabase[i].Interval = defaultInterval
		}
		if cfg.Supabase[i].TableName == "" {
			cfg.Supabase[i].TableName = "heartbeats"
		}
		if cfg.Supabase[i].Timeout == "" {
			cfg.Supabase[i].Timeout = "15s"
		}
		if cfg.Supabase[i].Cleanup.Retention == "" && cfg.Supabase[i].Cleanup.RetentionHours <= 0 && cfg.Supabase[i].Cleanup.RetentionDays <= 0 {
			cfg.Supabase[i].Cleanup.Retention = "24h"
			cfg.Supabase[i].Cleanup.RetentionHours = 24
		}
	}

	for i := range cfg.Kafka {
		if cfg.Kafka[i].Interval == "" {
			cfg.Kafka[i].Interval = defaultInterval
		}
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
			if sb.URL == "" && sb.Host == "" {
				return fmt.Errorf("supabase [%s] missing 'url' or 'host'", sb.Alias)
			}
			// If not a postgresql:// connection string, API key is required
			target := sb.Host
			if target == "" {
				target = sb.URL
			}
			if !strings.HasPrefix(target, "postgres://") && !strings.HasPrefix(target, "postgresql://") {
				if sb.ApiKey == "" {
					return fmt.Errorf("supabase [%s] missing 'api_key'", sb.Alias)
				}
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
