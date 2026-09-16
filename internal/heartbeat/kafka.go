package heartbeat

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type KafkaService struct{}

func NewKafkaService() *KafkaService {
	return &KafkaService{}
}

// Ping performs Kafka produce and consume operations for heartbeat keep-alive
func (k *KafkaService) Ping(ctx context.Context, cfg *config.KafkaConfig) *HeartbeatResult {
	start := time.Now()

	tlsConfig, saslMechanism, err := k.buildSecurity(cfg)
	if err != nil {
		return &HeartbeatResult{
			TargetType: "kafka",
			Alias:      cfg.Alias,
			Success:    false,
			Latency:    time.Since(start),
			Message:    fmt.Sprintf("security config error: %v", err),
			Timestamp:  time.Now(),
		}
	}

	transport := &kafka.Transport{
		TLS:  tlsConfig,
		SASL: saslMechanism,
	}

	var msgs []string

	// 1. Produce Heartbeat
	if cfg.Produce.IsEnabled() {
		produceErr := k.produceHeartbeat(ctx, cfg, transport)
		if produceErr != nil {
			return &HeartbeatResult{
				TargetType: "kafka",
				Alias:      cfg.Alias,
				Success:    false,
				Latency:    time.Since(start),
				Message:    fmt.Sprintf("produce failed: %v", produceErr),
				Timestamp:  time.Now(),
			}
		}
		msgs = append(msgs, fmt.Sprintf("produced to topic '%s'", cfg.Topic))
	}

	// 2. Consume Heartbeat (to activate consumer group / partition reads)
	if cfg.Consume.IsEnabled() {
		consumeErr := k.consumeHeartbeat(ctx, cfg, tlsConfig, saslMechanism)
		if consumeErr != nil {
			slog.Warn("Kafka consume check encountered notice",
				slog.String("alias", cfg.Alias),
				slog.Any("error", consumeErr),
			)
			msgs = append(msgs, fmt.Sprintf("consume: %v", consumeErr))
		} else {
			msgs = append(msgs, fmt.Sprintf("consumed from group '%s'", cfg.Consume.GroupID))
		}
	}

	slog.Info("Kafka heartbeat cycle completed",
		slog.String("alias", cfg.Alias),
		slog.String("topic", cfg.Topic),
		slog.Duration("latency", time.Since(start)),
	)

	return &HeartbeatResult{
		TargetType: "kafka",
		Alias:      cfg.Alias,
		Success:    true,
		Latency:    time.Since(start),
		Message:    strings.Join(msgs, " | "),
		Timestamp:  time.Now(),
	}
}

func (k *KafkaService) produceHeartbeat(ctx context.Context, cfg *config.KafkaConfig, transport *kafka.Transport) error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		Transport:    transport,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	payload := map[string]interface{}{
		"alias":     cfg.Alias,
		"status":    "alive",
		"source":    "heartbeat-daemon",
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if cfg.Produce.Message != "" {
		payload["message"] = cfg.Produce.Message
	}

	valueBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	key := cfg.Produce.Key
	if key == "" {
		key = cfg.Alias
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: valueBytes,
		Time:  time.Now(),
	}

	return writer.WriteMessages(ctx, msg)
}

func (k *KafkaService) consumeHeartbeat(ctx context.Context, cfg *config.KafkaConfig, tlsConfig *tls.Config, saslMech sasl.Mechanism) error {
	timeout := config.ParseDuration(cfg.Consume.Timeout, 8*time.Second)
	consumeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: saslMech,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.Consume.GroupID,
		Dialer:         dialer,
		CommitInterval: time.Second,
		MaxWait:        2 * time.Second,
	})
	defer reader.Close()

	// Read message or wait for timeout
	msg, err := reader.ReadMessage(consumeCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// Timeout is acceptable for idle keepalive if topic had no recent uncommitted message
			return nil
		}
		return err
	}

	slog.Debug("Kafka message consumed successfully",
		slog.String("alias", cfg.Alias),
		slog.String("key", string(msg.Key)),
		slog.Int64("offset", msg.Offset),
		slog.Int("partition", msg.Partition),
	)
	return nil
}

func (k *KafkaService) buildSecurity(cfg *config.KafkaConfig) (*tls.Config, sasl.Mechanism, error) {
	var tlsConfig *tls.Config
	var saslMech sasl.Mechanism

	protocol := strings.ToUpper(strings.TrimSpace(cfg.Security.Protocol))

	// Configure TLS / SSL
	if protocol == "SSL" || protocol == "SASL_SSL" {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: cfg.Security.SSL.InsecureSkipVerify,
			MinVersion:         tls.VersionTLS12,
		}

		// CA Certificate
		caData, err := loadCertificateData(cfg.Security.SSL.CACertFile, cfg.Security.SSL.CACertPEM)
		if err != nil {
			return nil, nil, fmt.Errorf("loading CA cert: %w", err)
		}
		if len(caData) > 0 {
			caPool := x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caData) {
				return nil, nil, fmt.Errorf("failed to append CA certificate to pool")
			}
			tlsConfig.RootCAs = caPool
		}

		// Client Certificate & Key (for Aiven mTLS)
		certData, err := loadCertificateData(cfg.Security.SSL.ClientCertFile, cfg.Security.SSL.ClientCertPEM)
		if err != nil {
			return nil, nil, fmt.Errorf("loading client cert: %w", err)
		}
		keyData, err := loadCertificateData(cfg.Security.SSL.ClientKeyFile, cfg.Security.SSL.ClientKeyPEM)
		if err != nil {
			return nil, nil, fmt.Errorf("loading client key: %w", err)
		}

		if len(certData) > 0 && len(keyData) > 0 {
			cert, err := tls.X509KeyPair(certData, keyData)
			if err != nil {
				return nil, nil, fmt.Errorf("creating x509 key pair: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	// Configure SASL
	if protocol == "SASL_PLAINTEXT" || protocol == "SASL_SSL" {
		mech := strings.ToUpper(strings.TrimSpace(cfg.Security.SASL.Mechanism))
		user := cfg.Security.SASL.Username
		pass := cfg.Security.SASL.Password

		switch mech {
		case "PLAIN":
			saslMech = plain.Mechanism{
				Username: user,
				Password: pass,
			}
		case "SCRAM-SHA-256":
			m, err := scram.Mechanism(scram.SHA256, user, pass)
			if err != nil {
				return nil, nil, fmt.Errorf("scram-sha-256 error: %w", err)
			}
			saslMech = m
		case "SCRAM-SHA-512":
			m, err := scram.Mechanism(scram.SHA512, user, pass)
			if err != nil {
				return nil, nil, fmt.Errorf("scram-sha-512 error: %w", err)
			}
			saslMech = m
		default:
			return nil, nil, fmt.Errorf("unsupported SASL mechanism: %s", mech)
		}
	}

	return tlsConfig, saslMech, nil
}

func loadCertificateData(filePath, rawPEM string) ([]byte, error) {
	if rawPEM != "" {
		return []byte(rawPEM), nil
	}
	if filePath != "" {
		return os.ReadFile(filePath)
	}
	return nil, nil
}
