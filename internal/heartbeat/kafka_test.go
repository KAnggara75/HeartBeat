package heartbeat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KAnggara75/HeartBeat/internal/config"
	"github.com/segmentio/kafka-go"
)

func TestBuildSecurityPlaintext(t *testing.T) {
	svc := NewKafkaService()
	cfg := &config.KafkaConfig{
		Alias:   "kafka-local",
		Brokers: []string{"localhost:9092"},
		Security: config.KafkaSecurityConfig{
			Protocol: "PLAINTEXT",
		},
	}

	tlsConfig, saslMech, err := svc.buildSecurity(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsConfig != nil {
		t.Errorf("expected nil tlsConfig for PLAINTEXT")
	}
	if saslMech != nil {
		t.Errorf("expected nil saslMech for PLAINTEXT")
	}
}

func TestBuildSecuritySASL(t *testing.T) {
	svc := NewKafkaService()
	cfg := &config.KafkaConfig{
		Alias:   "kafka-sasl",
		Brokers: []string{"localhost:9092"},
		Security: config.KafkaSecurityConfig{
			Protocol: "SASL_PLAINTEXT",
			SASL: config.KafkaSASLConfig{
				Mechanism: "PLAIN",
				Username:  "admin",
				Password:  "secret",
			},
		},
	}

	tlsConfig, saslMech, err := svc.buildSecurity(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsConfig != nil {
		t.Errorf("expected nil tlsConfig for SASL_PLAINTEXT")
	}
	if saslMech == nil {
		t.Fatalf("expected non-nil saslMech")
	}
	if saslMech.Name() != "PLAIN" {
		t.Errorf("expected PLAIN mechanism, got %s", saslMech.Name())
	}
}

func TestBuildSecuritySCRAM512(t *testing.T) {
	svc := NewKafkaService()
	cfg := &config.KafkaConfig{
		Alias:   "kafka-aiven",
		Brokers: []string{"kafka.aivencloud.com:12345"},
		Security: config.KafkaSecurityConfig{
			Protocol: "SASL_SSL",
			SSL: config.KafkaSSLConfig{
				InsecureSkipVerify: true,
			},
			SASL: config.KafkaSASLConfig{
				Mechanism: "SCRAM-SHA-512",
				Username:  "avnadmin",
				Password:  "password123",
			},
		},
	}

	tlsConfig, saslMech, err := svc.buildSecurity(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsConfig == nil {
		t.Errorf("expected non-nil tlsConfig for SASL_SSL")
	}
	if saslMech == nil {
		t.Fatalf("expected non-nil saslMech")
	}
	if saslMech.Name() != "SCRAM-SHA-512" {
		t.Errorf("expected SCRAM-SHA-512 mechanism, got %s", saslMech.Name())
	}
}

func TestLoadCertificateData(t *testing.T) {
	// 1. Raw PEM
	data, err := loadCertificateData("", "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----")
	if err != nil {
		t.Fatalf("unexpected error for raw PEM: %v", err)
	}
	if !strings.Contains(string(data), "BEGIN CERTIFICATE") {
		t.Errorf("expected cert content in data")
	}

	// 2. Local File
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "ca.pem")
	if err := os.WriteFile(certFile, []byte("local-cert-content"), 0644); err != nil {
		t.Fatalf("failed to write temp cert: %v", err)
	}

	data, err = loadCertificateData(certFile, "")
	if err != nil {
		t.Fatalf("unexpected error for file cert: %v", err)
	}
	if string(data) != "local-cert-content" {
		t.Errorf("expected 'local-cert-content', got %s", string(data))
	}

	// 3. Remote URL success & fail
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cert.pem" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("remote-cert-bytes"))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	remoteData, err := loadCertificateData(ts.URL+"/cert.pem", "")
	if err != nil {
		t.Fatalf("unexpected error for remote cert: %v", err)
	}
	if string(remoteData) != "remote-cert-bytes" {
		t.Errorf("expected 'remote-cert-bytes', got %s", string(remoteData))
	}

	// Remote 404
	_, err = loadCertificateData(ts.URL+"/not-found.pem", "")
	if err == nil {
		t.Errorf("expected error for 404 cert URL, got nil")
	}

	// Empty both
	emptyData, err := loadCertificateData("", "")
	if err != nil || emptyData != nil {
		t.Errorf("expected nil data and nil error for empty inputs")
	}
}

func TestBuildSecurityErrors(t *testing.T) {
	svc := NewKafkaService()

	// 1. Invalid CA PEM in SSL
	cfgInvalidCA := &config.KafkaConfig{
		Security: config.KafkaSecurityConfig{
			Protocol: "SSL",
			SSL: config.KafkaSSLConfig{
				CACertPEM: "invalid-not-a-pem",
			},
		},
	}
	if _, _, err := svc.buildSecurity(cfgInvalidCA); err == nil {
		t.Errorf("expected error for invalid CA PEM, got nil")
	}

	// 2. Invalid Key Pair (cert without valid key)
	cfgInvalidCert := &config.KafkaConfig{
		Security: config.KafkaSecurityConfig{
			Protocol: "SSL",
			SSL: config.KafkaSSLConfig{
				ClientCertPEM: "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----",
				ClientKeyPEM:  "invalid-key",
			},
		},
	}
	if _, _, err := svc.buildSecurity(cfgInvalidCert); err == nil {
		t.Errorf("expected error for invalid cert/key pair, got nil")
	}

	// 3. Unsupported SASL mechanism
	cfgUnsupportedSASL := &config.KafkaConfig{
		Security: config.KafkaSecurityConfig{
			Protocol: "SASL_PLAINTEXT",
			SASL: config.KafkaSASLConfig{
				Mechanism: "GSSAPI_UNSUPPORTED",
			},
		},
	}
	if _, _, err := svc.buildSecurity(cfgUnsupportedSASL); err == nil {
		t.Errorf("expected error for unsupported SASL mechanism, got nil")
	}

	// 4. SCRAM-SHA-256 success
	cfgScram256 := &config.KafkaConfig{
		Security: config.KafkaSecurityConfig{
			Protocol: "SASL_PLAINTEXT",
			SASL: config.KafkaSASLConfig{
				Mechanism: "SCRAM-SHA-256",
				Username:  "u",
				Password:  "p",
			},
		},
	}
	_, saslMech, err := svc.buildSecurity(cfgScram256)
	if err != nil || saslMech == nil || saslMech.Name() != "SCRAM-SHA-256" {
		t.Errorf("expected valid SCRAM-SHA-256 mechanism, err: %v", err)
	}
}

func TestPingSecurityFailure(t *testing.T) {
	svc := NewKafkaService()
	cfg := &config.KafkaConfig{
		Alias: "bad-sec",
		Security: config.KafkaSecurityConfig{
			Protocol: "SASL_PLAINTEXT",
			SASL: config.KafkaSASLConfig{
				Mechanism: "INVALID_MECH",
			},
		},
	}
	res := svc.Ping(context.Background(), cfg)
	if res.Success {
		t.Errorf("expected Ping failure on security config error, got success")
	}
}

func TestEnsureTopicExists_Failure(t *testing.T) {
	svc := NewKafkaService()
	// Unreachable broker port
	cfg := &config.KafkaConfig{
		Alias:   "test-kf",
		Brokers: []string{"127.0.0.1:59996"},
		Topic:   "hb-topic",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := svc.ensureTopicExists(ctx, cfg, &kafka.Transport{})
	if err == nil {
		t.Errorf("expected error connecting to non-existent broker, got nil")
	}
}

func TestConsumeHeartbeat_TimeoutHandled(t *testing.T) {
	svc := NewKafkaService()
	enabled := true
	cfg := &config.KafkaConfig{
		Alias:   "test-kf",
		Brokers: []string{"127.0.0.1:59995"},
		Topic:   "hb-topic",
		Consume: config.KafkaConsumeConfig{
			Enabled: &enabled,
			GroupID: "test-group",
			Timeout: "50ms",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// consumeHeartbeat gracefully returns nil on DeadlineExceeded/Canceled
	_ = svc.consumeHeartbeat(ctx, cfg, nil, nil)
}

func TestPingProduceAndConsumeEnabled(t *testing.T) {
	svc := NewKafkaService()
	enabled := true
	cfg := &config.KafkaConfig{
		Alias:   "test-kf-produce",
		Brokers: []string{"127.0.0.1:59994"},
		Topic:   "hb-topic",
		Produce: config.KafkaProduceConfig{
			Enabled: &enabled,
			Key:     "custom-key",
			Message: "custom-msg",
		},
		Consume: config.KafkaConsumeConfig{
			Enabled: &enabled,
			GroupID: "test-group",
			Timeout: "50ms",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	res := svc.Ping(ctx, cfg)
	if res.Success {
		t.Errorf("expected failure since broker is unreachable, got success")
	}
}
