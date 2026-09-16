package heartbeat

import (
	"testing"

	"github.com/KAnggara75/HeartBeat/internal/config"
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
