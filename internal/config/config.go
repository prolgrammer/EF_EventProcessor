package config

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config holds all configuration for the event-processor service.
// Values are loaded from environment variables.
type Config struct {
	// Kafka consumer settings
	KafkaBrokers    string `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	KafkaGroupID    string `envconfig:"KAFKA_GROUP_ID" default:"processor"`
	KafkaTopicRaw   string `envconfig:"KAFKA_TOPIC_RAW" default:"events.raw"`
	KafkaTopicValid string `envconfig:"KAFKA_TOPIC_VALID" default:"events.valid"`
	KafkaTopicDLQ   string `envconfig:"KAFKA_TOPIC_DLQ" default:"events.dlq"`

	// ClickHouse settings
	ClickHouseAddr     string        `envconfig:"CLICKHOUSE_ADDR" default:"localhost:9000"`
	ClickHouseDB       string        `envconfig:"CLICKHOUSE_DB" default:"eventflow"`
	ClickHouseUser     string        `envconfig:"CLICKHOUSE_USER" default:"default"`
	ClickHousePassword string        `envconfig:"CLICKHOUSE_PASSWORD" default:""`
	ClickHouseBatchSize int          `envconfig:"CLICKHOUSE_BATCH_SIZE" default:"10000"`
	ClickHouseFlushInterval time.Duration `envconfig:"CLICKHOUSE_FLUSH_INTERVAL" default:"1s"`

	// HTTP server
	HTTPAddr string `envconfig:"HTTP_ADDR" default:":8080"`

	// Pipeline settings
	ValidatorWorkers int `envconfig:"VALIDATOR_WORKERS" default:"4"`
	EnricherWorkers  int `envconfig:"ENRICHER_WORKERS" default:"4"`

	// Session stitching: idle timeout in minutes
	SessionIdleTimeout time.Duration `envconfig:"SESSION_IDLE_TIMEOUT" default:"30m"`
}

// Load reads configuration from environment variables.
// The prefix "EP" can be used (e.g. EP_KAFKA_BROKERS), though unprefixed
// names also work.
func Load() (*Config, error) {
	cfg := &Config{}
	if err := envconfig.Process("", cfg); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}
