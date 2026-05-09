package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/eventflow/event-processor/internal/config"
	"github.com/eventflow/event-processor/internal/consumer"
	"github.com/eventflow/event-processor/internal/logger"
	"github.com/eventflow/event-processor/internal/metrics"
	"github.com/eventflow/event-processor/internal/pipeline"
	"github.com/eventflow/event-processor/internal/producer"
	"github.com/eventflow/event-processor/internal/server"
	"github.com/eventflow/event-processor/internal/storage"
)

func main() {
	// ── Logger ────────────────────────────────────────────────────────────────
	log := logger.Must("event-processor")
	defer func() { _ = log.Sync() }()

	// ── Config ────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config", zap.Error(err))
	}
	log.Info("configuration loaded",
		zap.String("kafka_brokers", cfg.KafkaBrokers),
		zap.String("kafka_topic_raw", cfg.KafkaTopicRaw),
		zap.String("clickhouse_addr", cfg.ClickHouseAddr),
		zap.String("http_addr", cfg.HTTPAddr),
	)

	// ── HTTP server — starts FIRST ────────────────────────────────────────────
	// The server must be available immediately so Docker's liveness probe
	// (GET /healthz) succeeds during the ClickHouse / Kafka connection phase.
	// /readyz returns 503 until SetClickHouse is called below.
	srv := server.New(cfg.HTTPAddr, log)
	srv.Start()

	// ── Metrics ───────────────────────────────────────────────────────────────
	m := metrics.New()

	// ── ClickHouse ────────────────────────────────────────────────────────────
	log.Info("connecting to clickhouse", zap.String("addr", cfg.ClickHouseAddr))
	ch, err := storage.New(storage.Config{
		Addr:     cfg.ClickHouseAddr,
		Database: cfg.ClickHouseDB,
		Username: cfg.ClickHouseUser,
		Password: cfg.ClickHousePassword,
	})
	if err != nil {
		log.Fatal("clickhouse init failed", zap.Error(err))
	}
	defer func() {
		if err := ch.Close(); err != nil {
			log.Error("clickhouse close error", zap.Error(err))
		}
	}()
	log.Info("clickhouse connected")

	// Wire ClickHouse into the readiness probe — /readyz now returns 200.
	srv.SetClickHouse(ch)

	// ── Kafka consumer ────────────────────────────────────────────────────────
	log.Info("connecting to kafka", zap.String("topic", cfg.KafkaTopicRaw))
	reader := consumer.NewFromDSN(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaTopicRaw)
	defer func() {
		if err := reader.Close(); err != nil {
			log.Error("kafka reader close error", zap.Error(err))
		}
	}()

	// ── Kafka producer ────────────────────────────────────────────────────────
	prod := producer.NewFromDSN(
		cfg.KafkaBrokers,
		cfg.KafkaTopicValid,
		cfg.KafkaTopicDLQ,
	)
	defer func() {
		if err := prod.Close(); err != nil {
			log.Error("kafka producer close error", zap.Error(err))
		}
	}()

	// ── Pipeline ──────────────────────────────────────────────────────────────
	// done signals background goroutines (session cache cleanup) to stop.
	done := make(chan struct{})

	validator := pipeline.NewValidator()
	enricher := pipeline.NewEnricher(cfg.SessionIdleTimeout, done)

	p := pipeline.New(
		reader,
		validator,
		enricher,
		ch,
		prod,
		m,
		log,
		pipeline.Config{
			BatchSize:     cfg.ClickHouseBatchSize,
			FlushInterval: cfg.ClickHouseFlushInterval,
			WorkerCount:   cfg.ValidatorWorkers,
		},
	)

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Info("event-processor started")

	// Run the pipeline; blocks until ctx is cancelled.
	if err := p.Run(ctx); err != nil {
		log.Error("pipeline error", zap.Error(err))
	}

	// Signal background goroutines to stop.
	close(done)

	// Shutdown the HTTP server.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http server shutdown error", zap.Error(err))
	}

	log.Info("event-processor stopped gracefully")
}
