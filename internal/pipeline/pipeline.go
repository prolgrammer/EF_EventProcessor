package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/eventflow/event-processor/internal/consumer"
	"github.com/eventflow/event-processor/internal/metrics"
	"github.com/eventflow/event-processor/internal/model"
	"github.com/eventflow/event-processor/internal/producer"
	"github.com/eventflow/event-processor/internal/storage"
)

// pendingMessage carries a Kafka message through all pipeline stages so that
// we can commit offsets only after a successful ClickHouse write.
type pendingMessage struct {
	msg       consumer.Message
	raw       *model.RawEvent
	parseErr  error
	enriched  *model.EnrichedEvent
	dlqReason string
	dlqErrMsg string
}

// Pipeline is the core ETL component. It reads events from Kafka, validates
// and enriches them in parallel, batch-inserts into ClickHouse, publishes
// to events.valid / events.dlq, and commits Kafka offsets.
//
// Design decisions:
//   - Single reader goroutine (kafka-go Reader is not safe for concurrent reads).
//   - Validate + enrich stages run in parallel within each batch using errgroup.
//   - Kafka offset commit only happens after a successful ClickHouse insert;
//     this gives at-least-once delivery semantics.
type Pipeline struct {
	reader    *consumer.Reader
	validator *Validator
	enricher  *Enricher
	storage   *storage.ClickHouse
	producer  *producer.Producer
	metrics   *metrics.Metrics
	log       *zap.Logger

	batchSize     int
	flushInterval time.Duration
	workerCount   int
}

// Config holds the pipeline configuration.
type Config struct {
	BatchSize     int
	FlushInterval time.Duration
	WorkerCount   int
}

// New creates a new Pipeline.
func New(
	reader *consumer.Reader,
	validator *Validator,
	enricher *Enricher,
	ch *storage.ClickHouse,
	prod *producer.Producer,
	m *metrics.Metrics,
	log *zap.Logger,
	cfg Config,
) *Pipeline {
	return &Pipeline{
		reader:        reader,
		validator:     validator,
		enricher:      enricher,
		storage:       ch,
		producer:      prod,
		metrics:       m,
		log:           log,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		workerCount:   cfg.WorkerCount,
	}
}

// Run starts the main event-processing loop. It blocks until ctx is cancelled.
// Graceful shutdown: on ctx cancellation the current partial batch is flushed
// before the function returns.
func (p *Pipeline) Run(ctx context.Context) error {
	p.log.Info("pipeline started",
		zap.Int("batch_size", p.batchSize),
		zap.Duration("flush_interval", p.flushInterval),
		zap.Int("workers", p.workerCount),
	)

	pending := make([]*pendingMessage, 0, p.batchSize)
	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	for {
		// Try to fetch the next message with a short timeout so we can
		// check the flush ticker regularly without blocking forever.
		fetchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		msg, err := p.reader.FetchMessage(fetchCtx)
		cancel()

		switch {
		case err == nil:
			pending = append(pending, &pendingMessage{msg: msg})

		case errors.Is(err, context.DeadlineExceeded):
			// Normal timeout — no new messages yet, check flush below.

		case errors.Is(err, context.Canceled):
			// Parent context cancelled — flush remaining batch and exit.
			p.log.Info("pipeline shutting down, flushing remaining batch",
				zap.Int("pending", len(pending)),
			)
			if len(pending) > 0 {
				// Use a background context so the flush is not aborted.
				flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
				p.flush(flushCtx, pending)
				flushCancel()
			}
			return nil

		default:
			p.log.Error("kafka fetch error", zap.Error(err))
			continue
		}

		// Check time-based flush condition.
		shouldFlush := len(pending) >= p.batchSize
		select {
		case <-ticker.C:
			if len(pending) > 0 {
				shouldFlush = true
			}
		default:
		}

		if shouldFlush && len(pending) > 0 {
			p.flush(ctx, pending)
			pending = pending[:0]
		}
	}
}

// flush processes a batch of pending messages end-to-end:
//  1. Parse raw JSON in parallel.
//  2. Validate each parsed event in parallel.
//  3. Enrich valid events in parallel.
//  4. Batch-insert into ClickHouse.
//  5. Publish to events.valid and events.dlq.
//  6. Commit Kafka offsets.
func (p *Pipeline) flush(ctx context.Context, pending []*pendingMessage) {
	start := time.Now()
	defer func() {
		p.metrics.BatchProcessDuration.Observe(time.Since(start).Seconds())
	}()

	// ── Stage 1: Parse ────────────────────────────────────────────────────────
	g, _ := errgroup.WithContext(ctx)
	sem := make(chan struct{}, p.workerCount)
	for _, pm := range pending {
		pm := pm
		g.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()
			pm.raw, pm.parseErr = parseRawEvent(pm.msg.Value)
			return nil // never propagate; errors are tracked per-message
		})
	}
	_ = g.Wait()

	// ── Stage 2: Validate + Enrich ────────────────────────────────────────────
	g2, _ := errgroup.WithContext(ctx)
	for _, pm := range pending {
		pm := pm
		g2.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()

			if pm.parseErr != nil {
				pm.dlqReason = "parse_error"
				pm.dlqErrMsg = pm.parseErr.Error()
				return nil
			}
			if err := p.validator.Validate(pm.raw); err != nil {
				pm.dlqReason = "validation_error"
				pm.dlqErrMsg = err.Error()
				return nil
			}
			pm.enriched = p.enricher.Enrich(pm.raw)
			return nil
		})
	}
	_ = g2.Wait()

	// ── Separate valid vs DLQ ─────────────────────────────────────────────────
	validEvents := make([]*model.EnrichedEvent, 0, len(pending))
	dlqEvents := make([]model.DLQEvent, 0)
	allMsgs := make([]consumer.Message, 0, len(pending))

	for _, pm := range pending {
		allMsgs = append(allMsgs, pm.msg)
		p.metrics.EventsConsumedTotal.WithLabelValues(projectIDFromMsg(pm)).Inc()

		if pm.dlqReason != "" {
			dlqEvents = append(dlqEvents, buildDLQEvent(pm))
			p.metrics.EventsDLQTotal.WithLabelValues(pm.dlqReason).Inc()
			p.log.Warn("event rejected",
				zap.String("reason", pm.dlqReason),
				zap.String("error", pm.dlqErrMsg),
			)
		} else {
			validEvents = append(validEvents, pm.enriched)
		}
	}

	// ── Stage 3: ClickHouse insert ────────────────────────────────────────────
	if len(validEvents) > 0 {
		chStart := time.Now()
		if err := p.storage.Insert(ctx, validEvents); err != nil {
			p.metrics.CHInsertErrors.Inc()
			p.log.Error("clickhouse insert failed — not committing offsets",
				zap.Int("batch_size", len(validEvents)),
				zap.Error(err),
			)
			// Do NOT commit offsets: the batch will be reprocessed on restart.
			// Publish DLQ events that were already identified, still.
			_ = p.producer.PublishDLQ(ctx, dlqEvents)
			return
		}
		p.metrics.CHInsertDuration.Observe(time.Since(chStart).Seconds())
		p.metrics.CHInsertBatchSize.Observe(float64(len(validEvents)))
		p.metrics.EventsValidTotal.Add(float64(len(validEvents)))
		p.log.Info("batch inserted to clickhouse",
			zap.Int("events", len(validEvents)),
			zap.Duration("duration", time.Since(chStart)),
		)
	}

	// ── Stage 4: Produce to events.valid ──────────────────────────────────────
	if len(validEvents) > 0 {
		if err := p.producer.PublishValid(ctx, validEvents); err != nil {
			p.log.Error("publish to events.valid failed", zap.Error(err))
			p.metrics.KafkaProduceErrors.WithLabelValues("events.valid").Inc()
		} else {
			p.metrics.KafkaProduceTotal.WithLabelValues("events.valid").Add(float64(len(validEvents)))
		}
	}

	// ── Stage 5: Produce to events.dlq ───────────────────────────────────────
	if len(dlqEvents) > 0 {
		if err := p.producer.PublishDLQ(ctx, dlqEvents); err != nil {
			p.log.Error("publish to events.dlq failed", zap.Error(err))
			p.metrics.KafkaProduceErrors.WithLabelValues("events.dlq").Inc()
		} else {
			p.metrics.KafkaProduceTotal.WithLabelValues("events.dlq").Add(float64(len(dlqEvents)))
		}
	}

	// ── Stage 6: Commit Kafka offsets ─────────────────────────────────────────
	if err := p.reader.CommitMessages(ctx, allMsgs...); err != nil {
		p.log.Error("kafka commit failed", zap.Error(err))
	}

	// Update session cache size gauge.
	p.metrics.SessionCacheSize.Set(float64(p.enricher.SessionCacheSize()))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func parseRawEvent(data []byte) (*model.RawEvent, error) {
	var evt model.RawEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return nil, err
	}
	return &evt, nil
}

func projectIDFromMsg(pm *pendingMessage) string {
	if pm.raw != nil {
		return pm.raw.ProjectID
	}
	return "unknown"
}

func buildDLQEvent(pm *pendingMessage) model.DLQEvent {
	dlq := model.DLQEvent{
		Raw:      pm.msg.Value,
		Reason:   pm.dlqReason,
		Error:    pm.dlqErrMsg,
		FailedAt: time.Now().UTC(),
	}
	if pm.raw != nil {
		dlq.EventID = pm.raw.EventID
		dlq.ProjectID = pm.raw.ProjectID
	}
	return dlq
}
