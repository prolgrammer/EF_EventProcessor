package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/eventflow/event-processor/internal/model"
)

// Producer wraps two Kafka writers: one for events.valid and one for
// events.dlq. It serialises events to JSON before writing.
type Producer struct {
	validWriter *kafka.Writer
	dlqWriter   *kafka.Writer
}

// Config holds Kafka producer settings.
type Config struct {
	Brokers    []string
	TopicValid string
	TopicDLQ   string
}

// New creates a Producer with synchronous, batched writers.
// Synchronous mode (Async: false, the default) ensures WriteMessages returns
// any broker error to the caller, which is critical for the pipeline's
// error-handling logic (log, update metrics, decide whether to retry).
func New(cfg Config) *Producer {
	newWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // partition by message key (project_id)
			BatchTimeout: 5 * time.Millisecond,
			BatchSize:    100,
			RequiredAcks: kafka.RequireOne,
			// Async is intentionally left false (default): synchronous writes
			// allow the pipeline to capture and handle broker errors properly.
		}
	}

	return &Producer{
		validWriter: newWriter(cfg.TopicValid),
		dlqWriter:   newWriter(cfg.TopicDLQ),
	}
}

// NewFromDSN creates a Producer from a comma-separated broker string.
func NewFromDSN(brokersDSN, topicValid, topicDLQ string) *Producer {
	brokers := strings.Split(brokersDSN, ",")
	return New(Config{
		Brokers:    brokers,
		TopicValid: topicValid,
		TopicDLQ:   topicDLQ,
	})
}

// PublishValid publishes a slice of enriched events to events.valid.
// Errors are returned but are non-fatal from the pipeline's perspective —
// the caller decides whether to continue or retry.
func (p *Producer) PublishValid(ctx context.Context, events []*model.EnrichedEvent) error {
	msgs := make([]kafka.Message, 0, len(events))
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal enriched event %s: %w", e.EventID, err)
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(e.ProjectID),
			Value: payload,
		})
	}
	if err := p.validWriter.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("produce to events.valid: %w", err)
	}
	return nil
}

// PublishDLQ publishes failed events to events.dlq.
func (p *Producer) PublishDLQ(ctx context.Context, events []model.DLQEvent) error {
	msgs := make([]kafka.Message, 0, len(events))
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal dlq event: %w", err)
		}
		key := e.EventID
		if key == "" {
			key = "unknown"
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(key),
			Value: payload,
		})
	}
	if err := p.dlqWriter.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("produce to events.dlq: %w", err)
	}
	return nil
}

// Close flushes pending messages and closes both writers.
func (p *Producer) Close() error {
	var errs []error
	if err := p.validWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close valid writer: %w", err))
	}
	if err := p.dlqWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close dlq writer: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("producer close errors: %v", errs)
	}
	return nil
}
