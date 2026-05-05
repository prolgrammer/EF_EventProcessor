package consumer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message wraps a kafka.Message to expose only what the pipeline needs.
type Message = kafka.Message

// Reader wraps kafka-go Reader with additional configuration and helpers.
type Reader struct {
	r *kafka.Reader
}

// Config holds Kafka consumer configuration.
type Config struct {
	Brokers []string
	GroupID string
	Topic   string
}

// New creates a new Kafka reader configured for at-least-once delivery.
// Auto-commit is disabled; offsets are committed manually after successful
// batch processing.
func New(cfg Config) *Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		GroupID: cfg.GroupID,
		Topic:   cfg.Topic,

		// Never auto-commit; the pipeline commits after a successful flush.
		CommitInterval: 0,

		// Start from the earliest unread offset within the consumer group.
		StartOffset: kafka.FirstOffset,

		// How long to wait for new messages before returning.
		// A short timeout lets the pipeline flush partial batches on time.
		MaxWait: 200 * time.Millisecond,

		// Minimum bytes to fetch in one request.
		MinBytes: 1,

		// Maximum bytes per fetch (1 MB).
		MaxBytes: 1 << 20,

		// Prefer reading from the leader replica for lower latency.
		IsolationLevel: kafka.ReadCommitted,
	})

	return &Reader{r: r}
}

// NewFromDSN creates a Reader from a comma-separated broker string.
func NewFromDSN(brokersDSN, groupID, topic string) *Reader {
	brokers := strings.Split(brokersDSN, ",")
	return New(Config{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
	})
}

// FetchMessage fetches the next available message. It blocks until a message
// arrives or ctx is cancelled. Callers must call CommitMessages after
// processing to advance the consumer-group offset.
func (r *Reader) FetchMessage(ctx context.Context) (Message, error) {
	msg, err := r.r.FetchMessage(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("consumer fetch: %w", err)
	}
	return msg, nil
}

// CommitMessages commits the offsets of the provided messages.
func (r *Reader) CommitMessages(ctx context.Context, msgs ...Message) error {
	if err := r.r.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("consumer commit: %w", err)
	}
	return nil
}

// Close gracefully closes the reader.
func (r *Reader) Close() error {
	return r.r.Close()
}

// Stats returns the current consumer stats (lag, fetch rate, etc.).
func (r *Reader) Stats() kafka.ReaderStats {
	return r.r.Stats()
}
