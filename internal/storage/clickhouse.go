package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/eventflow/event-processor/internal/model"
)

// ClickHouse wraps the native ClickHouse connection and provides a batch
// insert method for EnrichedEvent slices.
type ClickHouse struct {
	conn driver.Conn
}

// Config holds ClickHouse connection settings.
type Config struct {
	Addr     string
	Database string
	Username string
	Password string
}

// New opens and pings a ClickHouse connection. It retries with exponential
// backoff to handle the case where ClickHouse is still initialising inside Docker.
func New(cfg Config) (*ClickHouse, error) {
	opts := &clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:  5 * time.Second,
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		// Native protocol LZ4 compression improves INSERT throughput.
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	var (
		conn driver.Conn
		err  error
	)

	// Retry up to 10 times with exponential backoff (max ~30 s total).
	backoff := 500 * time.Millisecond
	for attempt := 1; attempt <= 10; attempt++ {
		conn, err = clickhouse.Open(opts)
		if err == nil {
			pingErr := conn.Ping(context.Background())
			if pingErr == nil {
				return &ClickHouse{conn: conn}, nil
			}
			err = pingErr
			_ = conn.Close()
		}
		time.Sleep(backoff)
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
	return nil, fmt.Errorf("clickhouse connect to %s after retries: %w", cfg.Addr, err)
}

// Insert performs a batch INSERT of enriched events into the events_local table.
func (ch *ClickHouse) Insert(ctx context.Context, events []*model.EnrichedEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := ch.conn.PrepareBatch(ctx,
		`INSERT INTO events_local
		 (event_id, project_id, event_name, timestamp, user_id, anonymous_id,
		  session_id, properties, ua_browser, ua_os, geo_country, geo_city, ingested_at)`)
	if err != nil {
		return fmt.Errorf("clickhouse prepare batch: %w", err)
	}

	for _, e := range events {
		props := e.Properties
		if props == "" {
			props = "{}"
		}
		if err := batch.Append(
			e.EventID,
			e.ProjectID,
			e.EventName,
			e.Timestamp,
			e.UserID,
			e.AnonymousID,
			e.SessionID,
			props,
			e.UABrowser,
			e.UAOS,
			e.GeoCountry,
			e.GeoCity,
			e.IngestedAt,
		); err != nil {
			return fmt.Errorf("clickhouse append event %s: %w", e.EventID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse send batch: %w", err)
	}

	return nil
}

// Ping verifies the ClickHouse connection is alive. Used by the readiness probe.
func (ch *ClickHouse) Ping(ctx context.Context) error {
	return ch.conn.Ping(ctx)
}

// Close releases the underlying ClickHouse connection pool.
func (ch *ClickHouse) Close() error {
	return ch.conn.Close()
}
