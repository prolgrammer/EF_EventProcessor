package model

import (
	"encoding/json"
	"time"
)

// EventContext carries transport-level metadata attached by the SDK.
type EventContext struct {
	UA string `json:"ua"` // User-Agent string
	IP string `json:"ip"` // Client IP address
}

// RawEvent is the event as it arrives from Kafka topic events.raw.
// It mirrors exactly what the ingest-service publishes.
type RawEvent struct {
	EventID     string                 `json:"event_id"`
	ProjectID   string                 `json:"project_id"`
	EventName   string                 `json:"event_name"`
	Timestamp   time.Time              `json:"timestamp"`
	UserID      string                 `json:"user_id"`
	AnonymousID string                 `json:"anonymous_id"`
	Properties  map[string]interface{} `json:"properties"`
	Context     EventContext            `json:"context"`
}

// EnrichedEvent is the RawEvent after enrichment. It is inserted into
// ClickHouse and published to events.valid.
// JSON tags use snake_case so that downstream consumers (notification-service,
// analytics-service) receive a consistent field naming convention.
type EnrichedEvent struct {
	EventID     string    `json:"event_id"`
	ProjectID   string    `json:"project_id"`
	EventName   string    `json:"event_name"`
	Timestamp   time.Time `json:"timestamp"`
	UserID      string    `json:"user_id"`
	AnonymousID string    `json:"anonymous_id"`
	SessionID   string    `json:"session_id"`

	// Properties is the original properties map serialised to a JSON string
	// for storage in ClickHouse (String column with ZSTD codec).
	Properties string `json:"properties"`

	UABrowser  string    `json:"ua_browser"`
	UAOS       string    `json:"ua_os"`
	GeoCountry string    `json:"geo_country"`
	GeoCity    string    `json:"geo_city"`
	IngestedAt time.Time `json:"ingested_at"`
}

// DLQEvent is the payload written to the events.dlq topic when an event
// fails validation or cannot be parsed.
type DLQEvent struct {
	EventID   string          `json:"event_id,omitempty"`
	ProjectID string          `json:"project_id,omitempty"`
	Raw       json.RawMessage `json:"raw"`
	Reason    string          `json:"reason"`
	Error     string          `json:"error"`
	FailedAt  time.Time       `json:"failed_at"`
}
