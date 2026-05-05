package pipeline

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/eventflow/event-processor/internal/model"
)

// sessionEntry tracks the current session for a user within a project.
type sessionEntry struct {
	sessionID string
	lastSeen  time.Time
}

// Enricher adds derived fields to a validated event:
//   - UA parsing (browser, OS)
//   - GeoIP lookup (country, city) — stubbed for dev, plug MaxMind here
//   - Session stitching (30-minute idle timeout)
//
// All session state is protected by a mutex; safe for concurrent use.
type Enricher struct {
	sessionsMu  sync.Mutex
	sessions    map[string]*sessionEntry // key: "project_id:user_key"
	idleTimeout time.Duration
}

// NewEnricher creates an Enricher and starts a background goroutine that
// periodically evicts expired session entries to bound memory usage.
func NewEnricher(idleTimeout time.Duration, done <-chan struct{}) *Enricher {
	e := &Enricher{
		sessions:    make(map[string]*sessionEntry),
		idleTimeout: idleTimeout,
	}
	go e.cleanup(done)
	return e
}

// Enrich produces an EnrichedEvent from a validated RawEvent.
func (e *Enricher) Enrich(raw *model.RawEvent) *model.EnrichedEvent {
	browser, os := parseUA(raw.Context.UA)
	country, city := geoLookup(raw.Context.IP)
	sessionID := e.stitchSession(raw.ProjectID, userKey(raw))
	propsJSON := serializeProperties(raw.Properties)

	return &model.EnrichedEvent{
		EventID:     raw.EventID,
		ProjectID:   raw.ProjectID,
		EventName:   raw.EventName,
		Timestamp:   raw.Timestamp,
		UserID:      raw.UserID,
		AnonymousID: raw.AnonymousID,
		SessionID:   sessionID,
		Properties:  propsJSON,
		UABrowser:   browser,
		UAOS:        os,
		GeoCountry:  country,
		GeoCity:     city,
		IngestedAt:  time.Now().UTC(),
	}
}

// SessionCacheSize returns the current number of tracked sessions.
func (e *Enricher) SessionCacheSize() int {
	e.sessionsMu.Lock()
	defer e.sessionsMu.Unlock()
	return len(e.sessions)
}

// stitchSession returns the current session ID for the user, creating a new
// session if the previous one has expired (idle timeout exceeded).
func (e *Enricher) stitchSession(projectID, key string) string {
	mapKey := projectID + ":" + key
	now := time.Now()

	e.sessionsMu.Lock()
	defer e.sessionsMu.Unlock()

	entry, ok := e.sessions[mapKey]
	if !ok || now.Sub(entry.lastSeen) > e.idleTimeout {
		entry = &sessionEntry{
			sessionID: uuid.New().String(),
			lastSeen:  now,
		}
		e.sessions[mapKey] = entry
	} else {
		entry.lastSeen = now
	}

	return entry.sessionID
}

// cleanup removes expired session entries every 5 minutes.
func (e *Enricher) cleanup(done <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			e.evictExpired()
		}
	}
}

func (e *Enricher) evictExpired() {
	now := time.Now()
	e.sessionsMu.Lock()
	defer e.sessionsMu.Unlock()

	for k, v := range e.sessions {
		if now.Sub(v.lastSeen) > e.idleTimeout {
			delete(e.sessions, k)
		}
	}
}

// userKey returns the canonical user identifier used as the session-stitching
// key: prefers user_id, falls back to anonymous_id.
func userKey(evt *model.RawEvent) string {
	if evt.UserID != "" {
		return evt.UserID
	}
	return evt.AnonymousID
}

// serializeProperties converts the raw properties map to a JSON string for
// ClickHouse storage.  Returns "{}" on nil or marshal error.
func serializeProperties(props map[string]interface{}) string {
	if props == nil {
		return "{}"
	}
	b, err := json.Marshal(props)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// ─── User-Agent parser ────────────────────────────────────────────────────────

// parseUA extracts browser and OS from a User-Agent string.
// This is a best-effort implementation; replace with a proper UA library
// (e.g. ua-parser/uap-go) for higher accuracy in production.
func parseUA(ua string) (browser, os string) {
	lower := strings.ToLower(ua)

	// Browser — order matters: Edge/OPR are Chromium-based so check them first.
	switch {
	case strings.Contains(lower, "edg/") || strings.Contains(lower, "edge/"):
		browser = "Edge"
	case strings.Contains(lower, "opr/") || strings.Contains(lower, "opera"):
		browser = "Opera"
	case strings.Contains(lower, "chrome"):
		browser = "Chrome"
	case strings.Contains(lower, "firefox"):
		browser = "Firefox"
	case strings.Contains(lower, "safari"):
		browser = "Safari"
	case strings.Contains(lower, "curl"):
		browser = "curl"
	default:
		browser = "Unknown"
	}

	// OS — check mobile platforms first.
	switch {
	case strings.Contains(lower, "android"):
		os = "Android"
	case strings.Contains(lower, "iphone") || strings.Contains(lower, "ipad") || strings.Contains(lower, "ipod"):
		os = "iOS"
	case strings.Contains(lower, "windows"):
		os = "Windows"
	case strings.Contains(lower, "mac os") || strings.Contains(lower, "macos"):
		os = "macOS"
	case strings.Contains(lower, "linux"):
		os = "Linux"
	default:
		os = "Unknown"
	}

	return
}

// ─── GeoIP stub ──────────────────────────────────────────────────────────────

// geoLookup returns the country and city for an IP address.
// This is a stub implementation. In production, use a MaxMind GeoLite2 or
// GeoIP2 database (github.com/oschwald/maxminddb-golang).
// Per the spec: "if geo-IP is unavailable → country=unknown; event is NOT dropped."
func geoLookup(ip string) (country, city string) {
	if ip == "" {
		return "unknown", "unknown"
	}
	// Recognise obvious private/loopback ranges — no external lookup needed.
	switch {
	case strings.HasPrefix(ip, "127.") ||
		strings.HasPrefix(ip, "::1") ||
		strings.HasPrefix(ip, "10.") ||
		strings.HasPrefix(ip, "192.168.") ||
		strings.HasPrefix(ip, "172."):
		return "local", "local"
	}
	// TODO: replace with real MaxMind lookup.
	return "unknown", "unknown"
}
