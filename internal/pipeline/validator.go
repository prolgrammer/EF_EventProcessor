package pipeline

import (
	"errors"
	"fmt"
	"time"

	"github.com/eventflow/event-processor/internal/model"
)

// validationError is a domain error type that carries a human-readable reason.
type validationError struct {
	field  string
	reason string
}

func (e *validationError) Error() string {
	return fmt.Sprintf("field %q: %s", e.field, e.reason)
}

// Validator validates the mandatory fields of a RawEvent.
// It is intentionally stateless and safe for concurrent use.
type Validator struct{}

// NewValidator creates a new Validator.
func NewValidator() *Validator {
	return &Validator{}
}

// Validate checks that all required fields are present and plausible.
// Returns a *validationError describing the first problem found, or nil.
func (v *Validator) Validate(evt *model.RawEvent) error {
	if evt == nil {
		return errors.New("event is nil")
	}

	if evt.EventID == "" {
		return &validationError{field: "event_id", reason: "required"}
	}
	if len(evt.EventID) > 128 {
		return &validationError{field: "event_id", reason: "exceeds 128 characters"}
	}

	if evt.ProjectID == "" {
		return &validationError{field: "project_id", reason: "required"}
	}

	if evt.EventName == "" {
		return &validationError{field: "event_name", reason: "required"}
	}
	if len(evt.EventName) > 256 {
		return &validationError{field: "event_name", reason: "exceeds 256 characters"}
	}

	if evt.Timestamp.IsZero() {
		return &validationError{field: "timestamp", reason: "required"}
	}
	// Reject events more than 48 h in the future or more than 30 days in the
	// past — they are almost certainly clock-skew or test pollution.
	now := time.Now().UTC()
	if evt.Timestamp.After(now.Add(48 * time.Hour)) {
		return &validationError{field: "timestamp", reason: "too far in the future"}
	}
	if evt.Timestamp.Before(now.Add(-30 * 24 * time.Hour)) {
		return &validationError{field: "timestamp", reason: "too far in the past"}
	}

	if evt.UserID == "" && evt.AnonymousID == "" {
		return &validationError{
			field:  "user_id / anonymous_id",
			reason: "at least one must be set",
		}
	}

	return nil
}
