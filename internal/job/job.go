package job

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type State string

const (
	StatePending   State = "pending"
	StateScheduled State = "scheduled"
	StateClaimed   State = "claimed"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateDead      State = "dead"
)

// Priority constants — lower number = higher priority.
const (
	PriorityHigh   int16 = 1
	PriorityNormal int16 = 5
	PriorityLow    int16 = 10
)

type Job struct {
	ID             uuid.UUID
	TenantID       uuid.UUID
	Type           string
	Payload        json.RawMessage
	Priority       int16
	State          State
	RunAt          time.Time
	ClaimedAt      *time.Time
	ClaimedBy      *string
	ClaimToken     *uuid.UUID
	Deadline       *time.Time
	Attempt        int
	MaxRetries     int
	BackoffSeconds int
	IdempotencyKey *string
	LastError      *string
	CreatedAt      time.Time
	CompletedAt    *time.Time
}

func (j *Job) IsTerminal() bool {
	return j.State == StateSucceeded || j.State == StateDead
}

func (j *Job) ShouldRetry() bool {
	return j.Attempt < j.MaxRetries
}

// NextRetryAt calculates the next attempt time using exponential backoff with ±20% jitter.
// Pure function — no I/O.
func NextRetryAt(attempt, backoffSeconds int, now time.Time) time.Time {
	delay := backoffSeconds
	for i := 0; i < attempt && delay < 3600; i++ {
		delay *= 2
	}
	if delay > 3600 {
		delay = 3600
	}
	// Add ±20% jitter to spread out retries from many concurrent jobs.
	jitter := int(float64(delay) * 0.2)
	if jitter > 0 {
		delay += rand.Intn(2*jitter+1) - jitter
	}
	return now.Add(time.Duration(delay) * time.Second)
}

// WebhookPayload is the payload shape for type="webhook" jobs.
type WebhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}
