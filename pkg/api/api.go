// Package api contains the scheduler's shared JSON request and response types.
// Scheduling defaults and validation are owned by the server.
package api

import (
	"encoding/json"
	"time"
)

// MaxBatchSize is the maximum number of jobs accepted in one create batch.
const MaxBatchSize = 100

// Run status values returned by the API.
const (
	StatusPending   = "PENDING"
	StatusQueued    = "QUEUED"
	StatusRunning   = "RUNNING"
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
	StatusCancelled = "CANCELLED"
	StatusDead      = "DEAD"
)

// CreateJobRequest describes a job and, for a one-shot job, its first run time.
// Cron jobs use five-field expressions and an IANA timezone (default UTC).
// NextFireAt overrides the first fire; otherwise the server computes it from
// database time. Cron execution requires the server's materializer.
type CreateJobRequest struct {
	Name          string          `json:"name"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	CronExpr      string          `json:"cron_expr"`
	Timezone      string          `json:"timezone"`
	NextFireAt    *time.Time      `json:"next_fire_at"`
	MisfirePolicy string          `json:"misfire_policy"`
	Paused        bool            `json:"paused"`
	MaxAttempts   int             `json:"max_attempts"`
	BackoffBaseMS int             `json:"backoff_base_ms"`
	BackoffMaxMS  int             `json:"backoff_max_ms"`
	TimeoutMS     int             `json:"timeout_ms"`
	ScheduledFor  *time.Time      `json:"scheduled_for"`
}

// CreateBatchRequest creates Jobs atomically. It must contain 1–100 jobs.
// POST /v1/jobs/batch returns []Job in the same order as Jobs.
type CreateBatchRequest struct {
	Jobs []CreateJobRequest `json:"jobs"`
}

// Job is a persisted job definition returned by the API.
type Job struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	CronExpr      string          `json:"cron_expr,omitempty"`
	Timezone      string          `json:"timezone"`
	NextFireAt    *time.Time      `json:"next_fire_at,omitempty"`
	MisfirePolicy string          `json:"misfire_policy"`
	Paused        bool            `json:"paused"`
	MaxAttempts   int             `json:"max_attempts"`
	BackoffBaseMS int             `json:"backoff_base_ms"`
	BackoffMaxMS  int             `json:"backoff_max_ms"`
	TimeoutMS     int             `json:"timeout_ms"`
	// IdempotencyKey is storage bookkeeping and is never transmitted in JSON.
	IdempotencyKey string     `json:"-"`
	CreatedAt      time.Time  `json:"created_at"`
	DeletedAt      *time.Time `json:"deleted_at,omitempty"`
}

// Run is one scheduled fire and its current execution attempt.
type Run struct {
	ID              string     `json:"id"`
	JobID           string     `json:"job_id"`
	FireTime        time.Time  `json:"fire_time"`
	ScheduledFor    time.Time  `json:"scheduled_for"`
	Status          string     `json:"status"`
	Attempt         int        `json:"attempt"`
	WorkerID        string     `json:"worker_id,omitempty"`
	QueuedAt        *time.Time `json:"queued_at,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	LastError       string     `json:"last_error,omitempty"`
	CancelRequested bool       `json:"cancel_requested"`
	RetryOf         string     `json:"retry_of,omitempty"`
}

// HTTPPayload describes an HTTP job. An empty Method defaults to GET.
type HTTPPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// ErrorResponse is the JSON error body returned by API endpoints.
type ErrorResponse struct {
	Error string `json:"error"`
}
