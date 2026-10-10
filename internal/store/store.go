package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zishan044/distributed-job-scheduler/internal/metrics"
)

var (
	ErrNotFound          = errors.New("store: not found")
	ErrInvalidTransition = errors.New("store: invalid state transition")
	ErrInvalidJob        = errors.New("store: invalid job")
)

type Store struct {
	pool    *pgxpool.Pool
	Metrics *metrics.Metrics
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return &Store{pool: pool, Metrics: metrics.New()}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

type Job struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	CronExpr       string          `json:"cron_expr,omitempty"`
	Timezone       string          `json:"timezone"`
	NextFireAt     *time.Time      `json:"next_fire_at,omitempty"`
	MisfirePolicy  string          `json:"misfire_policy"`
	Paused         bool            `json:"paused"`
	MaxAttempts    int             `json:"max_attempts"`
	BackoffBaseMS  int             `json:"backoff_base_ms"`
	BackoffMaxMS   int             `json:"backoff_max_ms"`
	TimeoutMS      int             `json:"timeout_ms"`
	IdempotencyKey string          `json:"-"`
	CreatedAt      time.Time       `json:"created_at"`
	DeletedAt      *time.Time      `json:"deleted_at,omitempty"`
}

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

type DispatchMessage struct {
	RunID     string          `json:"run_id"`
	JobID     string          `json:"job_id"`
	Attempt   int             `json:"attempt"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	TimeoutMS int             `json:"timeout_ms"`
	FireTime  time.Time       `json:"fire_time"`
}

const jobColumns = `id::text, name, type, payload, COALESCE(cron_expr, ''), timezone,
	next_fire_at, misfire_policy, paused, max_attempts, backoff_base_ms,
	backoff_max_ms, timeout_ms, COALESCE(idempotency_key, ''), created_at, deleted_at`

const runColumns = `id::text, job_id::text, fire_time, scheduled_for, status, attempt,
	COALESCE(worker_id::text, ''), queued_at, started_at, finished_at,
	COALESCE(last_error, ''), cancel_requested, COALESCE(retry_of::text, '')`

type rowScanner interface {
	Scan(...any) error
}

func databaseUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: [16]byte(id), Valid: true}
}

func parseUUID(value string) (pgtype.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return pgtype.UUID{}, err
	}
	return databaseUUID(id), nil
}

func scanJob(row rowScanner) (Job, error) {
	var job Job
	var nextFireAt, deletedAt pgtype.Timestamptz
	err := row.Scan(
		&job.ID, &job.Name, &job.Type, &job.Payload, &job.CronExpr, &job.Timezone,
		&nextFireAt, &job.MisfirePolicy, &job.Paused, &job.MaxAttempts,
		&job.BackoffBaseMS, &job.BackoffMaxMS, &job.TimeoutMS,
		&job.IdempotencyKey, &job.CreatedAt, &deletedAt,
	)
	if err != nil {
		return Job{}, err
	}
	if nextFireAt.Valid {
		job.NextFireAt = &nextFireAt.Time
	}
	if deletedAt.Valid {
		job.DeletedAt = &deletedAt.Time
	}
	return job, nil
}

func scanRun(row rowScanner) (Run, error) {
	var run Run
	var queuedAt, startedAt, finishedAt pgtype.Timestamptz
	err := row.Scan(
		&run.ID, &run.JobID, &run.FireTime, &run.ScheduledFor, &run.Status,
		&run.Attempt, &run.WorkerID, &queuedAt, &startedAt, &finishedAt,
		&run.LastError, &run.CancelRequested, &run.RetryOf,
	)
	if err != nil {
		return Run{}, err
	}
	if queuedAt.Valid {
		run.QueuedAt = &queuedAt.Time
	}
	if startedAt.Valid {
		run.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		run.FinishedAt = &finishedAt.Time
	}
	return run, nil
}

func (s *Store) CreateJob(ctx context.Context, job Job, firstRunAt *time.Time) (Job, error) {
	if !json.Valid(job.Payload) || job.Name == "" || (job.Type != "http" && job.Type != "handler") {
		return Job{}, ErrInvalidJob
	}
	if (job.CronExpr == "") != (firstRunAt != nil) {
		return Job{}, fmt.Errorf("%w: one-shot jobs need a fire time and cron jobs must not have one", ErrInvalidJob)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return Job{}, fmt.Errorf("generate job id: %w", err)
	}
	if job.Timezone == "" {
		job.Timezone = "UTC"
	}
	if job.MisfirePolicy == "" {
		job.MisfirePolicy = "fire_once"
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = 5
	}
	if job.BackoffBaseMS == 0 {
		job.BackoffBaseMS = 1000
	}
	if job.BackoffMaxMS == 0 {
		job.BackoffMaxMS = 300000
	}
	if job.TimeoutMS == 0 {
		job.TimeoutMS = 30000
	}
	if job.MaxAttempts < 1 || job.BackoffBaseMS < 1 || job.BackoffMaxMS < job.BackoffBaseMS || job.TimeoutMS < 1 {
		return Job{}, ErrInvalidJob
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	created, err := scanJob(tx.QueryRow(ctx, `
		INSERT INTO jobs (
			id, name, type, payload, cron_expr, timezone, next_fire_at,
			misfire_policy, paused, max_attempts, backoff_base_ms,
			backoff_max_ms, timeout_ms, idempotency_key
		) VALUES (
			$1, $2, $3, $4, NULLIF($5, ''), $6, $7,
			$8, $9, $10, $11, $12, $13, NULLIF($14, '')
		)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING `+jobColumns,
		databaseUUID(id), job.Name, job.Type, []byte(job.Payload), job.CronExpr,
		job.Timezone, job.NextFireAt, job.MisfirePolicy, job.Paused,
		job.MaxAttempts, job.BackoffBaseMS, job.BackoffMaxMS, job.TimeoutMS,
		job.IdempotencyKey,
	))
	if errors.Is(err, pgx.ErrNoRows) && job.IdempotencyKey != "" {
		created, err = scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE idempotency_key = $1`, job.IdempotencyKey))
		if err != nil {
			return Job{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Job{}, err
		}
		return created, nil
	}
	if err != nil {
		return Job{}, err
	}

	if firstRunAt != nil {
		runID, err := uuid.NewV7()
		if err != nil {
			return Job{}, fmt.Errorf("generate run id: %w", err)
		}
		dbJobID, err := parseUUID(created.ID)
		if err != nil {
			return Job{}, err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO job_runs (id, job_id, fire_time, scheduled_for, status)
			VALUES ($1, $2, $3, $3, 'PENDING')
		`, databaseUUID(runID), dbJobID, *firstRunAt)
		if err != nil {
			return Job{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	s.Metrics.JobsCreated.Inc()
	return created, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	dbID, err := parseUUID(id)
	if err != nil {
		return Job{}, err
	}
	job, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, dbID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	dbID, err := parseUUID(id)
	if err != nil {
		return Run{}, err
	}
	run, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM job_runs WHERE id = $1`, dbID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return run, err
}

func (s *Store) ListJobRuns(ctx context.Context, jobID string, limit, offset int) ([]Run, error) {
	if limit < 1 || offset < 0 {
		return nil, fmt.Errorf("invalid run list limit or offset")
	}
	dbJobID, err := parseUUID(jobID)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+runColumns+` FROM job_runs
		WHERE job_id = $1
		ORDER BY fire_time DESC
		LIMIT $2 OFFSET $3
	`, dbJobID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	runs := make([]Run, 0)
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return runs, nil
}

func (s *Store) RunNow(ctx context.Context, jobID string) (Run, error) {
	dbJobID, err := parseUUID(jobID)
	if err != nil {
		return Run{}, err
	}
	runID, err := uuid.NewV7()
	if err != nil {
		return Run{}, fmt.Errorf("generate run id: %w", err)
	}
	run, err := scanRun(s.pool.QueryRow(ctx, `
		INSERT INTO job_runs (id, job_id, fire_time, scheduled_for, status)
		SELECT $1, id, now(), now(), 'PENDING'
		FROM jobs
		WHERE id = $2 AND deleted_at IS NULL
		RETURNING `+runColumns,
		databaseUUID(runID), dbJobID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		if _, getErr := s.GetJob(ctx, jobID); getErr != nil {
			return Run{}, getErr
		}
		return Run{}, ErrInvalidTransition
	}
	return run, err
}

func (s *Store) CancelRun(ctx context.Context, id string) (Run, error) {
	dbID, err := parseUUID(id)
	if err != nil {
		return Run{}, err
	}
	run, err := scanRun(s.pool.QueryRow(ctx, `
		UPDATE job_runs
		SET status = CASE WHEN status IN ('PENDING', 'QUEUED') THEN 'CANCELLED' ELSE status END,
		    finished_at = CASE WHEN status IN ('PENDING', 'QUEUED') THEN now() ELSE finished_at END,
		    cancel_requested = CASE WHEN status = 'RUNNING' THEN true ELSE cancel_requested END
		WHERE id = $1 AND status IN ('PENDING', 'QUEUED', 'RUNNING')
		RETURNING `+runColumns,
		dbID,
	))
	if err == nil {
		return run, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Run{}, err
	}

	run, err = s.GetRun(ctx, id)
	if err != nil {
		return Run{}, err
	}
	if run.Status == "CANCELLED" || (run.Status == "RUNNING" && run.CancelRequested) {
		return run, nil
	}
	return Run{}, ErrInvalidTransition
}

func (s *Store) QueueRun(ctx context.Context, id string) (bool, error) {
	dbID, err := parseUUID(id)
	if err != nil {
		return false, err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE job_runs SET status = 'QUEUED', queued_at = now()
		WHERE id = $1 AND status = 'PENDING' AND scheduled_for <= now()
	`, dbID)
	return tag.RowsAffected() == 1, err
}

// DispatchDue publishes a locked batch before marking it QUEUED and committing.
// A successful publish followed by a failed commit can produce a duplicate;
// StartRuns is the idempotent gate for that case.
func (s *Store) DispatchDue(ctx context.Context, limit int, publish func([]DispatchMessage) error) (int, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 2000 {
		limit = 2000
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT r.id::text, r.job_id::text, r.attempt, j.type, j.payload, j.timeout_ms, r.fire_time
		FROM job_runs r JOIN jobs j ON j.id = r.job_id
		WHERE r.status = 'PENDING' AND r.scheduled_for <= now()
		ORDER BY r.scheduled_for
		LIMIT $1 FOR UPDATE OF r SKIP LOCKED
	`, limit)
	if err != nil {
		return 0, err
	}
	messages := make([]DispatchMessage, 0, limit)
	for rows.Next() {
		var message DispatchMessage
		if err := rows.Scan(&message.RunID, &message.JobID, &message.Attempt, &message.Type, &message.Payload, &message.TimeoutMS, &message.FireTime); err != nil {
			rows.Close()
			return 0, err
		}
		messages = append(messages, message)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(messages) == 0 {
		return 0, tx.Commit(ctx)
	}
	if err := publish(messages); err != nil {
		return 0, err
	}
	latencies := make([]float64, 0, len(messages))
	for _, message := range messages {
		id, err := parseUUID(message.RunID)
		if err != nil {
			return 0, err
		}
		var latency float64
		if err := tx.QueryRow(ctx, `UPDATE job_runs SET status='QUEUED', queued_at=clock_timestamp()
			WHERE id=$1 AND status='PENDING'
			RETURNING EXTRACT(EPOCH FROM (queued_at-scheduled_for))::double precision`, id).Scan(&latency); err != nil {
			return 0, err
		}
		latencies = append(latencies, latency)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	s.Metrics.WatcherClaimed.Add(float64(len(messages)))
	for _, latency := range latencies {
		s.Metrics.DispatchLatency.Observe(max(0, latency))
	}
	return len(messages), nil
}

// StartRuns conditionally claims a batch and records attempt starts atomically.
func (s *Store) StartRuns(ctx context.Context, messages []DispatchMessage, workerID string) ([]DispatchMessage, error) {
	worker, err := parseUUID(workerID)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	ids := make([]pgtype.UUID, 0, len(messages))
	for _, message := range messages {
		id, err := parseUUID(message.RunID)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	// Kafka delivery can precede DispatchDue's commit. Wait for its row locks,
	// then evaluate QUEUED in the next statement's fresh read-committed snapshot.
	if _, err := tx.Exec(ctx, `SELECT id FROM job_runs WHERE id=ANY($1) ORDER BY id FOR UPDATE`, ids); err != nil {
		return nil, err
	}
	if alive, err := workerAlive(ctx, tx, worker); err != nil {
		return nil, err
	} else if !alive {
		return nil, ErrLeaseLost
	}
	batch := &pgx.Batch{}
	for i, message := range messages {
		batch.Queue(`UPDATE job_runs SET status='RUNNING', worker_id=$3, started_at=clock_timestamp()
			WHERE id=$1 AND attempt=$2 AND status='QUEUED'
			AND EXISTS (SELECT 1 FROM workers WHERE id=$3 AND lease_expires_at > clock_timestamp())
			RETURNING started_at, EXTRACT(EPOCH FROM (started_at-scheduled_for))::double precision`, ids[i], message.Attempt, worker)
	}
	results := tx.SendBatch(ctx, batch)
	type startedRun struct {
		message DispatchMessage
		at      time.Time
		latency float64
	}
	started := make([]startedRun, 0, len(messages))
	for _, message := range messages {
		item := startedRun{message: message}
		if err := results.QueryRow().Scan(&item.at, &item.latency); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			_ = results.Close()
			return nil, err
		}
		started = append(started, item)
	}
	if err := results.Close(); err != nil {
		return nil, err
	}
	claimed := make([]DispatchMessage, 0, len(started))
	for _, item := range started {
		runID, _ := parseUUID(item.message.RunID)
		if _, err := tx.Exec(ctx, `INSERT INTO run_attempts (run_id, attempt, worker_id, started_at) VALUES ($1,$2,$3,$4)`, runID, item.message.Attempt, worker, item.at); err != nil {
			return nil, err
		}
		claimed = append(claimed, item.message)
	}
	if alive, err := workerAlive(ctx, tx, worker); err != nil {
		return nil, err
	} else if !alive {
		return nil, ErrLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	for _, item := range started {
		s.Metrics.StartLatency.Observe(max(0, item.latency))
	}
	return claimed, nil
}

func (s *Store) StartRun(ctx context.Context, id string, attempt int, workerID string) (bool, error) {
	claimed, err := s.StartRuns(ctx, []DispatchMessage{{RunID: id, Attempt: attempt}}, workerID)
	return len(claimed) == 1, err
}

// SucceedRun finalizes an owned attempt. A committed cancellation wins over success.
func (s *Store) SucceedRun(ctx context.Context, id string, attempt int, workerID string) (bool, error) {
	dbID, err := parseUUID(id)
	if err != nil {
		return false, err
	}
	dbWorkerID, err := parseUUID(workerID)
	if err != nil {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var outcome string
	// Check the lease only after waiting for any concurrent run transition.
	if _, err := tx.Exec(ctx, `SELECT id FROM job_runs WHERE id=$1 FOR UPDATE`, dbID); err != nil {
		return false, err
	}
	err = tx.QueryRow(ctx, `
		UPDATE job_runs
		SET status = CASE WHEN cancel_requested THEN 'CANCELLED' ELSE 'SUCCEEDED' END,
		    finished_at = now()
		WHERE id = $1 AND attempt = $2 AND worker_id = $3 AND status = 'RUNNING'
		AND EXISTS (SELECT 1 FROM workers WHERE id=$3 AND lease_expires_at > clock_timestamp())
		RETURNING status
	`, dbID, attempt, dbWorkerID).Scan(&outcome)
	if errors.Is(err, pgx.ErrNoRows) {
		s.Metrics.StaleCompletions.Inc()
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := finishAttempt(ctx, tx, dbID, attempt, dbWorkerID, outcome, ""); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	s.recordOutcome(outcome)
	return true, nil
}

func finishAttempt(ctx context.Context, tx pgx.Tx, runID pgtype.UUID, attempt int, workerID pgtype.UUID, outcome, failure string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE run_attempts SET outcome=$4, error=NULLIF($5, ''), finished_at=now()
		WHERE run_id=$1 AND attempt=$2 AND worker_id=$3
	`, runID, attempt, workerID, outcome, failure)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("run attempt missing for attempt %d", attempt)
	}
	return nil
}
