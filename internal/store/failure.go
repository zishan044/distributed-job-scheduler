package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Failure describes an execution error; the store owns retry scheduling.
type Failure struct {
	Error      string
	Permanent  bool
	RetryAfter time.Duration
	RetryAt    time.Time
}

// FailRun records a fenced failure and returns the committed run. A zero Run
// means the attempt was no longer owned; callers must not publish it to the DLQ.
func (s *Store) FailRun(ctx context.Context, id string, attempt int, workerID string, failure Failure) (Run, error) {
	runID, err := parseUUID(id)
	if err != nil {
		return Run{}, err
	}
	worker, err := parseUUID(workerID)
	if err != nil {
		return Run{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	run, err := failRun(ctx, tx, runID, attempt, worker, failure, false)
	if err != nil {
		return run, err
	}
	if run.ID == "" {
		s.Metrics.StaleCompletions.Inc()
		return run, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return Run{}, err
	}
	s.recordOutcome(run.Status)
	return run, nil
}

// recordOutcome is called only after the enclosing transaction commits.
func (s *Store) recordOutcome(status string) {
	if status == "PENDING" {
		s.Metrics.Retries.Inc()
		status = "FAILED"
	}
	s.Metrics.Executions.WithLabelValues(strings.ToLower(status)).Inc()
}

// recovery requires an expired/missing worker; execution requires a live worker.
func failRun(ctx context.Context, tx pgx.Tx, runID pgtype.UUID, attempt int, worker pgtype.UUID, failure Failure, recovery bool) (Run, error) {
	var cancelled bool
	var maxAttempts, baseMS, maxMS int
	err := tx.QueryRow(ctx, `
		SELECT r.cancel_requested, j.max_attempts, j.backoff_base_ms, j.backoff_max_ms
		FROM job_runs r JOIN jobs j ON j.id=r.job_id
		WHERE r.id=$1 AND r.attempt=$2 AND r.worker_id=$3 AND r.status='RUNNING'
		FOR UPDATE OF r
	`, runID, attempt, worker).Scan(&cancelled, &maxAttempts, &baseMS, &maxMS)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, nil
	}
	if err != nil {
		return Run{}, err
	}
	if alive, err := workerAlive(ctx, tx, worker); err != nil {
		return Run{}, err
	} else if alive == recovery {
		return Run{}, nil
	}

	status, outcome := "DEAD", "DEAD"
	if cancelled {
		status, outcome = "CANCELLED", "CANCELLED"
	} else if !failure.Permanent && attempt < maxAttempts {
		status, outcome = "PENDING", "FAILED"
	}
	// Retry settings are SQL int columns. Capping the shift at 31 bounds the
	// bigint product even for extreme attempt counts; larger shifts exceed the cap.
	var retryAt *time.Time
	if !failure.RetryAt.IsZero() {
		retryAt = &failure.RetryAt
	}
	run, err := scanRun(tx.QueryRow(ctx, `
		UPDATE job_runs SET
			status=$4,
			scheduled_for=CASE WHEN $4='PENDING' THEN GREATEST(now() +
				(random() * LEAST($5::bigint, $6::bigint *
				(1::bigint << LEAST(attempt-1, 31)))) * interval '1 millisecond',
				CASE WHEN $9::double precision>0 THEN clock_timestamp() + $9 * interval '1 second' END,
				CASE WHEN $10::timestamptz>clock_timestamp() THEN $10 END)
				ELSE scheduled_for END,
			attempt=CASE WHEN $4='PENDING' THEN attempt+1 ELSE attempt END,
			worker_id=CASE WHEN $4='PENDING' THEN NULL ELSE worker_id END,
			queued_at=CASE WHEN $4='PENDING' THEN NULL ELSE queued_at END,
			started_at=CASE WHEN $4='PENDING' THEN NULL ELSE started_at END,
			finished_at=CASE WHEN $4='PENDING' THEN NULL ELSE now() END,
			last_error=$7
		WHERE id=$1 AND attempt=$2 AND worker_id=$3 AND status='RUNNING'
		AND (NOT EXISTS (SELECT 1 FROM workers WHERE id=$3 AND lease_expires_at > clock_timestamp()))=$8
		RETURNING `+runColumns,
		runID, attempt, worker, status, maxMS, baseMS, failure.Error, recovery, failure.RetryAfter.Seconds(), retryAt,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, nil
	}
	if err != nil {
		return Run{}, err
	}
	if err := finishAttempt(ctx, tx, runID, attempt, worker, outcome, failure.Error); err != nil {
		return Run{}, err
	}
	return run, nil
}
