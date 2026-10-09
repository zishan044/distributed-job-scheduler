package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// Failure describes an execution error; the store owns retry scheduling.
type Failure struct {
	Error     string
	Permanent bool
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

	var cancelled bool
	var maxAttempts, baseMS, maxMS int
	err = tx.QueryRow(ctx, `
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

	status, outcome := "DEAD", "DEAD"
	if cancelled {
		status, outcome = "CANCELLED", "CANCELLED"
	} else if !failure.Permanent && attempt < maxAttempts {
		status, outcome = "PENDING", "FAILED"
	}
	// Retry settings are SQL int columns. Capping the shift at 31 bounds the
	// bigint product even for extreme attempt counts; larger shifts exceed the cap.
	run, err := scanRun(tx.QueryRow(ctx, `
		UPDATE job_runs SET
			status=$4,
			scheduled_for=CASE WHEN $4='PENDING' THEN now() +
				(random() * LEAST($5::bigint, $6::bigint *
				(1::bigint << LEAST(attempt-1, 31)))) * interval '1 millisecond'
				ELSE scheduled_for END,
			attempt=CASE WHEN $4='PENDING' THEN attempt+1 ELSE attempt END,
			worker_id=CASE WHEN $4='PENDING' THEN NULL ELSE worker_id END,
			queued_at=CASE WHEN $4='PENDING' THEN NULL ELSE queued_at END,
			started_at=CASE WHEN $4='PENDING' THEN NULL ELSE started_at END,
			finished_at=CASE WHEN $4='PENDING' THEN NULL ELSE now() END,
			last_error=$7
		WHERE id=$1 AND attempt=$2 AND worker_id=$3 AND status='RUNNING'
		RETURNING `+runColumns,
		runID, attempt, worker, status, maxMS, baseMS, failure.Error,
	))
	if err != nil {
		return Run{}, err
	}
	if err := finishAttempt(ctx, tx, runID, attempt, worker, outcome, failure.Error); err != nil {
		return Run{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Run{}, err
	}
	return run, nil
}
