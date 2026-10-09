package store

import (
	"context"
	"fmt"
	"time"
)

type RecoveredRun struct {
	Message DispatchMessage
	Run     Run
}

type SweepResult struct {
	Recovered []RecoveredRun
	Requeued  int64
}

// Sweep atomically recovers a bounded batch; callers publish DEAD results only
// after this method returns successfully. Other sweepers skip locked runs.
func (s *Store) Sweep(ctx context.Context, limit int, queuedTimeout time.Duration) (SweepResult, error) {
	if queuedTimeout <= 0 {
		return SweepResult{}, fmt.Errorf("queued timeout must be positive")
	}
	limit = min(max(limit, 1), 2000)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SweepResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT r.id::text, r.job_id::text, r.attempt, j.type, j.payload, j.timeout_ms,
			r.fire_time, r.worker_id::text
		FROM job_runs r JOIN jobs j ON j.id=r.job_id
		WHERE r.status='RUNNING' AND NOT EXISTS (
			SELECT 1 FROM workers w WHERE w.id=r.worker_id AND w.lease_expires_at > clock_timestamp()
		)
		ORDER BY r.id LIMIT $1 FOR UPDATE OF r SKIP LOCKED
	`, limit)
	if err != nil {
		return SweepResult{}, err
	}
	type candidate struct {
		message DispatchMessage
		worker  string
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		m := &item.message
		if err := rows.Scan(&m.RunID, &m.JobID, &m.Attempt, &m.Type, &m.Payload, &m.TimeoutMS, &m.FireTime, &item.worker); err != nil {
			rows.Close()
			return SweepResult{}, err
		}
		candidates = append(candidates, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return SweepResult{}, err
	}
	var result SweepResult
	for _, item := range candidates {
		id, err := parseUUID(item.message.RunID)
		if err != nil {
			return SweepResult{}, err
		}
		worker, err := parseUUID(item.worker)
		if err != nil {
			return SweepResult{}, err
		}
		run, err := failRun(ctx, tx, id, item.message.Attempt, worker, Failure{Error: "worker lease expired"}, true)
		if err != nil {
			return SweepResult{}, err
		}
		if run.ID != "" {
			result.Recovered = append(result.Recovered, RecoveredRun{Message: item.message, Run: run})
		}
	}
	tag, err := tx.Exec(ctx, `
		WITH stuck AS (
			SELECT id FROM job_runs WHERE status='QUEUED'
			AND queued_at <= clock_timestamp() - $1 * interval '1 second'
			ORDER BY queued_at, id LIMIT $2 FOR UPDATE SKIP LOCKED
		)
		UPDATE job_runs SET status='PENDING', queued_at=NULL
		WHERE id IN (SELECT id FROM stuck)
	`, queuedTimeout.Seconds(), limit)
	if err != nil {
		return SweepResult{}, err
	}
	result.Requeued = tag.RowsAffected()
	if err := tx.Commit(ctx); err != nil {
		return SweepResult{}, err
	}
	return result, nil
}
