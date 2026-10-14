//go:build chaos

package store

import (
	"context"
	"time"
)

// ChaosAttempt is persisted evidence, available only to the chaos test binary.
type ChaosAttempt struct {
	RunID      string     `json:"run_id"`
	Attempt    int        `json:"attempt"`
	WorkerID   string     `json:"worker_id"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	Outcome    string     `json:"outcome"`
	Error      string     `json:"error"`
}

type ChaosWorker struct {
	ID             string    `json:"id"`
	Hostname       string    `json:"hostname"`
	LastHeartbeat  time.Time `json:"last_heartbeat"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	Live           bool      `json:"live"`
}

func (s *Store) ChaosWorkers(ctx context.Context) ([]ChaosWorker, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text, hostname, last_heartbeat,
		lease_expires_at, lease_expires_at > clock_timestamp() FROM workers ORDER BY started_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	workers := make([]ChaosWorker, 0)
	for rows.Next() {
		var worker ChaosWorker
		if err := rows.Scan(&worker.ID, &worker.Hostname, &worker.LastHeartbeat, &worker.LeaseExpiresAt, &worker.Live); err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}
	return workers, rows.Err()
}

func (s *Store) ChaosAttempts(ctx context.Context, runID string) ([]ChaosAttempt, error) {
	id, err := parseUUID(runID)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT run_id::text, attempt, worker_id::text,
		started_at, finished_at, COALESCE(outcome, ''), COALESCE(error, '')
		FROM run_attempts WHERE run_id=$1 ORDER BY attempt`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attempts := make([]ChaosAttempt, 0)
	for rows.Next() {
		var attempt ChaosAttempt
		if err := rows.Scan(&attempt.RunID, &attempt.Attempt, &attempt.WorkerID,
			&attempt.StartedAt, &attempt.FinishedAt, &attempt.Outcome, &attempt.Error); err != nil {
			return nil, err
		}
		attempts = append(attempts, attempt)
	}
	return attempts, rows.Err()
}
