package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// MaterializeCron inserts due fires and advances their job cursors atomically.
// The limit bounds both claimed definitions and generated runs; fire_all also
// allows at most 100 fires per job per pass. Competing watchers skip locked jobs.
func (s *Store) MaterializeCron(ctx context.Context, limit int) (int, error) {
	limit = min(max(limit, 1), 2000)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT `+jobColumns+` FROM jobs
		WHERE cron_expr IS NOT NULL AND NOT paused AND deleted_at IS NULL
		AND (next_fire_at <= now() OR next_fire_at IS NULL)
		ORDER BY next_fire_at NULLS FIRST, id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, job)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	created, remaining := 0, limit
	for _, job := range jobs {
		if remaining == 0 {
			break
		}
		fires, next, err := cronFires(job, now, remaining)
		if err != nil {
			return 0, fmt.Errorf("cron job %s: %w", job.ID, err)
		}
		jobID, err := parseUUID(job.ID)
		if err != nil {
			return 0, err
		}
		if len(fires) > 0 {
			args := make([]any, 0, len(fires)*5)
			for _, fire := range fires {
				id, err := uuid.NewV7()
				if err != nil {
					return 0, fmt.Errorf("generate cron run id: %w", err)
				}
				args = append(args, databaseUUID(id), jobID, fire, fire, "PENDING")
			}
			tag, err := tx.Exec(ctx, `INSERT INTO job_runs (id, job_id, fire_time, scheduled_for, status)
				VALUES `+insertValues(len(fires), 5)+` ON CONFLICT (job_id, fire_time) DO NOTHING`, args...)
			if err != nil {
				return 0, err
			}
			created += int(tag.RowsAffected())
		}
		if _, err := tx.Exec(ctx, `UPDATE jobs SET next_fire_at=$2 WHERE id=$1`, jobID, next); err != nil {
			return 0, err
		}
		remaining -= len(fires)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return created, nil
}

// cronFires computes against one database clock snapshot. Both fire_time and
// scheduled_for retain the original fire instant, including catch-up runs.
func cronFires(job Job, now time.Time, limit int) ([]time.Time, time.Time, error) {
	schedule, err := parseCron(job.CronExpr, job.Timezone)
	if err != nil {
		return nil, time.Time{}, err
	}
	if job.NextFireAt == nil {
		// Definitions created before M6 may not have an initialized cursor.
		if err := initializeCron(&job, now); err != nil {
			return nil, time.Time{}, err
		}
		return nil, *job.NextFireAt, nil
	}
	cursor := *job.NextFireAt
	if cursor.After(now) {
		return nil, cursor, nil
	}
	var fires []time.Time
	switch job.MisfirePolicy {
	case "fire_once":
		fires = append(fires, cursor)
		cursor = schedule.Next(now)
	case "skip":
		if now.Sub(cursor) <= 2*time.Second {
			fires = append(fires, cursor)
		}
		cursor = schedule.Next(now)
	case "fire_all":
		cutoff := now.Add(-24 * time.Hour)
		if cursor.Before(cutoff) {
			cursor = schedule.Next(cutoff.Add(-time.Nanosecond))
		}
		for !cursor.IsZero() && !cursor.After(now) && len(fires) < min(max(limit, 1), 100) {
			fires = append(fires, cursor)
			cursor = schedule.Next(cursor)
		}
	default:
		return nil, time.Time{}, fmt.Errorf("%w: unknown misfire_policy", ErrInvalidJob)
	}
	if cursor.IsZero() {
		return nil, time.Time{}, fmt.Errorf("%w: cron_expr has no next fire within five years", ErrInvalidJob)
	}
	return fires, cursor.UTC(), nil
}
