package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/zishan044/distributed-job-scheduler/pkg/api"
)

// CreateJobs atomically creates a batch and its initial runs. The bool reports
// an idempotent replay. Keys are scoped to this endpoint, independently of
// single-job create keys. Replays compare decoded requests using JSONB equality
// (object key order is ignored; array order is significant).
func (s *Store) CreateJobs(ctx context.Context, requests []api.CreateJobRequest, key string) ([]Job, bool, error) {
	if len(requests) < 1 || len(requests) > api.MaxBatchSize {
		return nil, false, fmt.Errorf("%w: batches must contain 1–100 jobs", ErrInvalidJob)
	}
	jobs := make([]Job, len(requests))
	for i, request := range requests {
		job, err := prepareJob(Job{
			Name: request.Name, Type: request.Type, Payload: request.Payload,
			CronExpr: request.CronExpr, Timezone: request.Timezone, NextFireAt: request.NextFireAt,
			MisfirePolicy: request.MisfirePolicy, Paused: request.Paused,
			MaxAttempts: request.MaxAttempts, BackoffBaseMS: request.BackoffBaseMS,
			BackoffMaxMS: request.BackoffMaxMS, TimeoutMS: request.TimeoutMS,
		}, request.ScheduledFor)
		if err != nil {
			return nil, false, fmt.Errorf("batch job %d: %w", i, err)
		}
		jobs[i] = job
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if key != "" {
		body, err := json.Marshal(requests)
		if err != nil {
			return nil, false, err
		}
		// The unique key serializes concurrent creates. The empty ID array is
		// filled before commit; any failure also rolls back this reservation.
		tag, err := tx.Exec(ctx, `INSERT INTO job_batches (idempotency_key, request, job_ids)
			VALUES ($1, $2::jsonb, '{}') ON CONFLICT (idempotency_key) DO NOTHING`, key, body)
		if err != nil {
			return nil, false, err
		}
		if tag.RowsAffected() == 0 {
			var same bool
			var ids []pgtype.UUID
			if err := tx.QueryRow(ctx, `SELECT request=$2::jsonb, job_ids FROM job_batches
				WHERE idempotency_key=$1`, key, body).Scan(&same, &ids); err != nil {
				return nil, false, err
			}
			if !same {
				return nil, false, ErrIdempotencyConflict
			}
			created, err := batchJobs(ctx, tx, ids)
			if err != nil {
				return nil, false, err
			}
			return created, true, tx.Commit(ctx)
		}
	}

	ids := make([]pgtype.UUID, len(jobs))
	jobArgs := make([]any, 0, len(jobs)*13)
	runArgs := make([]any, 0, len(jobs)*5)
	for i, job := range jobs {
		id, err := uuid.NewV7()
		if err != nil {
			return nil, false, fmt.Errorf("generate job id: %w", err)
		}
		ids[i] = databaseUUID(id)
		var cronExpr *string
		if job.CronExpr != "" {
			cronExpr = &job.CronExpr
		}
		jobArgs = append(jobArgs, ids[i], job.Name, job.Type, []byte(job.Payload), cronExpr,
			job.Timezone, job.NextFireAt, job.MisfirePolicy, job.Paused,
			job.MaxAttempts, job.BackoffBaseMS, job.BackoffMaxMS, job.TimeoutMS)
		if at := requests[i].ScheduledFor; at != nil {
			runID, err := uuid.NewV7()
			if err != nil {
				return nil, false, fmt.Errorf("generate run id: %w", err)
			}
			runArgs = append(runArgs, databaseUUID(runID), ids[i], *at, *at, api.StatusPending)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO jobs (id, name, type, payload, cron_expr, timezone,
		next_fire_at, misfire_policy, paused, max_attempts, backoff_base_ms, backoff_max_ms, timeout_ms)
		VALUES `+insertValues(len(jobs), 13), jobArgs...); err != nil {
		return nil, false, err
	}
	if len(runArgs) > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO job_runs (id, job_id, fire_time, scheduled_for, status)
			VALUES `+insertValues(len(runArgs)/5, 5), runArgs...); err != nil {
			return nil, false, err
		}
	}
	if key != "" {
		if _, err := tx.Exec(ctx, `UPDATE job_batches SET job_ids=$2 WHERE idempotency_key=$1`, key, ids); err != nil {
			return nil, false, err
		}
	}
	created, err := batchJobs(ctx, tx, ids)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	s.Metrics.JobsCreated.Add(float64(len(created)))
	return created, false, nil
}

func batchJobs(ctx context.Context, tx pgx.Tx, ids []pgtype.UUID) ([]Job, error) {
	rows, err := tx.Query(ctx, `SELECT `+jobColumns+` FROM jobs
		JOIN unnest($1::uuid[]) WITH ORDINALITY AS requested(job_id, position)
		ON jobs.id=requested.job_id ORDER BY requested.position`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]Job, 0, len(ids))
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// insertValues constructs parameter placeholders, never interpolated user data.
func insertValues(count, width int) string {
	rows := make([]string, count)
	for row := range count {
		columns := make([]string, width)
		for column := range width {
			columns[column] = fmt.Sprintf("$%d", row*width+column+1)
		}
		rows[row] = "(" + strings.Join(columns, ",") + ")"
	}
	return strings.Join(rows, ",")
}
