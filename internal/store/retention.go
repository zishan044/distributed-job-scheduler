package store

import "context"

// PruneRuns removes terminal runs finished more than seven days ago, together
// with their attempts. Job definitions and idempotency records are retained.
// Each atomic batch skips locked runs so competing watchers can share the work.
func (s *Store) PruneRuns(ctx context.Context, limit int) (int64, error) {
	limit = min(max(limit, 1), 2000)
	tag, err := s.pool.Exec(ctx, `
		WITH expired AS (
			SELECT id FROM job_runs
			WHERE status IN ('SUCCEEDED','FAILED','CANCELLED','DEAD')
			AND finished_at < now() - interval '168 hours'
			ORDER BY finished_at, id LIMIT $1 FOR UPDATE SKIP LOCKED
		), removed_attempts AS (
			DELETE FROM run_attempts a USING expired e WHERE a.run_id=e.id
		)
		DELETE FROM job_runs r USING expired e WHERE r.id=e.id
	`, limit)
	return tag.RowsAffected(), err
}
