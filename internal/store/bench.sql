-- Benchmark-only read-only reconciliation; no scheduler API or schema changes.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '120s';
WITH scoped_jobs AS MATERIALIZED (
  SELECT id, name FROM jobs WHERE left(name, length(:'prefix')) = :'prefix'
), scoped_runs AS MATERIALIZED (
  SELECT r.*, j.name FROM job_runs r JOIN scoped_jobs j ON j.id = r.job_id
), scoped_attempts AS MATERIALIZED (
  SELECT a.* FROM run_attempts a JOIN scoped_runs r ON r.id = a.run_id
), statuses AS (
  SELECT status, count(*) AS n FROM scoped_runs GROUP BY status
)
SELECT jsonb_build_object(
  'jobs', (SELECT count(*) FROM scoped_jobs),
  'runs', (SELECT count(*) FROM scoped_runs),
  'jobs_with_runs', (SELECT count(DISTINCT job_id) FROM scoped_runs),
  'attempt_rows', (SELECT count(*) FROM scoped_attempts),
  'statuses', (SELECT coalesce(jsonb_object_agg(status, n), '{}'::jsonb) FROM statuses),
  'invalid_runs', (SELECT count(*) FROM scoped_runs WHERE
    attempt <> 1 OR scheduled_for <> fire_time OR cancel_requested OR retry_of IS NOT NULL OR last_error IS NOT NULL OR
    CASE WHEN :'scenario' = 'ingest' THEN
      status <> 'PENDING' OR worker_id IS NOT NULL OR queued_at IS NOT NULL OR started_at IS NOT NULL OR finished_at IS NOT NULL
    ELSE
      status <> 'SUCCEEDED' OR worker_id IS NULL OR queued_at IS NULL OR started_at IS NULL OR finished_at IS NULL OR
      queued_at < scheduled_for OR started_at < queued_at OR finished_at < started_at
    END),
  'invalid_attempts', (SELECT count(*) FROM scoped_runs r
    LEFT JOIN run_attempts a ON a.run_id = r.id AND a.attempt = 1
    WHERE :'scenario' <> 'ingest' AND (
      a.run_id IS NULL OR a.worker_id IS DISTINCT FROM r.worker_id OR
      a.started_at IS DISTINCT FROM r.started_at OR a.finished_at IS DISTINCT FROM r.finished_at OR
      a.outcome IS DISTINCT FROM 'SUCCEEDED' OR a.error IS NOT NULL
    )),
  'due_min', (SELECT extract(epoch FROM min(scheduled_for)) FROM scoped_runs),
  'due_max', (SELECT extract(epoch FROM max(scheduled_for)) FROM scoped_runs),
  'last_queued', (SELECT extract(epoch FROM max(queued_at)) FROM scoped_runs),
  'last_finished', (SELECT extract(epoch FROM max(finished_at)) FROM scoped_runs),
  'dispatches_in_window', (SELECT count(*) FROM scoped_runs WHERE
    queued_at >= to_timestamp(:'window_start'::double precision) AND queued_at <= to_timestamp(:'window_end'::double precision)),
  'successes_in_window', (SELECT count(*) FROM scoped_runs WHERE status = 'SUCCEEDED' AND
    finished_at >= to_timestamp(:'window_start'::double precision) AND finished_at <= to_timestamp(:'window_end'::double precision)),
  'start_latency_seconds', (SELECT jsonb_build_object(
    'count', count(*),
    'p50', percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - scheduled_for)),
    'p95', percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - scheduled_for)),
    'p99', percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - scheduled_for)),
    'max', max(extract(epoch FROM started_at - scheduled_for))
  ) FROM scoped_runs WHERE started_at IS NOT NULL AND left(name, length(:'prefix') + 8) = :'prefix' || 'measure/'),
  'postgres', (SELECT jsonb_object_agg(name, setting) FROM pg_settings WHERE
    name IN ('synchronous_commit', 'fsync', 'shared_buffers', 'max_wal_size', 'max_connections',
      'autovacuum_naptime', 'autovacuum_vacuum_scale_factor', 'autovacuum_analyze_scale_factor')),
  'tables', (SELECT jsonb_agg(jsonb_build_object('table', relname, 'inserts', n_tup_ins,
    'updates', n_tup_upd, 'hot_updates', n_tup_hot_upd, 'dead_tuples', n_dead_tup,
    'autovacuum_count', autovacuum_count)) FROM pg_stat_user_tables)
);
COMMIT;
