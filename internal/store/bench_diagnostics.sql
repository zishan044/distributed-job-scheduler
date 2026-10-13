-- Benchmark observations only; EXPLAIN does not execute the dispatch query.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '5s';
SELECT jsonb_build_object(
  'time', clock_timestamp(),
  'activity', (SELECT jsonb_agg(jsonb_build_object('state', state,
    'wait_type', wait_event_type, 'wait', wait_event, 'query', left(query, 160)))
    FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()),
  'tables', (SELECT jsonb_agg(jsonb_build_object('table', relname,
    'live', n_live_tup, 'dead', n_dead_tup, 'updates', n_tup_upd,
    'hot_updates', n_tup_hot_upd, 'analyze', last_autoanalyze,
    'vacuum', last_autovacuum, 'analyze_count', autoanalyze_count,
    'vacuum_count', autovacuum_count, 'seq_scans', seq_scan, 'index_scans', idx_scan))
    FROM pg_stat_user_tables),
  'wal', (SELECT to_jsonb(w) FROM pg_stat_wal w),
  'checkpointer', (SELECT to_jsonb(c) FROM pg_stat_checkpointer c)
);
EXPLAIN SELECT r.id::text, r.job_id::text, r.attempt, j.type, j.payload, j.timeout_ms, r.fire_time
FROM job_runs r JOIN jobs j ON j.id=r.job_id
WHERE r.status='PENDING' AND r.scheduled_for<=now()
ORDER BY r.scheduled_for LIMIT :batch FOR UPDATE OF r SKIP LOCKED;
SELECT coalesce(array_agg(id), '{}'::uuid[])::text AS ids FROM (
  SELECT id FROM job_runs WHERE status='QUEUED' LIMIT 10
) q \gset
PREPARE bench_start(uuid[]) AS
  SELECT id FROM job_runs WHERE id=ANY($1) ORDER BY id FOR UPDATE;
SET LOCAL plan_cache_mode=force_custom_plan;
EXPLAIN EXECUTE bench_start(:'ids');
SET LOCAL plan_cache_mode=force_generic_plan;
EXPLAIN EXECUTE bench_start(:'ids');
DEALLOCATE bench_start;
SELECT 'Original executor point-update plan';
EXPLAIN (GENERIC_PLAN) UPDATE job_runs
SET status='RUNNING', worker_id=$3, started_at=clock_timestamp()
WHERE id=$1 AND attempt=$2 AND status='QUEUED'
AND EXISTS (SELECT 1 FROM workers WHERE id=$3 AND lease_expires_at>clock_timestamp())
RETURNING started_at, EXTRACT(EPOCH FROM (started_at-scheduled_for))::double precision;
SELECT 'Primary-key-first executor point-update plan';
EXPLAIN (GENERIC_PLAN) WITH candidate AS MATERIALIZED (
  SELECT id, attempt, status FROM job_runs WHERE id=$1 FOR UPDATE
)
UPDATE job_runs r SET status='RUNNING', worker_id=$3, started_at=clock_timestamp()
FROM candidate c WHERE r.id=c.id AND c.attempt=$2 AND c.status='QUEUED'
AND EXISTS (SELECT 1 FROM workers WHERE id=$3 AND lease_expires_at>clock_timestamp())
RETURNING r.started_at, EXTRACT(EPOCH FROM (r.started_at-r.scheduled_for))::double precision;
COMMIT;
