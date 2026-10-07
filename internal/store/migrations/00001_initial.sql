-- +goose Up
CREATE TABLE jobs (
  id uuid PRIMARY KEY,
  name text NOT NULL,
  type text NOT NULL CHECK (type IN ('http','handler')),
  payload jsonb NOT NULL,
  cron_expr text,
  timezone text NOT NULL DEFAULT 'UTC',
  next_fire_at timestamptz,
  misfire_policy text NOT NULL DEFAULT 'fire_once',
  paused boolean NOT NULL DEFAULT false,
  max_attempts int NOT NULL DEFAULT 5,
  backoff_base_ms int NOT NULL DEFAULT 1000,
  backoff_max_ms int NOT NULL DEFAULT 300000,
  timeout_ms int NOT NULL DEFAULT 30000,
  idempotency_key text,
  created_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz
);

CREATE UNIQUE INDEX jobs_idem ON jobs(idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_cron_due ON jobs(next_fire_at)
  WHERE cron_expr IS NOT NULL AND NOT paused AND deleted_at IS NULL;

CREATE TABLE job_runs (
  id uuid PRIMARY KEY,
  job_id uuid NOT NULL REFERENCES jobs(id),
  fire_time timestamptz NOT NULL,
  scheduled_for timestamptz NOT NULL,
  status text NOT NULL CHECK (status IN
    ('PENDING','QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED','DEAD')),
  attempt int NOT NULL DEFAULT 1,
  worker_id uuid,
  queued_at timestamptz,
  started_at timestamptz,
  finished_at timestamptz,
  last_error text,
  cancel_requested boolean NOT NULL DEFAULT false,
  retry_of uuid,
  UNIQUE (job_id, fire_time)
) WITH (fillfactor = 70);

CREATE INDEX job_runs_due ON job_runs(scheduled_for) WHERE status = 'PENDING';
CREATE INDEX job_runs_running ON job_runs(worker_id) WHERE status = 'RUNNING';
CREATE INDEX job_runs_queued ON job_runs(queued_at) WHERE status = 'QUEUED';

CREATE TABLE run_attempts (
  run_id uuid,
  attempt int,
  worker_id uuid,
  started_at timestamptz,
  finished_at timestamptz,
  outcome text,
  error text,
  PRIMARY KEY (run_id, attempt)
);

CREATE TABLE workers (
  id uuid PRIMARY KEY,
  hostname text,
  started_at timestamptz,
  last_heartbeat timestamptz,
  lease_expires_at timestamptz,
  capacity int
);

-- +goose Down
DROP TABLE workers;
DROP TABLE run_attempts;
DROP TABLE job_runs;
DROP TABLE jobs;
