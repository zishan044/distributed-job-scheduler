-- +goose Up
CREATE TABLE job_batches (
  idempotency_key text PRIMARY KEY,
  request jsonb NOT NULL,
  job_ids uuid[] NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE job_batches;
