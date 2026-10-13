-- +goose Up
CREATE INDEX job_runs_finished ON job_runs(finished_at, id)
  WHERE status IN ('SUCCEEDED','FAILED','CANCELLED','DEAD');

-- +goose Down
DROP INDEX job_runs_finished;
