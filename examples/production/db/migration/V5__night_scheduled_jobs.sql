-- Durable PostgreSQL scheduler; execution remains application-controlled.
-- Worker claims are at-least-once and require idempotent job handlers.
CREATE TABLE night_scheduled_jobs (
  job_id VARCHAR(80) PRIMARY KEY,
  task_name VARCHAR(64) NOT NULL,
  payload TEXT NOT NULL,
  state VARCHAR(16) NOT NULL,
  next_run_at BIGINT NOT NULL,
  repeat_seconds INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner VARCHAR(128),
  lease_until BIGINT NOT NULL DEFAULT 0,
  CONSTRAINT night_scheduled_state_check
    CHECK (state IN ('READY', 'LEASED', 'COMPLETED', 'DEAD')),
  CONSTRAINT night_scheduled_repeat_check CHECK (repeat_seconds >= 0),
  CONSTRAINT night_scheduled_attempts_check CHECK (attempts >= 0)
);
CREATE INDEX night_scheduled_due_idx
  ON night_scheduled_jobs (state, next_run_at, job_id);
