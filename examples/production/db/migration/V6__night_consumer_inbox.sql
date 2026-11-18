-- Night durable consumer inbox (PostgreSQL, app-side processing).
-- Ingestion and processing are separate: broker ACK only after durable insert.
-- Stable (namespace, topic, event_id) enforces idempotent intake.
CREATE TABLE IF NOT EXISTS night_inbox (
  inbox_namespace VARCHAR(64) NOT NULL,
  topic VARCHAR(128) NOT NULL,
  event_id VARCHAR(80) NOT NULL,
  payload TEXT NOT NULL,
  state VARCHAR(16) NOT NULL DEFAULT 'PENDING',
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  lease_owner VARCHAR(128),
  lease_until TIMESTAMP WITH TIME ZONE,
  received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (inbox_namespace, topic, event_id),
  CONSTRAINT night_inbox_state_check CHECK (state IN ('PENDING', 'LEASED', 'COMPLETED', 'DEAD'))
);
CREATE INDEX IF NOT EXISTS night_inbox_pending_idx
  ON night_inbox(inbox_namespace, state, available_at, received_at);
