-- Night durable outbox, PostgreSQL implementation.
-- Apply in the same Flyway location as application migrations.
CREATE TABLE IF NOT EXISTS night_outbox (
  event_id VARCHAR(80) PRIMARY KEY,
  topic VARCHAR(128) NOT NULL,
  payload TEXT NOT NULL,
  state VARCHAR(16) NOT NULL DEFAULT 'PENDING',
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  lease_owner VARCHAR(128),
  lease_until TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  delivered_at TIMESTAMP WITH TIME ZONE,
  CONSTRAINT night_outbox_state_check CHECK (state IN ('PENDING', 'LEASED', 'DELIVERED', 'DEAD'))
);
CREATE INDEX IF NOT EXISTS night_outbox_available_idx ON night_outbox(state, available_at, created_at);
