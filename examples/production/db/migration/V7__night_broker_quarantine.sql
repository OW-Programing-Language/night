-- Broker-level poison-event quarantine. This is distinct from application
-- processing DEAD state in night_inbox.
-- The full bounded message is retained for operator inspection/remediation.
CREATE TABLE IF NOT EXISTS night_broker_quarantine (
  inbox_namespace VARCHAR(64) NOT NULL,
  broker VARCHAR(16) NOT NULL,
  topic VARCHAR(128) NOT NULL,
  fingerprint CHAR(64) NOT NULL,
  reason_code VARCHAR(32) NOT NULL,
  original_event_id TEXT,
  payload TEXT,
  redeliveries INTEGER NOT NULL DEFAULT 1,
  first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (inbox_namespace, broker, topic, fingerprint),
  CONSTRAINT night_quarantine_broker_check CHECK (broker IN ('kafka', 'rabbitmq')),
  CONSTRAINT night_quarantine_reason_check CHECK
    (reason_code IN ('INVALID_EVENT_ID', 'INVALID_PAYLOAD', 'PAYLOAD_TOO_LARGE'))
);
CREATE INDEX IF NOT EXISTS night_quarantine_recent_idx
  ON night_broker_quarantine(inbox_namespace, last_seen DESC);
