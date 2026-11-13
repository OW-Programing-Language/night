-- Audit records when the configured SMTP server accepts or rejects a send.
-- ACCEPTED means SMTP handoff, not end-user inbox delivery.
CREATE TABLE night_mail_audit (
  event_id VARCHAR(80) PRIMARY KEY,
  recipient_sha256 CHAR(64) NOT NULL,
  state VARCHAR(16) NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  CONSTRAINT night_mail_state_check CHECK (state IN ('PENDING', 'ACCEPTED', 'FAILED'))
);
