-- Extend broker quarantine to distinguish malformed JSON from bodies that
-- parse but fail the configured per-topic Night field rules.
-- Works in PostgreSQL and H2; V7 is never modified after release.
ALTER TABLE night_broker_quarantine
  DROP CONSTRAINT night_quarantine_reason_check;

ALTER TABLE night_broker_quarantine
  ADD CONSTRAINT night_quarantine_reason_check
  CHECK (reason_code IN (
    'INVALID_EVENT_ID', 'INVALID_PAYLOAD', 'PAYLOAD_TOO_LARGE',
    'INVALID_JSON', 'SCHEMA_VIOLATION'
  ));
