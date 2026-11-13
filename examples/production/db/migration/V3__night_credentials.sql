-- Night password credential store; keep schema portable between PostgreSQL and H2.
-- Contains bcrypt password hashes and SHA-256 digests of high-entropy reset tokens.
-- Do not expose reset tokens, password hashes or lockout internals through HTTP.
CREATE TABLE night_credentials (
    username VARCHAR(128) PRIMARY KEY,
    password_hash VARCHAR(128) NOT NULL,
    failed_attempts INTEGER NOT NULL DEFAULT 0,
    locked_until BIGINT NOT NULL DEFAULT 0,
    reset_hash CHAR(64),
    reset_expires_at BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT night_credentials_failed_attempts_check CHECK (failed_attempts >= 0),
    CONSTRAINT night_credentials_locked_until_check CHECK (locked_until >= 0),
    CONSTRAINT night_credentials_reset_expires_check CHECK (reset_expires_at >= 0)
);
