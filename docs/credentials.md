# Night credential storage (experimental)

Night's `NightCredentials` class provides a **database-backed password verifier**, bcrypt cost 12, login-failure lockouts, and expiring single-use reset tokens. It is not a user-facing login system or a complete Spring Security replacement.

## Installation

Run the standard Night platform JDBC setup and Flyway migrations, including `V3__night_credentials.sql`. Import `src/modules/security/NightCredentials.ow`.

```ow
set credentials: new NightCredentials()
call: credentials.register("user@example.com", "choose-a-strong-password")
set valid: credentials.verify("user@example.com", "choose-a-strong-password")
```

The API supports account identifiers 3–128 characters matching `[A-Za-z0-9._@-]`, password length at least 12 code units and at most 72 UTF-8 bytes (bcrypt's input boundary), and locks an account for 15 minutes after 5 failures. The failed-attempt counter persists across process restarts. Passwords are never stored in plaintext.

A privileged reset orchestration service can call `issue_reset(username)`, deliver the returned 256-bit token through a controlled channel, then call `complete_reset(username, token, new_password)` once, within 15 minutes. Only the token's SHA-256 digest is persisted. A successful reset clears the token and lockout state.

**Security-critical:** never expose `issue_reset()` directly in an unauthenticated HTTP response. The OW runtime method is a privileged primitive, not a completed reset workflow. A public reset request endpoint must always return a generic response whether the account exists, implement throttling and abuse prevention, queue out-of-band delivery to a verified address, and securely handle links/callbacks. Credential changes need auditing and session invalidation (currently not provided). No automatic account creation/permission binding to `NightSecurity` is implemented. Do not use returned `verify` alone as a long-lived authentication token.

## Missing before full production parity

- Argon2id support, password policy and compromised-password controls.
- Generic reset-request anti-enumeration endpoint, mail connector, reset token purpose/recipient binding and independent security testing.
- Explicit account recovery policy, MFA, session invalidation, lockout-abuse defense, audit trail and tenant isolation.
- Race/interleaving and resilience tests under a real production-like PostgreSQL cluster.

Tests are in `NightCredentialStoreTest` for DB-backed registration, lockout, expiry, token replay and credential-change behavior.
