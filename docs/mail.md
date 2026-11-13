# Night SMTP mail starter (experimental)

`NightMail` sends **plain-text** messages using authenticated SMTP over mandatory STARTTLS (port 587) or implicit TLS (port 465). It includes bounded named substitutions, recipient/header validation, a unique caller-supplied message ID, and a JDBC audit row for every attempted handoff. This is **not** an inbound mail platform, a marketing engine, or guaranteed inbox delivery.

## Configure

Set the following environment variables using a secrets manager:

```sh
NIGHT_SMTP_HOST=smtp.example.com
NIGHT_SMTP_PORT=587
NIGHT_SMTP_USERNAME=service-user
NIGHT_SMTP_PASSWORD='use-an-application-specific-secret'
NIGHT_SMTP_FROM=no-reply@example.com
```

The implementation enforces `mail.smtp.starttls.required=true` for port 587, `mail.smtp.ssl.enable=true` for 465, authenticated SMTP, hostname certificate validation, and connection/read/write timeouts. Ports other than 587/465 and plain SMTP are rejected.

Run Flyway `V4__night_mail_audit.sql` before sending. OW integration example:

```ow
import "../../src/modules/io/NightMail.ow"
set mail: new NightMail()
call: mail.send("welcome-00000001", "alice@example.com", "Welcome!",
  "Hello {{FIRST_NAME}}, thanks for signing up.",
  ["FIRST_NAME"], ["Alice"])
```

Variable values are substituted into **plain text**, not rendered as HTML. Templates and variable values are size-bounded and unresolved tokens cause failure. SMTP recipients and subjects reject CR/LF header injection.

## Delivery auditing and retry semantics

Each outbound message ID is unique and persists in `night_mail_audit`. State is `PENDING` before an attempt, `ACCEPTED` when the SMTP server accepts a handoff, and `FAILED` if the send operation raises a mail error. The stored recipient reference is a SHA-256 digest, not an email body.

**Important:** `ACCEPTED` does **not** mean delivered to the inbox. When the process crashes between SMTP acceptance and recording success, the row may remain `PENDING` even though the remote server accepted it. Operators must reconcile delivery status rather than blindly resending the same event; at-most-once send attempts and exactly-once delivery are not guaranteed. This minimal audit is not yet a full durable mail queue or complaint/bounce webhook system.

Never call the mail sender directly from an unauthenticated public endpoint. Enforce role authorization, rate limiting, recipient consent and verified addresses in application services. For password resets, use a generic anti-enumeration response and keep the secret token off public API responses. No live STARTTLS/SMTPS integration test or production provider certification exists yet.

## Remaining 1.0 conditions

- Real SMTP provider integration and TLS failure-injection tests, mail queue/worker integration, bounce/delivery webhooks and appropriate suppression list.
- Idempotent send orchestration and reconciliation after uncertain outcomes.
- Audit retention, access controls, privacy compliance and alerting.
