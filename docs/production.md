# Night platform: security + database + actuator

This guide describes the implementation, not a claim of full Spring parity.

## Build

Java 17+ and Maven:

```bash
git clone https://github.com/OW-Programing-Language/night.git
cd night
git clone https://github.com/OW-Programing-Language/ow-lang.git
mvn -f ow-lang/pom.xml install
mvn -f platform/pom.xml package
```

Run the secure SQL application:

```bash
# Generate *random* long bearer tokens; only their hashes go into env.
python3 - <<'PY'
import secrets, hashlib
for role in ("ADMIN", "READER", "WRITER"):
    token = secrets.token_urlsafe(48)
    print(role, "token (save securely):", token)
    print(role, "SHA256:", hashlib.sha256(token.encode()).hexdigest())
PY

export NIGHT_BEARER_KEYS='ADMIN:ADMIN_SHA256_HEX;READER:READER_SHA256_HEX;WRITER:WRITER_SHA256_HEX'
export NIGHT_JDBC_URL='jdbc:postgresql://127.0.0.1:5432/night'
export NIGHT_JDBC_USER='night_app'
export NIGHT_JDBC_PASSWORD='secret-from-your-manager'
export OW_NO_BROWSER=1

java -cp 'ow-lang/target/OwIDE.jar:platform/target/night-platform-0.2.0-SNAPSHOT.jar' \
  dev.oreo.Main run examples/production/main.ow
```

On Windows PowerShell use `; ` instead of `:` for the classpath separator,
and configure credentials through `$env:NIGHT_JDBC_URL` etc. Do not commit
plaintext tokens or database passwords. The application refuses to start if
`NIGHT_BEARER_KEYS` is missing or invalid. Never use a short human password
as a bearer token; the digests are plain SHA-256 of **high-entropy random keys**,
not a password hashing scheme.

The CI uses a temporary H2 database to test query and migration behavior.
The production driver is PostgreSQL; real production operations should use
TLS to PostgreSQL, least-privilege DB users, backup/restore drills, and separate
migration credentials where appropriate.

## Supported APIs

| Operation | HTTP path | Auth |
|---|---|---|
| Liveness | GET `/_night/health` | PUBLIC (explicit allowlist) |
| Readiness | GET `/_night/ready` | ADMIN |
| Counters | GET `/_night/metrics` | ADMIN |
| Module graph | GET `/_night/modules` | ADMIN |
| Catalog | GET `/api/catalog` | Authenticated |
| Purchases | GET `/api/purchases` | Authenticated |
| Purchase by ID | GET `/api/purchases/:id` | Authenticated |
| Create purchase | POST `/api/purchases/:item` | WRITER (ADMIN allowed) |

No Authorization header, invalid bearer token or duplicate headers: HTTP 401.
Authenticated principal without the required role: HTTP 403.
A successful authorized request only reaches its route *after* the filter.

```bash
curl http://localhost:8080/_night/health
curl -H "Authorization: Bearer $READER_TOKEN" http://localhost:8080/api/purchases
curl -H "Authorization: Bearer $WRITER_TOKEN" -X POST \
  http://localhost:8080/api/purchases/1 -d 2
```

## SQL and migrations

The `NightData` module uses HikariCP and Flyway. Every query binds
parameters with JDBC prepared statements:

```ow
set records: database.query(
  "SELECT id, item, quantity, total FROM purchases WHERE id = ?",
  [wanted]
)
```

Within a transaction:

```ow
call: database.begin()
# Call database.execute with prepared parameters.
# If every operation succeeds:
call: database.commit()
# On any error, the handler must explicitly database.rollback().
```

**Transaction limitation:** OW currently has no `finally` statement, so
application authors must wrap transactional operations in `try` / `catch`
and roll back explicitly; the extension rolls back orphaned transactions
on extension shutdown, not automatically after every failed request. Do not
start a transaction in a request without explicit failure handling.

The migration file is
`examples/production/db/migration/V1__create_purchases.sql`. Flyway tracks
the schema version and validates checksums. For PostgreSQL migrations, Night
includes the Flyway PostgreSQL database plugin.

## Transport security and deployment

OW's HTTP server is currently a single-worker interpreter with a 1 MiB
request-body limit and generic HTTP 500 responses. Deploy behind a trusted
reverse proxy (e.g. Caddy or nginx) for HTTPS, request/connection timeouts,
body limits, slow-client protection and IP rate limiting. Bind the OW backend
on a private interface; expose *only* the reverse proxy publicly.

Security headers: `X-Content-Type-Options: nosniff`,
`X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`,
`Cache-Control: no-store`. No browser cookies or Basic authentication are
used; these endpoints accept only Authorization: Bearer tokens. CORS is
disabled by default. Bearer credentials should be rotated and stored in a
secret manager. The framework does not implement OAuth2/JWT or multi-tenant
authorization yet.

## Limits and next milestones

Read [Starter module catalog](starters.md). A successful CI alone does not
make this implementation a fully production-grade Spring Boot replacement.
