# Night 0.2 incremental production capabilities

This page documents implemented APIs, **not** full Spring Security or Spring Data parity.

## Optional per-process peer-IP quota

Set `NIGHT_IP_QUOTA_PER_MINUTE` to an integer from 1 to 1000000 before starting the process. Example:

```sh
export NIGHT_IP_QUOTA_PER_MINUTE=120
```

The extension performs bearer authentication first and then permits at most that many **authorized/public requests** per TCP peer IP per 60-second fixed window. The quota returns HTTP 429 with JSON `{"error":"rate_limited"}`. A bounded table of up to 100000 peer addresses is used; when full, new peers are rejected until an expired entry can be removed.

**Trust boundary:** `X-Forwarded-For`, `Forwarded`, and `X-Real-IP` are intentionally ignored. Behind a reverse proxy, the peer may be the proxy, so this is **not an end-user quota**. The proxy must enforce its own client-IP-aware rate limit and only trust forwarding headers from configured upstream proxies. This primitive does not provide distributed limits, request-cost accounting, or DoS protection. Roll out cautiously; a single-proxy deployment may share one bucket across all users.

## Minimal Night Data repository helpers

The `NightData` OW API adds:

```ow
set db: new NightData()
call: db.open()
set rows: db.page("purchases", "id", 50, 0)
set changed: db.update_versioned("purchases", "id", "quantity", 42, 3, 0)
```

- `page(table, order_column, limit, offset)` produces `SELECT * ... ORDER BY ... LIMIT ? OFFSET ?`, limits pages to 500 and validates schema identifiers.
- `update_versioned(table, id_column, value_column, id, value, expected_version)` uses a `version INTEGER NOT NULL` column and increments it atomically when the expected version matches. A result of zero is a not-found or optimistic-lock conflict; call sites must handle it.
- **Important:** Table and column names must originate in trusted source code/schema constants, **never** from request parameters. SQL identifiers cannot be parameter-bound. The helper rejects identifier punctuation and arbitrary order clauses; it does not implement schema authorization.
- It is a constrained JDBC helper, **not** a general-purpose ORM or typed repository compiler. Large offsets may be inefficient; keyset pagination remains future work.

## Validation status

Unit/integration test sources were added for quota windows, memory bounds, malicious identifiers, paging, version conflicts, and database isolation. GitHub Actions uses H2 and a separate PostgreSQL 16 job. End-to-end CI results and external production deployment tests have **not** been verified in this update.

## 1.0 remaining work

JWT/OIDC, credential/password workflows, ABAC, tenant isolation, distributed throttling, typed entity mapping, Redis, broker durability, distributed observability, load tests, disaster recovery drills, and independent security audit remain outstanding. See GitHub issue #3.
