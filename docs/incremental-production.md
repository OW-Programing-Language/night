# Night 0.2 incremental production capabilities

This page documents implemented APIs, **not** full Spring Security or Spring Data parity.

## Optional per-process IP quota with explicit trusted proxies

Set `NIGHT_IP_QUOTA_PER_MINUTE` to an integer from 1 to 1000000 before starting the process. Example:

```sh
export NIGHT_IP_QUOTA_PER_MINUTE=120
```

The extension counts **all requests, including invalid or missing authorization**, before expensive bearer/JWT verification and permits at most that many requests per selected client IP per 60-second fixed window. By default, the selected IP is **always the real TCP peer**. The quota returns HTTP 429 with JSON `{"error":"rate_limited"}`. A bounded table of up to 100000 peer addresses is used; when full, new peers are rejected until an expired entry can be removed.

**Trust boundary:** Optional `NIGHT_TRUSTED_PROXY_CIDRS` is a comma-separated list of numeric CIDRs for **explicitly controlled reverse proxies only** (for example `192.0.2.10/32,2001:db8:1::/64`). When set, Night inspects `X-Forwarded-For` **only** if the socket peer matches a trusted proxy. It walks the chain from right to left until reaching the first untrusted hop; a spoofed leftmost address cannot override that hop. `Forwarded` and `X-Real-IP` are always ignored. Invalid forwarding headers from trusted peers fail with HTTP 400, and `/0` trusted networks are rejected. IPv4/IPv6 literals are accepted; hostnames, port suffixes, repeated headers, and more than 16 hops are rejected.

Configure **only** your proxy's exact private addresses, and make sure the proxy overwrites or safely appends incoming `X-Forwarded-For`. Example:

```sh
export NIGHT_IP_QUOTA_PER_MINUTE=120
export NIGHT_TRUSTED_PROXY_CIDRS='10.20.0.5/32,fd00:1::5/128'
```

**Limitations:** This is still a **process-local** counter. It cannot enforce distributed quotas or mitigate DDoS. A proxy hop that is not on the trusted list intentionally becomes the quota identity; all clients behind it can share a bucket. Keep ingress proxy rate limits and TLS in place. Do not trust arbitrary public CIDRs or deploy Night directly exposed while trusting public clients.

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

## Prometheus counter exporter

`NightMetrics.prometheus()` generates Prometheus 0.0.4 text exposition with process-local totals for HTTP requests, 5xx responses, 401/403 responses, and aggregate request duration in seconds. This export intentionally has no unbounded labels.

The example `NightActuator.mount()` now registers `GET /_night/prometheus`; the production example requires the `ADMIN` role. A live smoke test checks 401/403/200 authorization and counter output. The response format and integration still require successful end-to-end CI validation.

This is a basic metrics exporter, **not** OpenTelemetry tracing, histograms, exemplar support, or distributed correlation. In a real Prometheus deployment protect scraping with private networking and dedicated scrape credentials; do not make the endpoint public.

## Validation status

Unit/integration test sources were added for quota windows, memory bounds, malicious identifiers, paging, version conflicts, and database isolation. GitHub Actions uses H2 and a separate PostgreSQL 16 job. End-to-end CI results and external production deployment tests have **not** been verified in this update.

## 1.0 remaining work

OIDC, account reset delivery, ABAC, tenant isolation, distributed throttling, typed entity mapping, Redis, broker durability, distributed observability, load tests, disaster recovery drills, and independent security audit remain outstanding. See GitHub issue #3.
