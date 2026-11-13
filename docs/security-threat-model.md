# Night 1.0 threat model and security review register

**Scope:** the OW HTTP runtime, Night pre-controller security extension, JWT/JWKS, JDBC/PostgreSQL, Redis, the transactional outbox, outbound HTTP, deployment proxy and administrator diagnostics. This is a working threat model, **not an independent penetration test or security certification**.

| Trust boundary / asset | Primary threat | Existing mitigation | Required validation before 1.0 |
|---|---|---|---|
| Public HTTP → OW interpreter | Request flooding, slow clients, oversized inputs | OW 1 MiB body limit, optional peer-IP quota, example Caddy timeouts | Reverse-proxy slow-client simulation, many-worker concurrency, trusted proxy setup |
| Bearer token → route authorization | Token theft, missing role, privilege escalation | Pre-controller denial; roles and hashed random API keys | Audit exact path matching, role resolution, session isolation, rotation tests |
| JWT → remote JWKS | Forged signatures, alg confusion, unknown kid, JWKS outage | Pinned RS256, configured HTTPS JWKS, issuer/audience/expiry checks | Real IdP key rollover and outage tests; replay/jti, claim policy review |
| Browser login → sessions | CSRF, session fixation, refresh token leaks | Not implemented | PKCE, SameSite, HttpOnly/Secure cookies, CSRF, logout, refresh rotation |
| Service → PostgreSQL | SQL injection, cross-tenant access, lost writes | Bound parameters, bounded JDBC pool, request-end rollback | Query/ORM fuzzing, tenant boundaries, real backup/restore tests |
| Service → Redis | TLS bypass, cross-application leakage, cache poisoning | rediss required except opt-in loopback, key namespace, TTL | ACLs, at-rest requirements, Redis failover, tenant-aware key policy |
| Application → outbox/brokers | Lost/duplicate/replayed events, unauthorized replay | PostgreSQL transaction, event ID, leases, dead-letter and manual replay | Worker idempotency, broker fault injection, admin replay authorization |
| Outbound HTTP → third party | SSRF, DNS rebinding, response bombing, retry storms | Exact DNS host allowlist, TLS, body/time bounds, no redirects | Network egress enforcement, DNS pinning/proxy, cert/redirect tests |
| Admin metrics/logs | Secret leak, high-cardinality exhaustion | Admin-protected route, low-cardinality counters | Independent endpoint review, log redaction, traces and correlation |
| CI/dependencies/images | Supply chain compromise, stale libraries | Declared versions, SBOM, Dependabot, image build check | Signed artifact provenance, secret/vulnerability scans, reproducibility |

## Audit prerequisites

1. Define target deployment and exposed endpoints; compile a route/access-policy inventory.
2. Establish real JWT issuer and JWKS configuration with a controlled test identity provider.
3. Capture independent tester scope, credentials and environment—not production customer secrets.
4. Test OW parser/input binding, CORS, HTTP smuggling, request cancellation and concurrency.
5. Fuzz authorization and tenant isolation; check all data and object references.
6. Validate JDBC failover, timeouts, rollback, migration locks and snapshot recovery.
7. Test rate-limit bypass under proxies, IPv6, multiple workers and deployments.
8. Inject broker outages, expired leases, duplicate delivery, dead-letter replay and delayed callbacks.
9. Verify container/Kubernetes/systemd hardening, TLS, DNS egress controls and secret handling.
10. Track findings with severity, reproducer, owner, patch commit, retest and signed assessor report.

## 1.0 exit rule

Do not mark the independent audit gate complete without an external assessor's report and verified closure or explicit risk acceptance for significant findings. Do not describe security tools or unit tests as a penetration test.
