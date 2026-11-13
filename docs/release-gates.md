# Night 1.0 production release gate checklist

**No release may claim 1.0 or production-certified until every required evidence field is populated and reviewed.** This template is intentionally mostly unchecked.

## Build and supply chain

- [ ] Source, compiler pin, Maven dependencies and container base digests are reproducible and reviewed
- [ ] CI unit, OW type-check, PostgreSQL, Redis and HTTP smoke jobs are green on the **release SHA**
- [ ] SBOM, dependency/secret/vulnerability scans, license report, and signed provenance are published
- [ ] Release binary/container signatures are verified by a separate deployment principal
- [ ] Upgrade and rollback from the preceding supported version succeed

## Security

- [ ] Authentication paths have negative issuer/audience/exp/JWKS/key-rotation tests
- [ ] Login/session cookies, OAuth2 code+PKCE, CSRF and logout are security reviewed
- [ ] Route and service RBAC/ABAC policies plus tenant boundaries have penetration coverage
- [ ] TLS proxy, CORS, trusted headers, per-client quotas and secret rotation pass edge tests
- [ ] Independent reviewer signs off security audit; important issues closed or risk-accepted

## Data, messaging and state

- [ ] PostgreSQL backup, point-in-time recovery, schema migrations and restore drill recorded
- [ ] Transaction rollback/isolation pass concurrency and failure-injection tests
- [ ] Redis keyspace isolation, expiration and failover tested with ACL/TLS configurations
- [ ] Broker delivery, retry, duplicate handling, replay and dead-letter procedures verified

## Operational readiness

- [ ] Multi-worker/async OW runtime supports request cancellation and graceful shutdown
- [ ] Compose, Kubernetes and systemd configurations have end-to-end deployment tests
- [ ] Prometheus exporter, OpenTelemetry spans and structured log correlation verified
- [ ] Metrics dashboard, alerts, SLO, runbooks and on-call ownership documented
- [ ] Representative load/soak/stress tests and restart/crash injection meet agreed thresholds
- [ ] Published compatibility matrix, versioned changelog and support/upgrade policy accepted

## Evidence record

| Gate | Evidence URL / artifact digest | Reviewer | Date | Outcome |
|---|---|---|---|---|
| CI on release SHA | Pending | Pending | Pending | OPEN |
| Security assessment | Pending | Pending | Pending | OPEN |
| Database restore | Pending | Pending | Pending | OPEN |
| Load/fault injection | Pending | Pending | Pending | OPEN |
| Deployment end-to-end | Pending | Pending | Pending | OPEN |

The GitHub roadmap in issue #3 is the authoritative feature/task checklist; this file defines the separate **release certification evidence**.
