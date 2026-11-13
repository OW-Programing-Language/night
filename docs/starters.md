# Night starter modules — Spring Boot-inspired ecosystem

Night uses native OW module types and an optional Java ServiceLoader platform
library. It is **not** Spring Boot and does not depend on Spring. The goal is to
provide useful, testable OW-native counterparts without pretending that Spring
or Java libraries run unchanged inside OW.

## Implemented in Night 0.2 (candidate)

| Night module | Inspiration | Implemented capability | Production caveat |
|---|---|---|---|
| `night-core` (`NightApp`) | Spring Modulith | Lifecycle, dependency graph, typed native functors, events, rollback | In-process, synchronous events |
| `night-web` (OW runtime) | Spring MVC | Real HTTP routes, JSON, method/param paths, response status, pre-controller filters | Single-worker server, reverse proxy required |
| `night-security` | Spring Security | Fail-closed hashed bearer tokens, experimental pinned RS256 JWT/JWKS resource server, route RBAC and security headers | OIDC login, sessions, token refresh, ABAC, IdP rotation tests and audit outstanding |
| `night-data` | Spring JDBC | HikariCP connection pool, JDBC prepared statements, PostgreSQL + H2 driver, explicit transactions | No ORM or automatic repository generation |
| `night-migrations` | Flyway | Versioned SQL files, checksum validation, migrate-on-startup | Migrations are operator-trusted SQL |
| `night-actuator` | Spring Boot Actuator | Readiness, health, low-cardinality HTTP metrics, existing module inspection | Metrics are per-process; protect admin endpoints |
| `night-config` | Spring Config | Environment-based configuration, required-variable checks | Secret rotation and hot reload not included |
| `night-cache` | Spring Cache | Bounded process-local TTL cache | Not a distributed Redis cache |
| `night-data-redis` | Spring Data Redis | Shared Redis string cache with TLS, TTL and namespace prefixes | No pub/sub, tenant isolation, Redis Streams or failover tests |
| `night-http-client` | RestClient / WebClient | Allowlisted outbound HTTPS GET, timeouts, body limits, non-following redirects, bounded retry | DNS rebinding not fully blocked; production egress gateway required |
| `night-outbox` | Transactional outbox pattern | PostgreSQL leases, dead-letter state and explicit replay | No broker worker or Kafka/RabbitMQ transport |
| `night-validation` | Bean Validation | Required and string-length guards | No full Jakarta Bean Validation metadata engine |
| `night-test` | Spring Boot Test | Java unit tests, OW type preflight, live auth/SQL/HTTP tests | Integration coverage, not formal verification |

## Not implemented yet (explicitly planned, not placeholders)

| Future Night starter | Closest Spring project | Missing work |
|---|---|---|
| `night-security-oauth2` | Spring Security OAuth2 | OIDC discovery, authorization code+PKCE, session/cookie security, refresh tokens and real JWKS rotation testing |
| `night-data-orm` | Spring Data JPA / Hibernate | Typed entities, repositories, relationship mappings, lazy loading, query derivation |
| `night-data-redis-streams` | Spring Data Redis | Streams/pub-sub, consumer-group semantics, cross-node invalidation, cluster recovery |
| `night-messaging` | Spring AMQP / Kafka | Durable retry/outbox, consumer groups, dead-letter handling |
| `night-scheduling` | Spring Task / Quartz | Scheduler persistence, safe OW callbacks, distributed lease management |
| `night-mail` | Spring Mail | SMTP provider, TLS, delivery tracing, template mailer |
| `night-graphql` | Spring GraphQL | Query parser, validation, resolvers, auth boundaries |
| `night-realtime` | Spring WebSocket | WebSocket and SSE, back-pressure, authenticated subscriptions |
| `night-reactive` | WebFlux | Async OW runtime, structured cancellation, nonblocking transport |
| `night-http-client-full` | Spring WebClient | Async streaming/HTTP2, DNS pinning, egress SSRF controls, tracing and circuit breaker failure testing |
| `night-openapi` | springdoc | Schema extraction and OpenAPI generation |
| `night-batch` | Spring Batch | Checkpointed jobs, retry, restartability |
| `night-observability-otel` | Micrometer / OpenTelemetry | Tracing, context propagation, collector exporter |
| `night-resilience` | Spring Retry / Resilience4j | Circuit breaking, bulkheads, timeout policies |
| `night-native` | GraalVM Native Image | Whole-program OW native backend and supported runtime extensions |

**Important:** Newly added modules are experimental and their CI outcomes must be verified on the release SHA. Production-ready is an operational/security claim requiring load
testing, dependency vulnerability analysis, audit, graceful shutdown,
connection/CPU limits, proper TLS termination, tracing and real deployment
validation. The 0.2 candidate implements a foundational set but is **not yet
certified for internet-facing production use**.

## Why the runtime SPI matters

Prior Night examples used OW's embedded HTTP server and a JSON file store.
The platform extension runs *before* OW controllers and can authorize or deny
requests even when a handler returns early. JDBC uses a pooled DataSource
rather than OW's JSON models. No credentials are baked into source; production
must supply them through a secret manager/environment.

See [Production guide](production.md).
