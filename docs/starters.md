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
| `night-outbox` | Transactional outbox pattern | PostgreSQL leases, dead-letter state and explicit replay | No automatic outbox-to-broker dispatcher |
| `night-scheduler` | Spring Task / Quartz | PostgreSQL leased one-off/repeating jobs, manual ACK, retry and replay | No cron, supervised worker or automatic safe OW callbacks |
| `night-rabbitmq` | Spring AMQP | Durable queue publishing with confirmations, poll and manual ACK, TLS policy | No consumer supervision, broker retry policy or Kafka/Streams connector |
| `night-credentials` | Spring Security PasswordEncoder/UserDetails | JDBC-backed bcrypt passwords, 5-attempt lockout and one-time reset primitives | No public anti-enumeration reset service, session invalidation, MFA or completed IdP integration |
| `night-openapi` | springdoc-openapi | Explicit route metadata and OpenAPI 3.0 spec generation | No schema introspection, typed request/response contracts or GraphQL |
| `night-mail` | Spring Mail | SMTP STARTTLS/SMTPS with plain-text templates and audit table | No live SMTP integration/queue/bounce handling; handoff is not delivery |
| `night-validation` | Bean Validation | Primitive guards plus strict flat-object schema and unknown-field denial | No nested schemas, auto binding or Jakarta Bean Validation metadata |
| `night-test` | Spring Boot Test | Java unit tests, OW type preflight, live auth/SQL/HTTP tests | Integration coverage, not formal verification |

## Not implemented yet (explicitly planned, not placeholders)

| Future Night starter | Closest Spring project | Missing work |
|---|---|---|
| `night-security-oauth2` | Spring Security OAuth2 | OIDC discovery, authorization code+PKCE, session/cookie security, refresh tokens and real JWKS rotation testing |
| `night-data-orm` | Spring Data JPA / Hibernate | Typed entities, repositories, relationship mappings, lazy loading, query derivation |
| `night-data-redis-streams` | Spring Data Redis | Streams/pub-sub, consumer-group semantics, cross-node invalidation, cluster recovery |
| `night-messaging-full` | Spring AMQP / Kafka | Durable retry worker, consumer groups, Kafka, Redis Streams and dead-letter transport |
| `night-scheduling-full` | Spring Task / Quartz | Cron/calendar scheduling, safe OW worker callbacks, persistent misfire rules and distributed fencing |
| `night-mail-delivery` | Spring Mail | Durable retries, live provider TLS testing, bounce/delivery webhooks, template HTML escaping |
| `night-graphql` | Spring GraphQL | Query parser, validation, resolvers, auth boundaries |
| `night-realtime` | Spring WebSocket | WebSocket and SSE, back-pressure, authenticated subscriptions |
| `night-reactive` | WebFlux | Async OW runtime, structured cancellation, nonblocking transport |
| `night-http-client-full` | Spring WebClient | Async streaming/HTTP2, DNS pinning, egress SSRF controls, tracing and circuit breaker failure testing |
| `night-openapi-schema` | springdoc | Typed request/response schema introspection and automatic route discovery |
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
