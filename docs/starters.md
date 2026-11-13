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
| `night-security` | Spring Security | Fail-closed Bearer SHA-256 token authentication, role-based route access rules, security headers | No OAuth2, identity provider, account/session store or browser-login CSRF flow |
| `night-data` | Spring JDBC | HikariCP connection pool, JDBC prepared statements, PostgreSQL + H2 driver, explicit transactions | No ORM or automatic repository generation |
| `night-migrations` | Flyway | Versioned SQL files, checksum validation, migrate-on-startup | Migrations are operator-trusted SQL |
| `night-actuator` | Spring Boot Actuator | Readiness, health, low-cardinality HTTP metrics, existing module inspection | Metrics are per-process; protect admin endpoints |
| `night-config` | Spring Config | Environment-based configuration, required-variable checks | Secret rotation and hot reload not included |
| `night-cache` | Spring Cache | Bounded process-local TTL cache | Not a distributed Redis cache |
| `night-validation` | Bean Validation | Required and string-length guards | No full Jakarta Bean Validation metadata engine |
| `night-test` | Spring Boot Test | Java unit tests, OW type preflight, live auth/SQL/HTTP tests | Integration coverage, not formal verification |

## Not implemented yet (explicitly planned, not placeholders)

| Future Night starter | Closest Spring project | Missing work |
|---|---|---|
| `night-security-oauth2` | Spring Security OAuth2 | OIDC discovery, JWT signature and claim verification, JWKS rotation, sessions, refresh tokens |
| `night-data-orm` | Spring Data JPA / Hibernate | Typed entities, repositories, relationship mappings, lazy loading, query derivation |
| `night-data-redis` | Spring Data Redis | Redis client, distributed cache, pub/sub, TTL |
| `night-messaging` | Spring AMQP / Kafka | Durable retry/outbox, consumer groups, dead-letter handling |
| `night-scheduling` | Spring Task / Quartz | Scheduler persistence, safe OW callbacks, distributed lease management |
| `night-mail` | Spring Mail | SMTP provider, TLS, delivery tracing, template mailer |
| `night-graphql` | Spring GraphQL | Query parser, validation, resolvers, auth boundaries |
| `night-realtime` | Spring WebSocket | WebSocket and SSE, back-pressure, authenticated subscriptions |
| `night-reactive` | WebFlux | Async OW runtime, structured cancellation, nonblocking transport |
| `night-rest-client` | Spring WebClient | Pooled outbound HTTP, circuit breakers, DNS and SSRF protections |
| `night-openapi` | springdoc | Schema extraction and OpenAPI generation |
| `night-batch` | Spring Batch | Checkpointed jobs, retry, restartability |
| `night-observability-otel` | Micrometer / OpenTelemetry | Tracing, context propagation, collector exporter |
| `night-resilience` | Spring Retry / Resilience4j | Circuit breaking, bulkheads, timeout policies |
| `night-native` | GraalVM Native Image | Whole-program OW native backend and supported runtime extensions |

**Important:** production-ready is an operational/security claim requiring load
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
