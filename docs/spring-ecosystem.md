# Night ecosystem — Spring-inspired production architecture

> Status: **architecture and delivery specification**, not a declaration of implemented features.
> Night is an OW-native framework. Spring artifacts, Java annotations, and Spring Boot starters cannot be used directly as OW modules.

## Design principles

1. **OW first:** each public API is a typed OW module/class; a Java runtime extension is an implementation detail only when necessary.
2. **Composable:** independent starters, explicit dependencies, lifecycle, configuration, diagnostics, and no hidden global state.
3. **Secure defaults:** fail-closed authorization, parameterized SQL, redacted secrets, no implicit public administrative endpoints.
4. **Operationally testable:** every module needs unit, integration, failure-path and security tests plus production runbooks.
5. **Honest compatibility:** maintain a status of `implemented`, `partial`, or `planned`, never advertise Spring interoperability or production certification without evidence.
6. **Versioned contracts:** OW APIs, extension operation names, configuration properties, migrations and error models are semantically versioned.

## Ecosystem catalog

Status is relative to the currently checked-in Night 0.2 implementation. `Partial` means there is an initial capability, not feature parity.

| Domain | Night starter / module | Spring counterpart | Status | Production acceptance criteria |
|---|---|---|---|---|
| Boot | `night-boot` | Spring Boot | Partial (NightApp) | Profiles, validated config binding, startup/shutdown hooks, startup failure rollback |
| Core | `night-core` | Spring Framework Core / IoC | Partial | Scoped providers, dependency resolution, lifecycle and circular dependency validation |
| Modulith | `night-modulith` | Spring Modulith | Partial | Compile-time boundaries, event publication, verification, module-level diagnostics |
| Web | `night-web-mvc` | Spring MVC | Partial | Typed requests, status/errors, middleware ordering, body limits and routing tests |
| Reactive | `night-web-reactive` | Spring WebFlux | Planned | Asynchronous OW runtime, backpressure, cancellation and nonblocking HTTP |
| Client | `night-http-client` | RestClient / WebClient | Planned | Connection pooling, TLS, DNS/SSRF defense, retries with idempotency |
| Realtime | `night-websocket` | Spring WebSocket / STOMP | Planned | Authenticated upgrades, connection limits, heartbeat, graceful close |
| Security | `night-security` | Spring Security | Partial | Identity context, policies, method/route auth, audit, safe error handling |
| OAuth | `night-security-oauth2` | Spring Security OAuth2 / OIDC | Planned | JWKS rotation, issuer/audience/time checks, PKCE, safe refresh-token flow |
| Sessions | `night-session` | Spring Session | Planned | Secure cookie controls, CSRF when cookie auth is used, expiry and distributed store |
| SQL | `night-data-jdbc` | Spring JDBC / Spring Data JDBC | Partial | PostgreSQL integration, transactions with rollback, pooling, query timeouts |
| ORM | `night-data-orm` | Spring Data JPA / Hibernate | Planned | Entity mapping, identity and relation handling, pagination, N+1 analysis |
| DB migrations | `night-data-migrations` | Flyway / Liquibase | Partial (Flyway) | Rollback strategy, drift validation, concurrent deployment tests |
| Reactive SQL | `night-data-r2dbc` | Spring Data R2DBC | Planned | Async database driver and transaction context propagation |
| MongoDB | `night-data-mongodb` | Spring Data MongoDB | Planned | Typed collections, indexes, mapping, authentication/TLS |
| Redis | `night-data-redis` | Spring Data Redis | Planned | Pooled TLS connections, namespaces, TTL, distributed cache, pub/sub |
| Search | `night-data-search` | Spring Data Elasticsearch | Planned | Index schema, safe query encoding, bulk and retry handling |
| REST repos | `night-data-rest` | Spring Data REST | Planned | Explicit exposure policy, serialization guards, authorization per entity |
| Cache | `night-cache` | Spring Cache | Partial (local TTL) | Stampede handling, invalidation policy; Redis backend separate |
| Validation | `night-validation` | Spring Validation / Bean Validation | Partial | Structured errors, nested/object constraints, consistent HTTP 400 |
| Configuration | `night-config` | Spring Boot Config | Partial | Typed values, profiles, secret handling, validation and overrides |
| Cloud config | `night-config-server` | Spring Cloud Config | Planned | Signed/authorized config retrieval, versioning, fallback and rotation |
| Gateway | `night-gateway` | Spring Cloud Gateway | Planned | Route filters, auth propagation, rate limits, timeouts and safe proxying |
| Discovery | `night-discovery` | Spring Cloud Discovery | Planned | Registration, heartbeats, health-aware lookup and failure handling |
| Resilience | `night-resilience` | Spring Retry / Resilience4j | Planned | Timeout, bounded retry, circuit breaker, bulkhead and jitter |
| Messaging | `night-messaging` | Spring Messaging / Integration | Planned | Message envelopes, schemas, acknowledgements, dead letters and tracing |
| Kafka | `night-kafka` | Spring for Apache Kafka | Planned | Consumer groups, offset policy, idempotency, retry/DLQ |
| RabbitMQ | `night-amqp` | Spring AMQP | Planned | Durable queues, delivery guarantees, bounded retry, DLQ |
| Pulsar | `night-pulsar` | Spring for Apache Pulsar | Planned | Subscription lifecycle, partitioning, retry and schema evolution |
| Cloud stream | `night-stream` | Spring Cloud Stream | Planned | Broker abstraction with documented delivery semantics |
| Scheduling | `night-scheduling` | Spring Scheduling / Quartz | Planned | Persistent jobs, cancellation, lease, misfire and shutdown semantics |
| Batch | `night-batch` | Spring Batch | Planned | Checkpoints, idempotent restarts, retries, chunking and metadata |
| Mail | `night-mail` | Spring Mail | Planned | STARTTLS/TLS, templates, throttling and provider errors |
| GraphQL | `night-graphql` | Spring GraphQL | Planned | Schema validation, resolvers, query complexity and auth |
| AI | `night-ai` | Spring AI | Planned | Provider interface, model credentials, tool policies, embeddings, vector search |
| Actuator | `night-actuator` | Spring Boot Actuator | Partial | Separate liveness/readiness, metrics, protected diagnostics, graceful shutdown |
| Observability | `night-observability` | Micrometer / OpenTelemetry | Partial (HTTP metrics) | OpenTelemetry context propagation, tracing, histogram boundaries |
| OpenAPI | `night-openapi` | springdoc-openapi | Planned | Deterministic schema/docs, security scheme declarations |
| Testing | `night-test` | Spring Boot Test | Partial | Test fixtures, integration harness, security and containerized DB/broker tests |
| Native packaging | `night-native` | Spring AOT / Native | Planned | Target-specific build backend, repeatable builds, compatibility tests |

## Repository layout target

```text
src/
  modules/
    boot/ core/ modulith/ web/ security/ config/
    data/ cache/ validation/ actuator/
    observability/ messaging/ scheduling/ batch/
platform/
  core/          # shared extension contract
  security/      # OAuth2 and authorization providers
  data/          # JDBC, migrations, ORM and optional DB clients
  messaging/     # optional broker providers
  observability/ # optional OpenTelemetry exporter
examples/
  production/    # secured SQL baseline
  oauth-api/     # planned, real OIDC provider integration
  async-orders/  # planned, broker + outbox
```

The layout is a proposed migration; present-day `platform/` is a single Maven module. Avoid splitting it until independent starter artifacts and dependency boundaries are tested.

## Non-negotiable cross-cutting requirements

- **Configuration:** per-environment overrides, schema validation, no credentials or tokens committed to git; use a secret manager.
- **Security:** authenticate before controller invocation, explicit authorization, least privilege, consistent 401/403; SSRF and injection defenses.
- **Database:** prepared statements, bounded pools, timeouts, transaction rollback on failures, migration locking and restore drills.
- **Deployments:** reverse proxy/TLS, structured logs, graceful shutdown, probes, resource limits, protected admin endpoints.
- **Availability:** defined SLOs, representative load tests, backpressure, retry budgets, and dependency failure injection.
- **Supply chain:** pinned dependencies, SBOM, vulnerability scanning, license review, signed/reproducible CI artifacts.
- **Documentation:** runnable OW examples, operations/limitations table and a change log for each starter.

## Implementation sequence and release gates

### Phase 1 — Harden the existing foundation

- Night web: request-scoped context, structured errors and safe middleware ordering.
- Night security: principal context and comprehensive route-policy tests.
- Night JDBC: transaction cleanup guarantees, query timeouts and PostgreSQL Testcontainers integration.
- Actuator: consistent machine-readable health/readiness and protected diagnostics.
- Automated CI: OW compiler checks, Maven tests, actual HTTP tests and dependency scanning.

**Gate:** repeatable tests and a documented reverse-proxy deployment with failure recovery.

### Phase 2 — Common production services

- OAuth2/OIDC JWT resource server with validated signatures, algorithm allowlist, issuer, audience, JWKS rotation and expiry.
- Redis cache/session with TLS, bounded pool and namespace isolation.
- HTTP client with security boundaries, timeout defaults and tracing.
- OpenAPI and structured validation.

**Gate:** negative auth tests, credential rotation tests and integration environments.

### Phase 3 — Persistence and asynchronous workloads

- ORM/repositories and MongoDB plus search connectors.
- Kafka and AMQP with outbox/inbox, dead letters and exactly-described delivery semantics.
- Scheduling and batch with durable checkpoints.

**Gate:** crash/restart, duplicate delivery, DB migration and broker outage tests.

### Phase 4 — Distributed/cloud/advanced

- Gateway/config/discovery/resilience, WebSocket and GraphQL.
- Reactive OW primitives and R2DBC only once the interpreter supports real nonblocking execution.
- Night AI, full observability and native packaging.

**Gate:** distributed soak tests, tracing propagation, security audit, documented compatibility matrix.

## Compatibility and naming

- Use `NightSecurity`, `NightData`, `NightApp`, etc. as OW-visible APIs.
- Never imply that `org.springframework.*` is loadable from OW.
- Spring project names are references to design domains; implementations must be independently designed and licensed appropriately.
- Module completeness requires implementation + tests + examples + docs + CI, not only a class stub.

See [existing starter status](starters.md) and [production limitations](production.md).
