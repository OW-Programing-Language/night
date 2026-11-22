# Night 🌙

**The OW-native modular web framework, inspired by Spring Modulith and the Spring Boot ecosystem.**

Night adds typed business modules and functors, HTTP routing, database access,
security, metrics, and externalized configuration to [OW](https://github.com/OW-Programing-Language/ow-lang).
It does **not** run Spring Boot or require Java/Spring APIs in OW source.

## Night starters

| Starter | Core functionality | Source |
|---|---|---|
| Night Core | Native module/functor lifecycle, dependency checks, rollback, events | `src/NightApp.ow` |
| Night Web | Built-in OW HTTP and REST route handlers | OW runtime + Night module controllers |
| Night Security | Bearer/JWT auth, RBAC, secure headers, experimental bcrypt credential store | `src/modules/security/` |
| Night Data | JDBC, HikariCP, PostgreSQL/H2, Flyway, transactions, pagination, optimistic updates, PostgreSQL outbox | `src/modules/data/` + `platform/` |
| Night Actuator | Health, readiness, HTTP metrics, module inspection | `src/modules/actuator/` |
| Night Config | Environment configuration | `src/modules/config/` |
| Night Cache | Local TTL cache | `src/modules/cache/` |
| Night Redis | Shared string cache, TTL and key prefix isolation (TLS Redis) | `src/modules/data/NightRedis.ow` + `platform/` |
| Night HTTP | Allowlisted outbound HTTPS GET, bounded response and circuit breaker | `src/modules/http/` + `platform/` |
| Night OpenAPI | Explicit route metadata registry and generated OpenAPI 3.0 document | `src/modules/web/NightOpenApi.ow` + `platform/` |
| Night Mail | TLS-only SMTP text templates and JDBC handoff auditing | `src/modules/io/NightMail.ow` + `platform/` |
| Night Scheduler | PostgreSQL one-shot/repeating jobs, leases, retry and dead-letter replay | `src/modules/jobs/NightScheduler.ow` + `platform/` |
| Night RabbitMQ | TLS-only durable queue adapter, publisher confirms, manual ACKs | `src/modules/messaging/NightRabbitMQ.ow` + `platform/` |
| Night Redis Streams | At-least-once producer, consumer groups, manual ACK, pending recovery | `src/modules/messaging/NightRedisStreams.ow` + `platform/` |
| Night Kafka | TLS-enabled producer, consumer groups, explicit offset ACK and restart redelivery | `src/modules/messaging/NightKafka.ow` + `platform/` |
| Night Inbox | Supervised Kafka/RabbitMQ → PostgreSQL intake, deduplication, application retries and dead letters | `src/modules/messaging/NightInbox.ow` + `platform/` |
| Night Broker Quarantine | PostgreSQL-backed malformed-envelope capture before Kafka/RabbitMQ ACK | `platform/` + `docs/broker-quarantine.md` |
| Night Message Schemas | Opt-in per-topic JSON rules, malformed JSON/schema quarantine before ACK | `platform/` + `docs/message-schemas.md` |
| Night Outbox Dispatcher | Separate worker for PostgreSQL outbox → Kafka or RabbitMQ, retries, dead letters | `platform/.../NightOutboxWorker.java` + `docs/outbox-dispatcher.md` |
| Night Validation | Input guards | `src/modules/validation/` |
| Night Test | Compiler checks, lifecycle tests, secured HTTP+SQL tests | `tests/` + GitHub Actions |

See the [full starter ecosystem matrix](docs/starters.md) for what is
implemented versus planned (OAuth2, ORM, Kafka durable workers, mail, GraphQL, etc.).

## Quickstart — two example applications

### 1. Original modular Bookstore (no extra platform)

```sh
git clone https://github.com/OW-Programing-Language/night.git
cd night
git clone https://github.com/OW-Programing-Language/ow-lang.git
mvn -f ow-lang/pom.xml package
java -jar ow-lang/target/OwIDE.jar run examples/bookstore/main.ow
```

### 2. Secured SQL Bookstore (Night Platform starter)

```sh
mvn -f ow-lang/pom.xml install
mvn -f platform/pom.xml package
# Set NIGHT_BEARER_KEYS and NIGHT_JDBC_* credentials (see production.md)
java -cp 'ow-lang/target/OwIDE.jar:platform/target/night-platform-0.2.0-SNAPSHOT.jar' \
  dev.oreo.Main run examples/production/main.ow
```

`NightSecurity.enable()` fails closed if authentication credentials are
missing. Every route is authenticated by default except explicitly public
routes. Writes require the WRITER role; metrics and readiness require ADMIN.

## OW code example

```ow
main:
  set security: new NightSecurity()
  call: security.role("POST /api/purchases/:item", "WRITER")
  call: security.enable()

  set database: new NightData()
  call: database.open()
  call: database.migrate("filesystem:examples/production/db/migration")

  set purchases PURCHASES: MakePurchases(Production.Catalog)
  set night: new NightApp()
  call: night.use_native_as(purchases, "PURCHASES")
  call: night.use_native_as(Production.Catalog, "PROD_CATALOG")
  call: assert(night.boot() == 1)
  serve: 8080
```

`MakePurchases` is a real OW functor consuming `PROD_CATALOG`, and
`NightApp` uses that type-checked module in a dependency-aware application.
The Night Java platform is optional and installed via OW's
`RuntimeExtension` ServiceLoader SPI. It provides real pre-controller
security and JDBC rather than unsecured application-level helper stubs.

## Native module auto-discovery (experimental)

Night can now discover imported public OW modules and main-scope functor
instances by their `NIGHT_COMPONENT` signature. Call
`night.discover_native()` **before** `night.boot()`, rather than registering
each native component manually. Explicit registration remains supported.

`module_scan()` is introduced by the linked
[OW runtime PR #62](https://github.com/OW-Programing-Language/ow-lang/pull/62).
This runtime introspection is **opt-in**, does not auto-import arbitrary files
and is not yet supported by the native LLVM backend. See the
[auto-discovery bookstore](examples/bookstore/auto_main.ow) and
[guide](docs/guide.md).

## Smart starter auto-configuration (experimental)

Night optionally reads a `night:` section from the standard OW
`dependencies.yml` manifest. Declare the desired starters and call
`NightAutoConfig.apply("dependencies.yml")` **before starting the HTTP
server**. The Night platform configures selected JDBC, Redis, Kafka, RabbitMQ
and security adapters, while passive starters remain available for explicit
OW imports and route mounting. Unknown or conflicting settings fail early,
and secrets remain in `NIGHT_*` environment variables.

See [smart auto-configuration](docs/auto-configuration.md) and the
[runnable OW example](examples/autoconfig/main.ow). This is opt-in and
does not silently enable authentication, data access or brokers.

## OW-native annotated services and controllers (experimental)

Night can now use OW language `@Service`, `@Controller`, and typed
`@Inject` field annotations from
[ow-lang PR #63](https://github.com/OW-Programing-Language/ow-lang/pull/63).
Imported OW classes are instantiated as per-interpreter singletons with
validated dependency graphs; `night.mount_controllers()` invokes each
annotated controller's `mount()` after a successful `night.boot()`.

See [annotated components](docs/annotated-components.md) and the
[annotated HTTP server example](examples/controllers/main.ow).
This does not automatically infer routes or replace OW native modules;
existing manual and module discovery APIs remain compatible.

## OW-native method mappings and JDBC transactions (experimental)

Controllers can now declare `@GetMapping("/path")` and
`@PostMapping("/path")` on OW methods. Night registers them using
`night.mount_controllers()` without a manual `mount()` method.
OW `@Transactional` methods use Night's existing pooled JDBC adapter,
committing on success and rolling back on failure; nested annotated calls
join the outer transaction and propagate rollback-only when an inner call
fails. This requires the companion
[OW language PR #64](https://github.com/OW-Programing-Language/ow-lang/pull/64).

See the [mapped-transactions guide](docs/mapped-transactions.md) and its
[real H2-backed HTTP example](examples/mapped-transactions/main.ow).
No native LLVM, distributed XA or `REQUIRES_NEW` support is implied.

## Generated OW JDBC repositories (experimental)

Night now supports OW-native `@Repository("table", "id")` singletons,
typed `@Inject` into services, and generated
`save`, `updateById`, `findById`, `findAll`, `existsById`, `deleteById`,
and bounded sorted `page` operations. They use the same pooled JDBC
connection as `@Transactional` with REQUIRED propagation.

See [repository guide](docs/repositories.md) and
[working OW example](examples/repositories/main.ow). The language
annotation requires [OW PR #65](https://github.com/OW-Programing-Language/ow-lang/pull/65).
This is a safe SQL repository adapter, not a full JPA ORM: no
automatic schema generation, upsert, derived finder methods,
entity relationships, or LLVM native lowering.

## Documentation

[Production and security guide](docs/production.md) · [Smart auto-configuration](docs/auto-configuration.md) · [Annotated OW controllers](docs/annotated-components.md) · [Mapped transactions](docs/mapped-transactions.md) ·
[Spring module feature matrix](docs/starters.md) · [Full Spring-inspired ecosystem architecture](docs/spring-ecosystem.md) ·
[Modulith architecture](docs/modules.md) ·
[Bookstore example](examples/bookstore/README.md) ·
[Deployment templates](docs/deployment.md) · [Redis cache](docs/redis.md) ·
[Credential store](docs/credentials.md) · [OpenAPI](docs/openapi.md) · [SMTP Mail](docs/mail.md) ·
[Scheduler](docs/scheduler.md) · [RabbitMQ](docs/rabbitmq.md) · [Redis Streams](docs/redis-streams.md) · [Kafka](docs/kafka.md) · [Inbox](docs/inbox.md) · [Broker Quarantine](docs/broker-quarantine.md) · [Dead-letter Routing](docs/dead-letter-routing.md) · [Broker Restart Tests](docs/broker-restart-tests.md) · [Three-Node HA](docs/three-node-ha.md) · [Majority Loss](docs/quorum-majority-loss.md) · [Live Network Partitions](docs/live-network-partitions.md) · [Message Schemas](docs/message-schemas.md) · [Outbox Worker](docs/outbox-dispatcher.md) · [Validation](docs/validation.md) ·
[Security/Data increments](docs/incremental-production.md) ·
[Upgrade/rollback policy](docs/upgrade-policy.md) ·
[OW starter guide](docs/guide.md)

## Operational status

Night 0.2 is a **production-oriented foundation and integration-tested
candidate**, **not** a production-certified replacement for Spring Boot.
The OW HTTP server remains single-worker and requires a reverse proxy for
TLS, timeouts and rate limiting. JWT RS256, bcrypt credentials, OpenAPI metadata,
SMTP handoff, a PostgreSQL outbox/scheduler, Redis caching and a RabbitMQ
queue adapter, Redis Streams groups and Kafka producer/consumer support are experimental increments; they are **not** full OAuth2/OIDC,
Spring Data, multi-broker outbox fanout, fully supervised broker processing, Quartz callback parity or 1.0
readiness. End-to-end deployment tests and an independent audit remain outstanding.

[MIT License](LICENSE).

**Optional broker DLQ notifications:** The supervised Kafka/RabbitMQ inbox worker can publish a broker-confirmed fingerprint reference after PostgreSQL quarantine and before ACK; see [dead-letter routing](docs/dead-letter-routing.md). These notifications contain no rejected payload and remain at-least-once.

**Broker recovery CI:** Kafka and RabbitMQ integration jobs deliberately restart their broker containers between separate Java test processes, verifying committed offsets/manual ACKs and delivery recovery. Kafka also tests consumer-group handoff and PostgreSQL deduplication. [Test design and limits](docs/broker-restart-tests.md): this is not multi-node HA certification.

**Three-node broker failover testing:** a dedicated CI job provisions 3 Kafka KRaft voters or 3 RabbitMQ quorum queue members, kills the *actual* leader, then verifies durable recovery through a surviving majority and rejoins the failed member. This is same-host, loopback-only fault injection, not production multi-host certification. See [evidence boundaries](docs/three-node-ha.md).

**Quorum majority-loss rehearsal:** the 3-node broker CI now also removes *two* voters, verifies writes are not confirmed with a minority, then rejoins the voters and retries using the original event ID. This is not network-partition or production multi-host certification. [Safety guarantees and test boundaries](docs/quorum-majority-loss.md).
