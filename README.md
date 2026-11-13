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
| Night Security | Bearer auth, experimental RS256/JWKS JWT resource server, RBAC per route, secure headers | `src/modules/security/` |
| Night Data | JDBC, HikariCP, PostgreSQL/H2, Flyway, transactions, pagination, optimistic updates, PostgreSQL outbox | `src/modules/data/` + `platform/` |
| Night Actuator | Health, readiness, HTTP metrics, module inspection | `src/modules/actuator/` |
| Night Config | Environment configuration | `src/modules/config/` |
| Night Cache | Local TTL cache | `src/modules/cache/` |
| Night Redis | Shared string cache, TTL and key prefix isolation (TLS Redis) | `src/modules/data/NightRedis.ow` + `platform/` |
| Night HTTP | Allowlisted outbound HTTPS GET, bounded response and circuit breaker | `src/modules/http/` + `platform/` |
| Night Validation | Input guards | `src/modules/validation/` |
| Night Test | Compiler checks, lifecycle tests, secured HTTP+SQL tests | `tests/` + GitHub Actions |

See the [full starter ecosystem matrix](docs/starters.md) for what is
implemented versus planned (OAuth2, ORM, Redis, Kafka, mail, GraphQL, etc.).

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

## Documentation

[Production and security guide](docs/production.md) ·
[Spring module feature matrix](docs/starters.md) · [Full Spring-inspired ecosystem architecture](docs/spring-ecosystem.md) ·
[Modulith architecture](docs/modules.md) ·
[Bookstore example](examples/bookstore/README.md) ·
[Deployment templates](docs/deployment.md) · [Redis cache](docs/redis.md) ·
[Security/Data increments](docs/incremental-production.md) ·
[OW starter guide](docs/guide.md)

## Operational status

Night 0.2 is a **production-oriented foundation and integration-tested
candidate**, **not** a production-certified replacement for Spring Boot.
The OW HTTP server remains single-worker and requires a reverse proxy for
TLS, timeouts and rate limiting. JWT RS256 resource verification, a PostgreSQL
outbox and a Redis cache starter are experimental increments; they are **not**
full OAuth2/OIDC, Spring Data, durable broker integration or 1.0 readiness.
The deployment manifests and expanded CI still require successful validation.

[MIT License](LICENSE).
