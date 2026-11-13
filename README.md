# Night 🌙

**A modular web framework for [OW](https://github.com/OW-Programing-Language/ow-lang), inspired by Spring Modulith.**

Night combines OW's **real HTTP server and persistent models** with **native module signatures, functors, business-module dependencies and lifecycle events**. It is not a Spring port or another JVM server: it is a small framework written in OW that composes OW's built-in web features.

## What you get

- **Actual HTTP applications**: `route`, `respond` and `serve` are provided by the OW runtime. Night mounts routes from independently developed business modules.
- **True native OW modules and functors**: `module_type`, `include`, qualified modules and `MakeOrders(Catalog)` are checked by OW.
- **A modular container**: modules implement `NIGHT_COMPONENT`; `NightApp` validates dependencies, detects duplicates/cycles, and starts modules topologically.
- **Lifecycle + rollback**: `register → start → mount → serve`; failure during startup stops already-started modules in reverse order.
- **Typed service retrieval**: `night.service("catalog")` returns the original OW module, which can be bound to `CATALOG`.
- **Domain events**: publish `night.emit("order.placed", id)`, dispatch to active subscribers in process.
- **Observability**: `/_night/health` and `/_night/modules` endpoints.
- **Persistence**: OW's `model` / `save` / `find` / `delete` database.
- **Tests**: compilation preflight, dependency and failure regressions, 70 restart cycles and live HTTP/Persistence tests in GitHub Actions.

## Quickstart

Requires Java 17+ and an OW compiler build or release with native modules and HTTP support.

```bash
git clone https://github.com/OW-Programing-Language/night.git
cd night
git clone https://github.com/OW-Programing-Language/ow-lang.git
mvn -f ow-lang/pom.xml package

java -jar ow-lang/target/OwIDE.jar check examples/bookstore/main.ow
OW_NO_BROWSER=1 java -jar ow-lang/target/OwIDE.jar run examples/bookstore/main.ow
```

Try a request from another terminal:

```bash
curl http://localhost:8080/_night/health
curl http://localhost:8080/api/catalog
curl -i -X POST http://localhost:8080/api/orders/1 -d '2'
curl http://localhost:8080/api/orders
```

The POST creates a persistent purchase of two 12-unit books and emits an
`order.placed` event that Notifications receives. See the
[Bookstore walkthrough](examples/bookstore/README.md).

## Architecture

<details>
<summary>Module graph (Catalog → Orders → Notifications)</summary>

```text
               NightApp (OW)
             ┌────────────────┐
             │  Module graph  │
             │  Lifecycle     │
             │  Event bus     │
             │  HTTP mounting │
             └───────┬────────┘
                     │
     ┌───────────────┴─────────────────┐
     ▼               ▼                 ▼
  Catalog        Orders            Notifications
  CATALOG        ORDERS            NOTIFICATIONS
  Shop.Catalog   MakeOrders(...)   MakeNotifications(...)
  /api/catalog   /api/orders       /api/notifications
      └──────────────►└─────────────────►
```

</details>

The composition root constructs and registers modules:

```ow
import "../../src/NightApp.ow"

main:
  set orders ORDERS: MakeOrders(Shop.Catalog)
  set notifications NOTIFICATIONS: MakeNotifications(orders)
  set night: new NightApp()

  # Register in any order: Night starts dependencies first.
  call: night.use_native_as(notifications, "NOTIFICATIONS")
  call: night.use_native_as(orders, "ORDERS")
  call: night.use_native_as(Shop.Catalog, "CATALOG")

  call: assert(night.boot() == 1)
  call: night.mount_system()
  serve: 8080
```

Domain interfaces use **one reusable lifecycle signature**:

```ow
module_type CATALOG:
  include NIGHT_COMPONENT
  fn available(item int) -> int
  fn price(item int) -> int
```

A `fn mount()` method inside each native module registers its routes using OW's `route "METHOD /path":` syntax. These are real endpoints, not a mocked dispatcher.

## Documentation

[Getting started / HTTP guide](docs/guide.md) ·
[Modulith architecture](docs/modules.md) ·
[Bookstore example](examples/bookstore/README.md) ·
[Original Todo app](examples/todo_app.ow)

## Design and current limits

**Night is an early framework, not a production-ready Spring Boot replacement.**
The built-in OW HTTP server is single-worker; persistence is a local JSON data
store, not transactional PostgreSQL; events are synchronous, in-process and
not durable. There is no authentication middleware, transaction/outbox,
connection pooling, migrations, or structured validation layer yet.

OW's HTTP handlers currently do not capture method-local variables. Night's
route handlers therefore refer to public global module values and the
conventional app variable `night`. The module container is implemented in
OW, while HTTP networking and persistence come from the OW runtime itself.

For boundary enforcement, keep each business module's public contracts under
`api/`, private implementations under `infra/`, and all implementation
imports in the composition root. OW's loader prevents cross-module imports of
another domain's `infra/` sources.

Night and flOW are complementary: Night provides a web-oriented module
container using similar lifecycle principles. It does not require installing
flOW and can eventually share a smaller runtime core with it.

Licensed under [MIT](LICENSE).
