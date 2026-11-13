# Night framework guide

Night runs on OW's HTTP runtime. OW owns socket listening, route matching,
status responses, JSON serialization and the persistent model store. Night
adds modular composition, typed services, events and lifecycle.

## 1. Create a business module

Put its public signature under `api/`. Define a native OW module or functor
in `infra/` that includes `NIGHT_COMPONENT`:

```ow
module_type CATALOG:
  include NIGHT_COMPONENT
  fn available(item int) -> int
```

`NIGHT_COMPONENT` requires `name`, `requires`, `register`, `start`,
`mount`, `stop` and `on_event`. These methods are checked by OW's
module type system.

## 2. Declare routes in your module

Routes are *real* OW route statements executed once by `NightApp.boot()`
when it invokes each component's `mount()` method. For example:

```ow
fn mount() -> int:
  route "GET /api/catalog":
    respond: [{id: 1, name: "Book"}]
  return: 0
```

Route patterns accept `:param` path segments. Supported handlers use the
OW builtins `param(name)`, `query(name)`, `body()`, `body_json()`,
`response(status, value)` and `respond:`. A record or array response is
serialized as JSON, and a string is sent as text or HTML.

**OW limitation:** these handler AST blocks are not closures. Handler code
must use globally bound business modules and the main `night` variable. Do
not refer to `this`, the local `app` parameter or local `fn mount`
variables from inside the route body.

## 3. Use a composition root

```ow
main:
  set orders ORDERS: MakeOrders(Shop.Catalog)
  set night: new NightApp()
  call: night.use_native_as(orders, "ORDERS")
  call: night.use_native_as(Shop.Catalog, "CATALOG")
  call: assert(night.boot() == 1)
  call: night.mount_system()
  serve: 8080
```

Night resolves all `requires()` dependencies by name. It checks for absent
providers, duplicate module names and cycles; successful startup uses the
topological dependency order, regardless of registration order. A negative
`start()` return causes reverse-order rollback.

## 4. Persistent models

```ow
model Purchase:
  item int
  total int
```

Within a route:

```ow
route "GET /purchases":
  respond: find Purchase

route "POST /purchases":
  save Purchase: {item: 1, total: 12}
  respond: response(201, {created: true})
```

OW models auto-persist to `ow-data.json` (override with `OW_DB_PATH`).
`save` upserts a record carrying an existing `id`. `find Model where`
filters. `delete Model where` removes matching rows. OW currently uses a
local file, not relational SQL or schema migration.

## 5. Events

```ow
call: night.emit("order.placed", 1)
```

Subscribers receive `on_event(type string, payload int)` while active.
Events are synchronous and local, so avoid slow subscriber methods. This is
closer to Spring Modulith's application events than to a durable broker.
Transactional outbox support remains future work.

## 6. Live module operations

Night exposes `is_active`, `disable`, `enable`, `stop`, and
`service(name)`. Disabling a provider with active dependents is rejected.
Handlers should explicitly check `night.is_active("orders")` and return
HTTP 503 when their component is disabled, because OW's HTTP runtime does
not yet support unregistering routes dynamically.

Repeated stop/restart of a single leaf module does not leak startup order
entries. The whole application `boot` operation is one-shot; create a new
NightApp to perform a full application restart.

## 7. Health and inspection

After `night.mount_system()`:

- `GET /_night/health` returns JSON status, number of started modules,
  and published event count.
- `GET /_night/modules` returns each module name and current started/enabled
  state.

These endpoints are **not authenticated**. Put the application behind
appropriate network access controls in deployments.

## 8. Verification

```bash
java -jar ow-lang/target/OwIDE.jar check examples/bookstore/main.ow
python3 tests/http_smoke.py
```

GitHub Actions builds OW, runs compile-time checks, lifecycle regressions,
negative cases and a live HTTP process. See
[Modulith design](modules.md) for dependency and isolation rules.
