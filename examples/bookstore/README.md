# Night Bookstore — full modular web backend

This application runs an actual HTTP server using OW's built-in `route` and
`serve` statements. Three isolated business domains have a public `api/`
signature and an internal `infra/` implementation.

```text
bookstore/
  main.ow                              ← composition root
  catalog/
    api/CatalogApi.ow                  ← CATALOG public module_type
    infra/Catalog.ow                   ← Shop.Catalog native OW module
  orders/
    api/OrdersApi.ow                   ← ORDERS public module_type
    api/PurchaseModel.ow               ← persistent Purchase table
    infra/MakeOrders.ow                ← MakeOrders(CATALOG) functor
  notifications/
    api/NotificationsApi.ow            ← NOTIFICATIONS public module_type
    infra/MakeNotifications.ow         ← MakeNotifications(ORDERS) functor
```

## Try it

```bash
java -jar ow-lang/target/OwIDE.jar check examples/bookstore/main.ow
OW_NO_BROWSER=1 java -jar ow-lang/target/OwIDE.jar run examples/bookstore/main.ow
```

In another terminal:

```bash
curl -i http://localhost:8080/_night/health
curl -i http://localhost:8080/_night/modules
curl -i http://localhost:8080/api/catalog
curl -i http://localhost:8080/api/catalog/1
curl -i -X POST http://localhost:8080/api/orders/1 -d '2'
curl -i http://localhost:8080/api/orders
curl -i http://localhost:8080/api/orders/1
curl -i http://localhost:8080/api/notifications/1
```

### Expected results

- A POST to `/api/orders/1` with body `2` returns HTTP 201 and `total: 24`.
- An invalid quantity returns HTTP 400; unknown catalog IDs return 404.
- Purchase rows persist in `ow-data.json`; set `OW_DB_PATH` to isolate data.
- The synchronous `order.placed` domain event reaches Catalog and Notifications.
- `/_night/modules` lists three modules as started, even though the composition
  root registers them in reverse dependency order.
- `/_night/health` shows the framework status and emitted event count.

## Why functors?

`MakeOrders(Shop.Catalog)` consumes the `CATALOG` signature, never the
concrete `Shop.Catalog` implementation. Swap in another module matching the
same signature and Orders can be reused unchanged. `MakeNotifications(orders)`
consumes only `ORDERS`. Night validates their runtime dependency graph and
dispatches events after successful startup.

The Night container does **not** replace the OW compiler: signature checking,
abstract types and `api/infra` import restrictions are handled by OW. Night
adds lifecycle management, HTTP route composition, module discovery and events.

## Important limitation

OW route handlers are registered as AST blocks rather than closures, so
handlers must refer to globally available module values (`Shop.Catalog`,
`orders`, `notifications`) and the conventional global `night` binding.
This is documented and checked in the example; a closure-based routing API
will require an OW runtime improvement. Night currently dispatches synchronous,
in-process events; there is no outbox or cross-process delivery.
