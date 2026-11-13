# Night module architecture — Spring Modulith inspiration

Night takes the **application modularity** ideas from Spring Modulith while
remaining entirely OW-native: public contracts, independently developed
business modules, dependency graph verification, lifecycle events and
module-focused tests.

## Contract ownership

Each business module has an `api/` and an `infra/` folder. Only the
composition root imports the private `infra/` source files. OW's own
module boundary checker enforces inter-domain import restrictions.

Business dependencies must be **named explicitly** in
`NIGHT_COMPONENT.requires()`. Night verifies that every required name
exists, refuses self-dependencies, and detects cycles by topological startup
rather than silently choosing registration order.

A typed service is retrieved from the container:

```ow
set catalog CATALOG: night.service("catalog")
call: assert(catalog.available(1) == 1)
```

`night.service` returns the **original native module**, not an untyped
class object. The OW type system verifies the requested `CATALOG` signature;
string labels supplied to `use_native_as` are metadata only.

## Module lifecycle

```text
register all modules
     │
     ▼
verify dependencies
     │
     ▼
start in dependency order ── failure → stop started modules (reverse order)
     │
     ▼
mount each HTTP controller
     │
     ▼
application serves HTTP
```

Each `start`, `register` or `mount` method returns a nonnegative integer
for success or a negative integer for failure. A failed `start` is
transactionally rolled back *for successfully started modules* by calling
their `stop` callbacks. Effects inside `register` or `mount` are not
automatically reversible. Do not run a half-initialized application.

## Events and module communication

A module can emit `night.emit("order.placed", id)` and all active
subscribers receive an `on_event` callback. The event is an integer payload
and a string event type in v0.1; advanced typed records are deferred.

Events are synchronous, in-process, and **not durable**; do not treat them as
transactional outbox or guaranteed-delivery messages.

## What Night does not claim

- It does not implement Spring's annotations, IoC container or AOP proxying.
- It has no SQL transaction boundary or background event publication registry.
- It cannot yet dynamically unregister/reload mounted routes.
- It does not alter OW's networking or compiler semantics.
- It is not a production-hardened public HTTP framework.

This intentional separation keeps OW language features in OW and business
composition in Night.
