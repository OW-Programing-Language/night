# Annotated OW HTTP mappings and transactions

Night now supports OW `@GetMapping("/path")`, `@PostMapping("/path")`
and `@Transactional`, in addition to the previously added OW-native
`@Service`, `@Controller` and `@Inject` annotations. These features
are implemented in [ow-lang PR #64](https://github.com/OW-Programing-Language/ow-lang/pull/64)
(stacked on [OW #63](https://github.com/OW-Programing-Language/ow-lang/pull/63)).

## Controller without manual mount()

```ow
@Service
class EntryService:
  @Transactional
  fn insert(id int, name string) -> int:
    set db: new NightData()
    return: db.execute("INSERT INTO entries(id, name) VALUES (?, ?)", [id, name])

@Controller
class EntryController:
  @Inject
  field private entries EntryService

  @PostMapping("/api/entries/:id")
  @Transactional
  fn create():
    set id int: int(param("id"))
    call: this.entries.insert(id, body())
    return: response(201, {id: id, created: 1})

main:
  set db: new NightData()
  call: db.open()
  set night: new NightApp()
  call: assert(night.boot() == 1)
  call: night.mount_controllers()
  serve: 8080
```

Import the OW class modules and SQL helper in the entrypoint; the code
above omits imports and schema creation for brevity. The complete tested
example is [examples/mapped-transactions](../examples/mapped-transactions/main.ow).

## Transactional semantics

- `@Transactional` wraps the annotated OW method with Night JDBC
  `begin()` / `commit()`, and rolls back when a method fails.
- Nested annotated calls **join the outer JDBC transaction** (REQUIRED).
  If any inner annotated method fails, the outermost transaction is
  rollback-only **even if a caller catches the exception**.
- A failed outer transaction is not committed. The Night after-request
  cleanup hook remains an additional safety net for uncommitted work.
- The same connection is used only within one execution thread. There
  is **no** XA/distributed transaction, event-broker atomicity, savepoint,
  `REQUIRES_NEW`, retry, async context propagation or reactive support.
- The database pool must be explicitly opened (or enabled through
  `NightAutoConfig`) **before** calling annotated database methods.
  With no Night extension, transactions fail closed.
- Do not perform irreversible filesystem/network side effects and assume
  `@Transactional` can roll them back. Only JDBC operations are atomic.

## HTTP and security

OW registers mapped methods with the **existing HTTP dispatcher**;
authentication middleware still runs before a mapped handler. An annotated
route is **not** automatically public, and Night doesn't auto-enable or
bypass bearer authentication. In secured deployments, protect write routes
with the corresponding roles and explicitly choose any public endpoints.

Mapped methods must be public and argument-free. They can access
`body()`, `body_json()`, `param("id")` and `query("key")`. They
return OW response values or `response(status, value)`. A legacy
controller without mapping annotations continues to use `mount()`;
mixing manual `mount()` with mapping annotations in one controller is
rejected.

## Verification

`tests/mapped_mount.ow` checks automatic and idempotent mounting.
`tests/mapped_transactions_smoke.py` runs a real Night HTTP process with
H2 and asserts:
- POST commits and GET sees the persisted row
- a nested annotated service exception produces HTTP 500 and no persisted row
- a caught nested exception still forces outer rollback-only and HTTP 500
- a later independent POST commits normally

These tests are included in GitHub CI. This is a framework capability,
not a replacement for independent production security and recovery audits.
