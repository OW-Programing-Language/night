# OW-native generated JDBC repositories (PR #29)

Night's repository integration is built around the OW language annotation
`@Repository("table", "id_column")` introduced in
[OW PR #65](https://github.com/OW-Programing-Language/ow-lang/pull/65).

```ow
@Repository("night_repository_users", "id")
class UserRepository:

@Service
class UserService:
  @Inject
  field private users UserRepository

  @Transactional
  fn create(id int, username string) -> int:
    return: this.users.save({id: id, username: username, score: 0})

  fn find(id int):
    return: this.users.findById(id)
```

**Required:** import the repository and service OW classes into the
application, enable the Night JDBC platform extension, and open the
database pool or use `NightAutoConfig`. Manage tables using Flyway
or explicit schema setup; repositories do not auto-create tables.

## Generated methods

| OW method | Behavior |
| --- | --- |
| `save(record)` | Insert one row; returns inserted row count; duplicate ID errors |
| `findById(id)` | Return OW record, or numeric 0 when missing |
| `existsById(id)` | Return 1/0 |
| `deleteById(id)` | Return row count |
| `findAll()` | Return up to 500 rows, sorted by ID |
| `page("score", 20, 0)` | Return bounded ascending rows |
| `page("-score", 20, 0)` | Return bounded descending rows |

Pagination accepts limit 1..500, offset 0..10,000,000, and validates
the sorting column. A stable ID secondary key prevents ambiguous order
between equally ranked rows. OW record field names are normalized to
lowercase consistently across H2 and PostgreSQL.

## Security and transactions

- OW `@Repository` accepts only **literal validated** table and ID
  identifiers. Every dynamic record column name is checked against the
  strict SQL identifier grammar; **all values are bound placeholders**,
  never SQL-interpolated.
- OW records passed to `save` must contain 1..32 columns including
  the ID and non-null JDBC scalar values. `save` is an **insert-only**
  operation, not an upsert; there are no implicit generated IDs.
- SQL injection protection is **not** field-level authorization.
  Before calling `save` with an HTTP request, validate and allowlist
  user-writable fields and enforce authentication/authorization.
- `@Transactional` service methods and their generated repository calls
  share the **same NightJdbc thread-local connection**. Exceptions
  trigger rollback; nested REQUIRED propagation marks the outer
  transaction rollback-only even if a nested error is caught.
- There is no JPA ORM, lazy relationships, derived
  `findByUsername` queries, automatic schema migrations, optimistic
  update method generation, request-scoped ORM session, XA, reactive
  transaction propagation or LLVM native lowering.
- Existing `NightData.page()`, `update_versioned()` and raw parameterized
  `query()/execute()` remain available for advanced queries and
  concurrency control.

## Verification

`platform/src/test/java/org/owlang/night/data/NightCrudRepositoryTest.java`
tests generated CRUD, sorting, SQL identifier safety, duplicate IDs,
transaction commit and rollback against **H2 and PostgreSQL**.

`tests/repositories_http_smoke.py` starts a real OW + Night HTTP
application using H2, exercises `GET/POST` routes, injected repositories,
saved rows, sorted pages, duplicate ID rejection, nested rollback-only,
and recovery after a failure. See [the complete example](../examples/repositories/main.ow).
