# Night guide

Night apps are plain Ow programs that use the runtime's built-in web server and
database. A typical app is a single `.ow` file: models at the top, routes inside
`main`, and `serve` at the end.

## 1. Models

```yaml
model User:
  name string
  age int
```

A model is a persistent table. Rows are records (`{name: "Ada", age: 36}`) and
each gets an auto `id`. Data is stored in `ow-data.json` (override with the
`OW_DB_PATH` environment variable).

- **Insert:** `save User: {name: "Ada", age: 36}`
- **Update:** `save User: row` — when the record carries an existing `id`, the
  matching row is replaced (upsert).
- **Query:** `find User` (all) or `find User where age > 30`. In a `where`
  clause, field names are used directly; your handler's variables are also in
  scope, so `find User where id == want` works.
- **Delete:** `delete User` or `delete User where id == want`.

## 2. Routes

```yaml
route "GET /users/:id":
  set want int: int(param("id"))
  set rows: find User where id == want
  if: len(rows) == 0
    respond: response(404, {error: "not found"})
  respond: rows[0]
```

- The spec is `"METHOD /path"`. Method defaults to `GET`. `:name` segments are
  path parameters.
- The body is an inline handler, run per request.

### Reading the request

| Helper | Returns |
|---|---|
| `req` | the full request record: `method`, `path`, `query`, `params`, `body` |
| `param(name)` | a path parameter (string) |
| `query(name)` | a query-string value |
| `body()` | the raw request body (string) |
| `body_json()` | the request body parsed from JSON |

### Sending a response

- `respond: value` ends the handler and sends `value`:
  - a **record or array** → `application/json`
  - a **string** → `text/html` if it starts with `<`, otherwise `text/plain`
- `respond: response(status, value)` sets an explicit status code.

## 3. Serving

```yaml
main:
  route "GET /": ...
  serve: 8080
```

`serve: PORT` starts the server and blocks. Requests are handled one at a time,
so your models stay consistent without extra locking.

## 4. JSON helpers

- `json(value)` → JSON string
- `json_parse(string)` → value

## Conventions

- One app per file; models first, routes in `main`, `serve` last.
- Group related routes together; factor shared logic into `fn` functions
  (e.g. a `page()` that renders HTML).
- Return records/arrays for APIs; return HTML strings for pages.

## Not yet

Updates are upsert-by-id; there is no partial update or migration system yet.
Middleware, sessions and templating are planned — for now compose with plain
functions.
