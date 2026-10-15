# Night 🌙

**The web framework for [Ow](https://github.com/OW-Programing-Language/ow-lang).**

Java has Spring. Python has Django. **Ow has Night** — a batteries-included way
to build web backends with almost no code, on top of Ow's built-in HTTP server
and database.

```yaml
model Todo:
  text string
  done bool

main:
  route "GET /todos":
    respond: find Todo

  route "POST /todos":
    save Todo: {text: body(), done: false}
    respond: response(201, {ok: true})

  serve: 8080
```

That's a complete, persistent REST service — no ORM, no SQL, no config, no
dependencies.

## Why Night

- **Zero setup.** A `model` is a table; `route`/`serve` is your server. Both are
  built into the Ow runtime, so there's nothing to install.
- **Tiny.** CRUD endpoints are a few lines each. Handlers are inline blocks.
- **Persistent by default.** Models auto-save to `ow-data.json`.
- **One language, one file.** Models, routes, views and logic live together in
  readable, YAML-shaped Ow.

## Requirements

The Ow toolchain (`OwIDE.jar`) — grab it from the
[ow-lang releases](https://github.com/OW-Programing-Language/ow-lang/releases).

```sh
# run a Night app
java -cp OwIDE.jar dev.oreo.Main --run app.ow
# or just open app.ow in OwIDE and press Run
```

## Quickstart

1. Copy [`examples/todo_app.ow`](examples/todo_app.ow).
2. Run it; open <http://localhost:8080/> for the HTML view, or use the JSON API:

```sh
curl -X POST localhost:8080/todos -d 'Buy milk'
curl localhost:8080/todos
curl -X POST localhost:8080/todos/1/done
curl -X DELETE localhost:8080/todos/1
```

## The essentials

| Concept | Night / Ow |
|---|---|
| Model (table) | `model User:` with typed fields; auto-persisted |
| Create / update | `save User: {...}` (upserts when the record has an `id`) |
| Query | `find User` · `find User where age > 30` |
| Delete | `delete User where id == want` |
| Route | `route "GET /users/:id": <block>` |
| Request | `req`, `param(name)`, `query(name)`, `body()`, `body_json()` |
| Response | `respond: value` (record/array → JSON, string → HTML/text); `response(status, value)` |
| Start | `serve: 8080` |

See [`docs/guide.md`](docs/guide.md) for the full walkthrough.

## Status

Night is young and tracks the Ow runtime closely. These primitives live in the
Ow core; Night is the conventions, templates and docs around them, and will grow
higher-level helpers (middleware, auth, templating) over time.

## License

[MIT](LICENSE).
