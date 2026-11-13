# Night OpenAPI registry (experimental)

`NightOpenApi` lets OW applications explicitly register route metadata and generate a minimal **OpenAPI 3.0.3 document**. Route metadata registration is separate from defining actual `route` handlers, since the current OW interpreter does not expose controller metadata or typed JSON schema introspection.

```ow
import "../../src/modules/web/NightOpenApi.ow"

set docs: new NightOpenApi()
call: docs.register("GET", "/api/items", "List items", 1)
call: docs.register("GET", "/api/items/:id", "Get item", 1)
call: docs.register("GET", "/_night/health", "Liveness", 0)
call: docs.mount()
```

The `register` method accepts an uppercase HTTP method, a route path (including `:name` parameters), a bounded human summary, and `authenticated=1` for bearer-protected routes (`0` for public). The generator converts `:id` to OpenAPI `{id}` and documents required string path parameters and default successful response semantics.

The `mount()` method registers `GET /_night/openapi`, returning a JSON object containing OpenAPI `openapi`, `info`, `paths`, and a bearer security scheme. **Protect this administrative route** before enabling it:

```ow
call: security.role("GET /_night/openapi", "ADMIN")
```

The production example does this and registers a small set of its real API paths. The registry is synchronized, limits itself to 1,000 routes and rejects duplicate or malformed declarations.

## Not implemented

This is **not** full Springdoc or GraphQL parity: it does not derive typed request/response schemas automatically, introspect route handlers, validate JSON bodies, expose a GraphQL server, or generate clients. Unregistered HTTP routes are absent. The default response description is deliberately generic. Tests in `NightOpenApiTest` cover the registry and parameters, and the production smoke test covers admin-only access to the generated route.
