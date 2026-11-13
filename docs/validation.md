# Night strict object validation (experimental)

Night adds a strict structured-object validator to its existing primitive guards. The API accepts an OW record (a map) and a bounded list of field rules.

```ow
set guards: new NightValidation()
set report: guards.object(
  {name: "Ada", age: 25},
  [["name", "string", 1, 80], ["age", "integer", 0, 150]])
```

Each rule has four fields: `[name, type, required, maximum]`. Supported types: `string` (maximum UTF-16 length), `integer` (absolute numeric bound), `boolean` (maximum ignored but must be positive). `required` is `1` or `0`.

The result is `{valid: 1|0, errors: ["name:required", "age:type", ...]}`. Unknown fields are rejected with `unknown_field`, protecting against accidental mass assignment when callers validate the entire request object. Error messages never include input values.

Schema definitions must come from trusted source code, not untrusted HTTP requests. Validate input **before** creating entities or running SQL. Applications must return HTTP 400 on validation failure; the validator does not automatically return a response status.

Current restrictions: flat objects only, no nested maps/arrays, no generic Bean Validation annotations, no JSON decoding/binding by the interpreter, no per-field coercion, no cross-field rules, and no schemas derived from OpenAPI. A future validation engine needs to address these before full Spring Validation parity.
