# Night strict object and nested array validation (experimental)

Night validates incoming OW records (maps) with explicit, source-controlled field rules. Unknown fields are denied to help prevent mass assignment. Validation does not mutate or coerce input.

```ow
set guards: new NightValidation()
set report: guards.object(
  {name: "Ada", age: 25},
  [["name", "string", 1, 80], ["age", "integer", 0, 150]]
)
```

## Rules

Primitive rules stay backward-compatible: `[fieldName, type, required, maximum]`.

- `string`: maximum UTF-16 length
- `integer`: maximum absolute integer value
- `boolean`: must be true or false (maximum is validated but otherwise unused)

Nested object rules add a fifth element: `[fieldName, "object", required, 1, nestedFieldRules]`.

Array rules add a fifth element: `[fieldName, "array", required, maxItems, elementDescriptor]`. The element descriptor is `["string", maxLength]`, `["integer", maxAbsoluteValue]`, `["boolean", 1]`, or `["object", nestedFieldRules]`. Arrays of arrays are not yet supported.

For example, this request allows a nested profile, bounded tags, and line items:

```ow
set schema: [
  ["profile", "object", 1, 1, [
    ["city", "string", 1, 64]
  ]],
  ["tags", "array", 0, 16, ["string", 32]],
  ["items", "array", 0, 50, ["object", [
    ["sku", "string", 1, 64],
    ["quantity", "integer", 1, 9999]
  ]]]
]
set report: guards.object(request_body, schema)
```

The result is `{valid: 1|0, errors: ["profile.city:required", "items[0].sku:type", ...]}`. Unknown fields are represented by `unknown_field` or `profile:unknown_field`; their names and values are not echoed. The caller must explicitly return HTTP 400 on failure.

**Resource limits:** maximum 128 fields per object, 64 rules per object, 256 rules per schema, 8 levels of nesting, 256 allowed array elements, and 64 errors per call. A rule exceeding these limits or containing invalid types fails closed. An oversized nested object or array reports a validation error instead of traversing its contents.

**Boundary:** Schema definitions must be trusted source code, not arbitrary HTTP input. Validate before entity construction, SQL, or business logic. This is not an ORM, automatic HTTP binding layer, Bean Validation equivalent, cross-field validator, or protection against malicious input when applications skip the validation call.
