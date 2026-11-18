# Night message body schemas (experimental)

Night's Kafka and RabbitMQ **consumer inbox worker** now supports optional, **versioned JSON-body validation**. A local configuration file maps exact broker topic/queue names to bounded Night field rules. Malformed JSON and schema violations are durably quarantined **before broker acknowledgement**, rather than entering the application inbox.

This is **Night's narrow field-rule format**, *not* the JSON Schema standard. No network/registry lookups, remote references, arbitrary Java deserialization, programmable validators, or automatic OW callbacks are supported.

## Setup

1. Apply Flyway migrations through `V8__night_message_schema_quarantine.sql`. Never edit the previously released V7 migration in place.
2. Install a trusted, read-only JSON schema file on each worker node; deploy the same version to all replicas of the same consumer group.
3. Set `NIGHT_CONSUMER_SCHEMAS_FILE` to its **absolute path**, and restart the consumer worker after a schema change.

Example `/etc/night/message-schemas.json`:

```json
{
  "version": 1,
  "topics": {
    "orders.created": [
      ["orderId", "integer", 1, 100000],
      ["customer", "object", 1, 1, [
        ["city", "string", 1, 64],
        ["verified", "boolean", 0, 1]
      ]],
      ["items", "array", 0, 16, ["string", 64]]
    ]
  }
}
```

```sh
export NIGHT_CONSUMER_SCHEMAS_FILE=/etc/night/message-schemas.json
```

For **Kafka**, the key under `topics` is the exact Kafka topic. For **RabbitMQ**, it is the configured queue name (`NIGHT_RABBITMQ_QUEUE`). Every consumed topic/queue must be explicitly listed when validation is enabled.

If `NIGHT_CONSUMER_SCHEMAS_FILE` is absent, the consumer preserves the previous envelope-only validation behavior. If set, missing or malformed files, unsupported versions, duplicate JSON keys, excessive rules or unknown topics **fail closed**. The worker never ACKs a message when the schema configuration is missing for its topic.

## Rule format

Night reuses the native [nested validation](validation.md) rule grammar.

- Primitive: `[field, "string"|"integer"|"boolean", required_0_or_1, maximum]`; strings use UTF-16 length, integers use max absolute value.
- Nested object: `[field, "object", required, 1, nestedRules]`.
- Array: `[field, "array", required, maxItems, ["string"|"integer"|"boolean", maximum]]` or `["object", nestedRules]`.

Unknown fields, wrong types, missing required values, invalid nested objects or oversized arrays fail validation. Coercion is not performed. Root payload must be a JSON object. Arrays of arrays, numeric ranges other than absolute integers, cross-field conditions, `$ref`, regex patterns and conditional schemas are not implemented.

Configuration limits: 64 KiB file, 32 topic definitions, 256 total field rules per topic, 64 rules per object, 8 rule levels, 256 elements per array and 64 validation errors (not included in broker logs). Runtime JSON parsing is limited to 256 KiB UTF-8 bodies, 16 JSON nesting levels, and 64-digit numbers. Duplicate JSON object keys are rejected, including nested duplicates.

**Important:** Transport decoding currently produces Java strings from broker bytes. Validation is applied to that string; strict raw-byte UTF-8 verification and cryptographic event authenticity remain separate work.

## Quarantine and acknowledgements

```text
Kafka / RabbitMQ message
  → validate envelope (event ID, size, presence)
  → if configured, parse JSON and validate exact topic rules
     ├─ valid: PostgreSQL night_inbox INSERT/ON CONFLICT → commit → ACK
     └─ invalid JSON or field rules:
        PostgreSQL night_broker_quarantine UPSERT → commit → ACK
```

New fixed reason codes are `INVALID_JSON` and `SCHEMA_VIOLATION`. Quarantine retains the original bounded payload with no raw values in validator errors or worker logs.

**Unknown topic/rule configuration is an operational error, not poison input:** Night exits without ACK instead of quarantining messages under the wrong schema. If PostgreSQL is unavailable or quarantine cannot store the complete payload, no broker ACK occurs. Lost ACK after a committed quarantine write is handled by the existing idempotent quarantine fingerprint.

Quarantined messages are **not** automatically replayed. An operator must review/remediate the producer and explicitly republish a corrected message with an appropriate event ID. Restrict database access and set a retention/encryption policy because quarantined bodies may include sensitive data.

## Rollout and compatibility

Make schema changes using a versioned deployment. Additive optional fields can be introduced together with producers; changing required fields or types may quarantine existing in-flight events. Use staging with recorded representative payloads, controlled producer rollout, and a fallback/rollback plan. **All workers in one group must have the same schema file** to avoid nondeterministic handling during rolling deployments.

Tests cover nested rule acceptance/rejection, malformed JSON, duplicate keys, invalid startup configuration, unknown topics, and real Kafka/RabbitMQ → PostgreSQL quarantine before ACK. This does **not** prove multi-node Kafka or RabbitMQ TLS failover, broker-side dead-letter exchanges, distributed exactly-once delivery, producer contract compatibility, or production reliability targets.

Per-topic validation failures may also emit **optional broker-confirmed dead-letter reference notifications** after durable quarantine; see [dead-letter routing](dead-letter-routing.md). They reference the original payload by SHA-256 fingerprint instead of copying it into Kafka/RabbitMQ.
