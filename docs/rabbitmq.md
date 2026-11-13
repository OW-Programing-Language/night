# Night RabbitMQ connector (experimental)

NightRabbitMQ is a narrow, OW-native durable queue adapter backed by the official RabbitMQ Java client. It supports publishing persistent JSON payloads to a named, durable queue through the default exchange, waits for broker publisher confirmations, and uses manual acknowledgement when polling messages.

## Configuration

Configure a TLS-protected RabbitMQ virtual host with a scoped user:

```sh
NIGHT_RABBITMQ_URL=amqps://night_user:use-a-secret@broker.example.com:5671/%2F
NIGHT_RABBITMQ_QUEUE=night_events
```

Plain `amqp://` is rejected outside an explicitly configured local test:
`NIGHT_RABBITMQ_ALLOW_INSECURE_LOOPBACK=true` and a loopback URI. Do not enable it for production. The default RabbitMQ virtual host is `/`, encoded as `%2F` in a URI path. A literal trailing `/` means an empty vhost and may fail with `vhost not found`. The broker account must have appropriate configure/write/read permissions only on its queue. Credentials in an environment URI may be visible to operators; use a secret manager.

```ow
import "../../src/modules/messaging/NightRabbitMQ.ow"
set broker: new NightRabbitMQ()
call: broker.open()
call: broker.publish("created-order-1001", "{\"orderId\":1001}")
set message: broker.poll()
# Application must verify message ID, validate JSON, and perform its side effect.
# After success:
call: broker.acknowledge(message.delivery_tag)
```

A worker can call `reject(delivery_tag, 1)` to requeue after failure. `reject(..., 0)` drops a message unless an administrator configured a RabbitMQ dead-letter exchange. The application must include a consumer-side deduplication mechanism keyed by message ID; the broker may redeliver when a worker exits before ACK. Publishers should put messages in `NightOutbox` inside the same transaction as business updates, then publish and acknowledge the outbox event only after broker confirmation.

## Guarantees and limits

- Durable queue declaration and persistent AMQP delivery mode.
- Confirmation that the broker accepted publication, **not** that a consumer processed it.
- Manual per-message ACK/NACK; polling API for OW (no arbitrary callback evaluation).
- TLS and server hostname verification for `amqps://` URIs.
- All channel operations synchronized to protect against concurrent access; this is not a high-throughput multi-channel worker pool.
- No automatic dead-letter exchange setup, delayed retry scheduler, consumer group framework, exactly-once semantics, schema registry, Kafka adapter or Redis Streams adapter.
- The RabbitMQ Java client cautions that polling with `basicGet` is inefficient. Production worker workloads should use a dedicated supervised consumer loop outside the OW request thread.

`NightRabbitMQTest` checks URI policy and a real broker publish/poll/ACK round trip on CI. Successful CI outcome must be verified before claiming integration readiness.
