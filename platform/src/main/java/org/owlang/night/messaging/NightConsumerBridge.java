package org.owlang.night.messaging;

import dev.oreo.Interpreter;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.owlang.night.data.NightInbox;

/**
 * Durable broker → PostgreSQL inbox bridge. Neither a normal message nor an
 * invalid envelope is ACKed until its PostgreSQL insert/upsert commits.
 *
 * Quarantine is an explicitly configured option; the default constructor
 * preserves the existing fail-closed behavior (malformed input is NOT ACKed).
 */
public final class NightConsumerBridge {
    @FunctionalInterface
    public interface Acknowledger {
        void acknowledge();
    }

    public record Message(String topic, String eventId, String payload,
                          String broker, String sourceIdentity, Acknowledger acknowledger) {
        public Message {
            Objects.requireNonNull(acknowledger);
        }

        /** Backwards-compatible constructor for existing tests/adapters. */
        public Message(String topic, String eventId, String payload, Acknowledger acknowledger) {
            this(topic, eventId, payload, "", "", acknowledger);
        }
    }

    @FunctionalInterface
    public interface Source {
        Message poll();
    }

    public enum Result { IDLE, STORED, DUPLICATE, QUARANTINED }

    private final NightInbox inbox;
    private final NightBrokerQuarantine quarantine;
    private final NightMessageSchemas schemas;
    private final NightPoisonDlq.Publisher poisonPublisher;
    private final Source source;
    private final String namespace;

    public NightConsumerBridge(NightInbox inbox, Source source, String namespace) {
        this(inbox, null, source, namespace, null);
    }

    public NightConsumerBridge(NightInbox inbox, NightBrokerQuarantine quarantine,
                               Source source, String namespace) {
        this(inbox, quarantine, source, namespace, null);
    }

    /** Schema validation requires durable quarantine to prevent unvalidated intake. */
    public NightConsumerBridge(NightInbox inbox, NightBrokerQuarantine quarantine,
                               Source source, String namespace, NightMessageSchemas schemas) {
        this(inbox, quarantine, source, namespace, schemas, null);
    }

    /**
     * Optional DLQ notification is only legal when original messages are
     * persisted in quarantine. This is not a substitute for broker ACK safety.
     */
    public NightConsumerBridge(NightInbox inbox, NightBrokerQuarantine quarantine,
                               Source source, String namespace, NightMessageSchemas schemas,
                               NightPoisonDlq.Publisher poisonPublisher) {
        this.inbox = Objects.requireNonNull(inbox);
        if ((schemas != null || poisonPublisher != null) && quarantine == null)
            throw new IllegalArgumentException("Schema and DLQ require durable quarantine");
        this.quarantine = quarantine;
        this.schemas = schemas;
        this.poisonPublisher = poisonPublisher;
        this.source = Objects.requireNonNull(source);
        this.namespace = NightInbox.namespace(namespace);
    }

    static String invalidReason(String eventId, String payload) {
        try {
            NightInbox.eventId(eventId);
        } catch (IllegalArgumentException ex) {
            return "INVALID_EVENT_ID";
        }
        if (payload == null) return "INVALID_PAYLOAD";
        if (payload.getBytes(StandardCharsets.UTF_8).length > 262_144)
            return "PAYLOAD_TOO_LARGE";
        return null;
    }

    public Result consumeOne() {
        Message message = source.poll();
        if (message == null) return Result.IDLE;
        String reason = invalidReason(message.eventId(), message.payload());
        if (reason == null && schemas != null)
            reason = schemas.invalidReason(message.topic(), message.payload());
        if (reason != null && quarantine != null) {
            // Neither a DB outage nor a too-large-to-retain envelope may
            // advance Kafka offsets or remove Rabbit messages.
            quarantine.capture(namespace, message.broker(), message.topic(),
                    message.sourceIdentity(), reason, message.eventId(), message.payload());
            // A failed DLQ confirm deliberately leaves the source unacknowledged.
            // Retried notifications have the same stable fingerprint/key.
            if (poisonPublisher != null) poisonPublisher.publish(message, reason);
            message.acknowledger().acknowledge();
            return Result.QUARANTINED;
        }

        int inserted = inbox.accept(namespace, message.topic(),
                message.eventId(), message.payload());
        message.acknowledger().acknowledge();
        return inserted == 1 ? Result.STORED : Result.DUPLICATE;
    }

    /** Raw Kafka envelope intake: malformed Kafka keys must reach quarantine. */
    public static Source kafka(NightKafka kafka, int waitMillis) {
        Objects.requireNonNull(kafka);
        if (waitMillis < 0 || waitMillis > 2000)
            throw new IllegalArgumentException("Invalid Kafka poll wait");
        return () -> {
            Interpreter.OwRecord record = kafka.pollForIngestion(waitMillis);
            if (((Number) record.get("present")).intValue() == 0) return null;
            String topic = (String) record.get("topic");
            int partition = ((Number) record.get("partition")).intValue();
            long offset = ((Number) record.get("offset")).longValue();
            String eventId = (String) record.get("message_id");
            String body = (String) record.get("body");
            return new Message(topic, eventId, body, "kafka",
                    topic + ":" + partition + ":" + offset,
                    () -> kafka.acknowledgeIngestion(topic, partition, offset));
        };
    }

    /** RabbitMQ message redelivery fingerprint derives from immutable content. */
    public static Source rabbitMq(NightRabbitMQ rabbit, String queue) {
        Objects.requireNonNull(rabbit);
        NightInbox.topic(queue);
        return () -> {
            Interpreter.OwRecord record = rabbit.poll();
            if (((Number) record.get("present")).intValue() == 0) return null;
            long tag = ((Number) record.get("delivery_tag")).longValue();
            String eventId = (String) record.get("message_id");
            String body = (String) record.get("body");
            // Rabbit delivery tags change on redelivery. Content-hash identity
            // is intentionally best-effort: identical poison envelopes collapse.
            String identity = NightBrokerQuarantine.fingerprint(
                    queue + "\u0000" + eventId + "\u0000" + body);
            return new Message(queue, eventId, body, "rabbitmq", identity,
                    () -> rabbit.ack(tag));
        };
    }
}
