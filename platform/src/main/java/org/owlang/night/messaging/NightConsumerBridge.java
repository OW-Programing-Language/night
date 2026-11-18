package org.owlang.night.messaging;

import dev.oreo.Interpreter;
import java.util.Objects;
import org.owlang.night.data.NightInbox;

/**
 * Durable broker → PostgreSQL inbox bridge. No application callback code is
 * evaluated on the broker thread, and broker ACK follows the committed insert.
 *
 * A crash or broken ACK after insert can redeliver, but accept() is protected
 * by (namespace, topic, event_id) uniqueness. Business processing is a
 * separate, explicitly transactional NightInbox claim/ack workflow.
 */
public final class NightConsumerBridge {
    @FunctionalInterface
    public interface Acknowledger {
        void acknowledge();
    }

    public record Message(String topic, String eventId, String payload,
                          Acknowledger acknowledger) {
        public Message {
            Objects.requireNonNull(acknowledger);
        }
    }

    @FunctionalInterface
    public interface Source {
        /** Poll at most one message, or return null when the broker is idle. */
        Message poll();
    }

    public enum Result { IDLE, STORED, DUPLICATE }

    private final NightInbox inbox;
    private final Source source;
    private final String namespace;

    public NightConsumerBridge(NightInbox inbox, Source source, String namespace) {
        this.inbox = Objects.requireNonNull(inbox);
        this.source = Objects.requireNonNull(source);
        this.namespace = NightInbox.namespace(namespace);
    }

    public Result consumeOne() {
        Message message = source.poll();
        if (message == null) return Result.IDLE;
        int inserted = inbox.accept(namespace, message.topic(),
                message.eventId(), message.payload());
        // Acknowledge only after the PostgreSQL INSERT (or dedup check) commits.
        // Do not catch exceptions: the supervisor restarts on DB/broker failure.
        message.acknowledger().acknowledge();
        return inserted == 1 ? Result.STORED : Result.DUPLICATE;
    }

    /** Adapter for the existing one-pending-record Kafka consumer. */
    public static Source kafka(NightKafka kafka, int waitMillis) {
        Objects.requireNonNull(kafka);
        if (waitMillis < 0 || waitMillis > 2000)
            throw new IllegalArgumentException("Invalid Kafka poll wait");
        return () -> {
            Interpreter.OwRecord record = kafka.poll(waitMillis);
            if (((Number) record.get("present")).intValue() == 0) return null;
            String topic = (String) record.get("topic");
            int partition = ((Number) record.get("partition")).intValue();
            long offset = ((Number) record.get("offset")).longValue();
            String eventId = (String) record.get("message_id");
            String body = (String) record.get("body");
            return new Message(topic, eventId, body,
                    () -> kafka.acknowledge(topic, partition, offset, eventId));
        };
    }

    /** Adapter for one manually ACKed message on the configured durable queue. */
    public static Source rabbitMq(NightRabbitMQ rabbit, String queue) {
        Objects.requireNonNull(rabbit);
        NightInbox.topic(queue);
        return () -> {
            Interpreter.OwRecord record = rabbit.poll();
            if (((Number) record.get("present")).intValue() == 0) return null;
            long tag = ((Number) record.get("delivery_tag")).longValue();
            return new Message(queue, (String) record.get("message_id"),
                    (String) record.get("body"), () -> rabbit.ack(tag));
        };
    }
}
