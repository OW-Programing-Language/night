package org.owlang.night.messaging;

import dev.oreo.Interpreter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NightKafkaTest {
    @Test void tlsIsRequiredUnlessEveryEndpointIsExplicitLoopback() {
        assertThrows(IllegalStateException.class, () -> NightKafka.config(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> NightKafka.config(Map.of(
                "NIGHT_KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9092",
                "NIGHT_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")));
        assertThrows(IllegalArgumentException.class, () -> NightKafka.config(Map.of(
                "NIGHT_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,broker.example.com:9092",
                "NIGHT_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "NIGHT_KAFKA_ALLOW_INSECURE_LOOPBACK", "true")));
        assertThrows(IllegalArgumentException.class, () -> NightKafka.config(Map.of(
                "NIGHT_KAFKA_BOOTSTRAP_SERVERS", "localhost:99999",
                "NIGHT_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "NIGHT_KAFKA_ALLOW_INSECURE_LOOPBACK", "true")));
        assertThrows(IllegalArgumentException.class, () -> NightKafka.config(Map.of(
                "NIGHT_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "NIGHT_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")));
        Properties safeLocal = NightKafka.config(Map.of(
                "NIGHT_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092",
                "NIGHT_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "NIGHT_KAFKA_ALLOW_INSECURE_LOOPBACK", "true"));
        assertEquals("PLAINTEXT", safeLocal.getProperty("security.protocol"));
        assertEquals("SSL", NightKafka.config(Map.of(
                "NIGHT_KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9093"))
                .getProperty("security.protocol"));
        assertThrows(IllegalArgumentException.class, () -> NightKafka.config(Map.of(
                "NIGHT_KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9093",
                "NIGHT_KAFKA_SECURITY_PROTOCOL", "SASL_SSL")));
    }

    @Test void rejectsInvalidTopicIdsAndUnsafePayloads() {
        for (String bad : List.of("", "1topic", "a/b", "..bad*", "bad topic")) {
            assertThrows(IllegalArgumentException.class, () -> NightKafka.identifier(bad));
        }
        assertThrows(IllegalArgumentException.class, () -> NightKafka.messageId("short"));
        assertThrows(IllegalArgumentException.class,
                () -> new NightKafka().publish("bad/topic", "message-12345", "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> new NightKafka().publish("orders", "short", "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> new NightKafka().subscribe("bad topic", "group"));
        assertThrows(IllegalArgumentException.class,
                () -> new NightKafka().poll(-1));
        assertThrows(IllegalArgumentException.class,
                () -> new NightKafka().poll(2001));
    }

    private static String unique(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "");
    }

    private static void topic(String topic) throws Exception {
        Properties props = NightKafka.config(System.getenv());
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
    }

    private static Interpreter.OwRecord awaitRecord(NightKafka kafka) {
        for (int i = 0; i < 30; i++) {
            var found = kafka.poll(500);
            if (Integer.valueOf(1).equals(found.get("present"))) return found;
        }
        fail("Kafka consumer did not receive the test record");
        return null;
    }

    @Test void durablePublishManualAckAndCommittedConsumerOffset() throws Exception {
        Assumptions.assumeTrue(System.getenv("NIGHT_KAFKA_BOOTSTRAP_SERVERS") != null);
        String name = unique("night_events_");
        String group = unique("night_workers_");
        topic(name);
        try (NightKafka kafka = new NightKafka()) {
            assertEquals(1, kafka.open());
            assertEquals(1, kafka.subscribe(name, group));
            assertEquals(1, kafka.subscribe(name, group));
            assertThrows(IllegalStateException.class,
                    () -> kafka.subscribe(name, unique("different_")));
            String message = "order-created-1001";
            var published = kafka.publish(name, message, "{\"orderId\":1001}");
            assertEquals(name, published.get("topic"));
            var event = awaitRecord(kafka);
            assertEquals(message, event.get("message_id"));
            assertEquals("{\"orderId\":1001}", event.get("body"));
            assertEquals(event.get("offset"), kafka.poll(0).get("offset"));
            int partition = ((Number) event.get("partition")).intValue();
            long offset = ((Number) event.get("offset")).longValue();
            assertThrows(IllegalArgumentException.class,
                    () -> kafka.acknowledge(name, partition, offset + 1, message));
            assertEquals(1, kafka.acknowledge(name, partition, offset, message));
            assertEquals(0, kafka.retryPending());
        }
        try (NightKafka restarted = new NightKafka()) {
            restarted.subscribe(name, group);
            // Poll through assignment; previous ACK must have committed offset+1.
            for (int i = 0; i < 5; i++) {
                assertEquals(0, restarted.poll(500).get("present"));
            }
        }
    }

    @Test void unacknowledgedEventsAreRedeliveredAfterRestart() throws Exception {
        Assumptions.assumeTrue(System.getenv("NIGHT_KAFKA_BOOTSTRAP_SERVERS") != null);
        String name = unique("night_recovery_");
        String group = unique("night_recovery_group_");
        topic(name);
        long pendingOffset;
        try (NightKafka failed = new NightKafka()) {
            failed.open();
            failed.publish(name, "event-11111111", "{\"attempt\":1}");
            failed.subscribe(name, group);
            var event = awaitRecord(failed);
            pendingOffset = ((Number) event.get("offset")).longValue();
            assertEquals(1, failed.retryPending());
            assertEquals(pendingOffset, ((Number) awaitRecord(failed).get("offset")).longValue());
            // Simulate worker crash before commit/ACK.
        }
        try (NightKafka recovered = new NightKafka()) {
            recovered.subscribe(name, group);
            var event = awaitRecord(recovered);
            assertEquals(pendingOffset, ((Number) event.get("offset")).longValue());
            assertEquals("event-11111111", event.get("message_id"));
            assertEquals(1, recovered.acknowledge(name,
                    ((Number) event.get("partition")).intValue(),
                    pendingOffset, "event-11111111"));
        }
    }
}
