package org.owlang.night.messaging;

import dev.oreo.Interpreter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightJdbc;
import org.owlang.night.data.NightOutbox;
import static org.junit.jupiter.api.Assertions.*;

class NightOutboxDispatcherTest {
    private static String id() {
        return "outbox-" + UUID.randomUUID().toString().replace("-", "");
    }

    private static boolean postgres() {
        return System.getenv().getOrDefault("NIGHT_JDBC_URL", "").startsWith("jdbc:postgresql:");
    }

    private static NightJdbc jdbc() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        Path migrations = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(migrations))
            migrations = Path.of("../examples/production/db/migration");
        assertTrue(Files.isDirectory(migrations), "Missing Night production SQL migrations");
        jdbc.migrate("filesystem:" + migrations.toAbsolutePath());
        return jdbc;
    }

    private static void enqueue(NightJdbc jdbc, String eventId, String topic, String payload) {
        NightOutbox outbox = new NightOutbox(jdbc);
        jdbc.begin();
        assertEquals(1, outbox.enqueue(eventId, topic, payload));
        jdbc.commit();
    }

    private static NightOutboxDispatcher.Policy policy(int maxAttempts) {
        return new NightOutboxDispatcher.Policy(30, maxAttempts, 1, 4);
    }

    private static String state(NightJdbc jdbc, String eventId) {
        return (String) jdbc.query("SELECT state FROM night_outbox WHERE event_id = ?",
                List.of(eventId)).get(0).get("state");
    }

    @Test void enforcesExplicitPolicyAndNoImplicitDestination() {
        assertThrows(IllegalArgumentException.class,
                () -> new NightOutboxDispatcher.Policy(1, 8, 1, 5));
        assertThrows(IllegalArgumentException.class,
                () -> new NightOutboxDispatcher.Policy(60, 0, 1, 5));
        assertThrows(IllegalArgumentException.class,
                () -> new NightOutboxDispatcher.Policy(60, 8, 5, 4));
        assertEquals(1, policy(4).backoffSeconds(1));
        assertEquals(2, policy(4).backoffSeconds(2));
        assertEquals(4, policy(4).backoffSeconds(3));
        assertEquals(4, policy(4).backoffSeconds(100));
        assertEquals("kafka", NightOutboxWorker.brokerSetting(Map.of("NIGHT_OUTBOX_BROKER", "kafka")));
        assertEquals("rabbitmq", NightOutboxWorker.brokerSetting(Map.of("NIGHT_OUTBOX_BROKER", "rabbitmq")));
        assertThrows(IllegalArgumentException.class,
                () -> NightOutboxWorker.brokerSetting(Map.of("NIGHT_OUTBOX_BROKER", "both")));
        assertThrows(IllegalArgumentException.class,
                () -> NightOutboxWorker.integerSetting(Map.of("NIGHT_OUTBOX_MAX_ATTEMPTS", "99999"),
                        "NIGHT_OUTBOX_MAX_ATTEMPTS", 8, 1, 1000));
    }

    @Test void handlesDeliveryFailureExponentialBackoffDeadLettersAndManualReplay() throws Exception {
        Assumptions.assumeTrue(postgres());
        try (NightJdbc jdbc = jdbc()) {
            NightOutbox outbox = new NightOutbox(jdbc);
            String eventId = id();
            enqueue(jdbc, eventId, "orders.created", "{\"order\":42}");
            AtomicInteger sends = new AtomicInteger();
            NightOutboxDispatcher dispatcher = new NightOutboxDispatcher(
                    outbox, (e, topic, payload) -> {
                        sends.incrementAndGet();
                        assertEquals(eventId, e);
                        assertEquals("orders.created", topic);
                        assertEquals("{\"order\":42}", payload);
                        if (sends.get() < 3)
                            throw new IllegalStateException("simulated broker outage");
                    }, "night-worker-a", policy(2));

            assertEquals("RETRY", dispatcher.dispatchOne().status());
            assertEquals("PENDING", state(jdbc, eventId));
            assertEquals("IDLE", dispatcher.dispatchOne().status());
            Thread.sleep(1250);
            assertEquals("DEAD", dispatcher.dispatchOne().status());
            assertEquals("DEAD", state(jdbc, eventId));
            assertEquals("IDLE", dispatcher.dispatchOne().status());
            assertEquals(1, outbox.replayDead(eventId));
            assertEquals("DELIVERED", dispatcher.dispatchOne().status());
            assertEquals("DELIVERED", state(jdbc, eventId));
            assertEquals(3, sends.get());
            assertEquals("IDLE", dispatcher.dispatchOne().status());
        }
    }

    @Test void expiredWorkerLeasesAreReclaimedOrDeadLetteredAtAttemptLimit() throws Exception {
        Assumptions.assumeTrue(postgres());
        try (NightJdbc jdbc = jdbc()) {
            NightOutbox outbox = new NightOutbox(jdbc);
            String eventId = id();
            enqueue(jdbc, eventId, "orders.created", "{}");
            assertEquals(1, outbox.claim("night-crashed-worker", 1).size());
            // Simulate a crash AFTER the broker published, BEFORE outbox ACK.
            // A later worker should publish again, requiring downstream dedup.
            Thread.sleep(1250);
            AtomicInteger duplicatePublications = new AtomicInteger(1);
            NightOutboxDispatcher recovering = new NightOutboxDispatcher(
                    outbox, (e, t, p) -> duplicatePublications.incrementAndGet(),
                    "night-recovery-worker", policy(3));
            assertEquals("DELIVERED", recovering.dispatchOne().status());
            assertEquals(2, duplicatePublications.get());
            assertEquals(0, outbox.acknowledge(eventId, "night-crashed-worker"));
            assertEquals("DELIVERED", state(jdbc, eventId));

            String exhausted = id();
            enqueue(jdbc, exhausted, "orders.created", "{}");
            assertEquals(1, outbox.claim("night-stuck-worker", 1).size());
            Thread.sleep(1250);
            NightOutboxDispatcher bounded = new NightOutboxDispatcher(
                    outbox, (e, t, p) -> fail("Exhausted lease must not be republished"),
                    "night-exhaustion-worker", policy(1));
            assertEquals("IDLE", bounded.dispatchOne().status());
            assertEquals("DEAD", state(jdbc, exhausted));
        }
    }

    @Test void deliversActualRabbitMqWithBrokerConfirmation() {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_RABBITMQ_URL") != null);
        try (NightJdbc jdbc = jdbc(); NightRabbitMQ broker = new NightRabbitMQ()) {
            broker.open();
            String eventId = id();
            enqueue(jdbc, eventId, "orders.created", "{\"broker\":\"rabbit\"}");
            var dispatcher = new NightOutboxDispatcher(new NightOutbox(jdbc),
                    (e, t, p) -> assertEquals(1, broker.publish(e, p)),
                    "night-rabbit-dispatch", policy(3));
            assertEquals("DELIVERED", dispatcher.dispatchOne().status());
            var received = broker.poll();
            assertEquals(1, received.get("present"));
            assertEquals(eventId, received.get("message_id"));
            assertEquals("{\"broker\":\"rabbit\"}", received.get("body"));
            broker.ack(((Number) received.get("delivery_tag")).longValue());
            assertEquals("DELIVERED", state(jdbc, eventId));
        }
    }

    @Test void deliversToActualKafkaAndIsConsumedByGroup() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_KAFKA_BOOTSTRAP_SERVERS") != null);
        String topic = "night_outbox_" + UUID.randomUUID().toString().replace("-", "");
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        try (NightJdbc jdbc = jdbc(); NightKafka publisher = new NightKafka();
             NightKafka consumer = new NightKafka()) {
            String eventId = id();
            enqueue(jdbc, eventId, topic, "{\"broker\":\"kafka\"}");
            publisher.open();
            var dispatcher = new NightOutboxDispatcher(new NightOutbox(jdbc),
                    (e, t, p) -> publisher.publish(t, e, p),
                    "night-kafka-dispatch", policy(3));
            assertEquals("DELIVERED", dispatcher.dispatchOne().status());
            consumer.subscribe(topic, "night_test_consumers_" + UUID.randomUUID().toString().replace("-", ""));
            Interpreter.OwRecord event = null;
            for (int attempt = 0; attempt < 30; attempt++) {
                var received = consumer.poll(500);
                if (Integer.valueOf(1).equals(received.get("present"))) {
                    event = received;
                    break;
                }
            }
            assertNotNull(event);
            assertEquals(eventId, event.get("message_id"));
            assertEquals("{\"broker\":\"kafka\"}", event.get("body"));
            assertEquals(1, consumer.acknowledge(topic,
                    ((Number) event.get("partition")).intValue(),
                    ((Number) event.get("offset")).longValue(), eventId));
            assertEquals("DELIVERED", state(jdbc, eventId));
        }
    }
}
