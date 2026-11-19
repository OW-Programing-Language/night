package org.owlang.night.messaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightInbox;
import org.owlang.night.data.NightJdbc;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two separate Maven invocations, with a REAL broker container restart
 * between them. Unlike a mock, phases share the same durable PostgreSQL DB,
 * RabbitMQ queue/Kafka group, and topic, but never reuse the same JVM.
 *
 * These are single-node broker restart tests, not multi-node HA certification.
 */
class NightBrokerRestartTest {
    private static final String PREFIX = "night_restart_";

    private static boolean phase(String broker, String step) {
        return broker.equals(System.getProperty("night.failover.transport"))
                && step.equals(System.getProperty("night.failover.phase"));
    }

    private static String runId() {
        String run = System.getenv("NIGHT_FAILOVER_RUN_ID");
        if (run == null || !run.matches("[0-9]{1,20}"))
            throw new IllegalArgumentException("NIGHT_FAILOVER_RUN_ID must contain 1..20 digits");
        return run;
    }

    private static String source() { return PREFIX + runId(); }
    private static String namespace() { return "restarted_" + runId(); }
    private static String group() { return "restarted_group_" + runId(); }
    private static String event(int ordinal) { return "event-" + runId() + "-" + ordinal; }

    private static NightJdbc database() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        Path migrations = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(migrations))
            migrations = Path.of("../examples/production/db/migration");
        assertTrue(Files.isDirectory(migrations), "Night migrations must be present");
        jdbc.migrate("filesystem:" + migrations.toAbsolutePath());
        return jdbc;
    }

    private static List<String> storedIds(NightJdbc jdbc) {
        var records = jdbc.query("""
                SELECT event_id FROM night_inbox WHERE inbox_namespace = ?
                 ORDER BY event_id
                """, List.of(namespace()));
        return records.stream().map(row -> (String) row.get("event_id")).toList();
    }

    private static void verifyAllEventsPresent(NightJdbc jdbc) {
        assertEquals(List.of(event(1), event(2), event(3)), storedIds(jdbc));
        var rows = jdbc.query("""
                SELECT topic, state, attempts FROM night_inbox WHERE inbox_namespace = ?
                """, List.of(namespace()));
        assertEquals(3, rows.size());
        for (var row : rows) {
            assertEquals(source(), row.get("topic"));
            assertEquals("PENDING", row.get("state"));
            assertEquals(0, ((Number) row.get("attempts")).intValue());
        }
    }

    private static NightConsumerBridge.Result awaitDelivery(NightConsumerBridge bridge) {
        for (int i = 0; i < 30; i++) {
            NightConsumerBridge.Result outcome = bridge.consumeOne();
            if (outcome != NightConsumerBridge.Result.IDLE) return outcome;
        }
        fail("Broker did not redeliver within 15 seconds");
        return null;
    }

    @Test void kafkaBeforeRestartCommitsOneAndLeavesAnotherUnacknowledged() throws Exception {
        Assumptions.assumeTrue(phase("kafka", "before"));
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            admin.createTopics(List.of(new NewTopic(source(), 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        try (NightJdbc jdbc = database();
             NightKafka producer = new NightKafka();
             NightKafka consumer = new NightKafka()) {
            producer.open();
            for (int i = 1; i <= 3; i++)
                producer.publish(source(), event(i), "{\"ordinal\":" + i + "}");
            consumer.subscribe(source(), group());

            NightConsumerBridge bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), namespace());
            assertEquals(NightConsumerBridge.Result.STORED, awaitDelivery(bridge));

            // Exactly one offset was committed. Read a second record but never
            // ACK it; closing the consumer simulates a worker dying pre-commit.
            var unacked = consumer.poll(500);
            assertEquals(1, unacked.get("present"));
            assertEquals(event(2), unacked.get("message_id"));
            assertEquals(List.of(event(1)), storedIds(jdbc));
        }
    }

    @Test void kafkaAfterBrokerRestartReplaysUncommittedButNotCommitted() {
        Assumptions.assumeTrue(phase("kafka", "after"));
        try (NightJdbc jdbc = database(); NightKafka consumer = new NightKafka()) {
            assertEquals(List.of(event(1)), storedIds(jdbc));
            consumer.subscribe(source(), group());
            NightConsumerBridge bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), namespace());
            assertEquals(NightConsumerBridge.Result.STORED, awaitDelivery(bridge));
            assertEquals(NightConsumerBridge.Result.STORED, awaitDelivery(bridge));
            verifyAllEventsPresent(jdbc);
            for (int i = 0; i < 4; i++)
                assertEquals(NightConsumerBridge.Result.IDLE, bridge.consumeOne());
        }
        // A further consumer restart must not deliver any already committed
        // event; this also checks that the group offset survived broker restart.
        try (NightKafka restarted = new NightKafka()) {
            restarted.subscribe(source(), group());
            for (int i = 0; i < 3; i++)
                assertEquals(0, restarted.poll(500).get("present"));
        }
    }

    @Test void rabbitBeforeRestartPersistsFirstThenLeavesSecondUnacked() {
        Assumptions.assumeTrue(phase("rabbitmq", "before"));
        try (NightJdbc jdbc = database(); NightRabbitMQ client = new NightRabbitMQ(source())) {
            client.open(); // Declares durable queue.
            for (int i = 1; i <= 3; i++)
                assertEquals(1, client.publish(event(i), "{\"ordinal\":" + i + "}"));

            NightConsumerBridge bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.rabbitMq(client, source()), namespace());
            assertEquals(NightConsumerBridge.Result.STORED, bridge.consumeOne());
            var unacked = client.poll();
            assertEquals(1, unacked.get("present"));
            assertEquals(event(2), unacked.get("message_id"));
            assertEquals(List.of(event(1)), storedIds(jdbc));
            // Closing the connection requeues the unacked persistent delivery.
        }
    }

    @Test void rabbitAfterRestartRequeuesUnackedAndPreservesDurableMessages() {
        Assumptions.assumeTrue(phase("rabbitmq", "after"));
        try (NightJdbc jdbc = database(); NightRabbitMQ client = new NightRabbitMQ(source())) {
            client.open();
            assertEquals(List.of(event(1)), storedIds(jdbc));
            NightConsumerBridge bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.rabbitMq(client, source()), namespace());
            assertEquals(NightConsumerBridge.Result.STORED, bridge.consumeOne());
            assertEquals(NightConsumerBridge.Result.STORED, bridge.consumeOne());
            verifyAllEventsPresent(jdbc);
            assertEquals(NightConsumerBridge.Result.IDLE, bridge.consumeOne());
        }
    }
}
