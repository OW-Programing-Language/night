package org.owlang.night.messaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightInbox;
import org.owlang.night.data.NightJdbc;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Quorum queue leader failover on three real, separate RabbitMQ processes.
 * CI changes the RabbitMQ endpoint between JVM phases after killing the
 * actual queue leader identified by the management API.
 */
class NightRabbitQuorumFailoverTest {
    private static boolean phase(String expected) {
        return expected.equals(System.getProperty("night.ha.phase"));
    }
    private static String runId() {
        String id = System.getenv("NIGHT_HA_RUN_ID");
        if (id == null || !id.matches("[0-9]{1,20}"))
            throw new IllegalStateException("NIGHT_HA_RUN_ID must be CI numeric");
        return id;
    }
    private static String queue() { return "night_qq_" + runId(); }
    private static String namespace() { return "rabbit_ha_inbox_" + runId(); }
    private static String event(int i) { return "event-" + runId() + "-" + i; }

    private static NightJdbc database() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        Path migrations = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(migrations)) migrations = Path.of("../examples/production/db/migration");
        assertTrue(Files.isDirectory(migrations), "Missing Night SQL migrations");
        jdbc.migrate("filesystem:" + migrations.toAbsolutePath());
        return jdbc;
    }

    private static NightRabbitMQ connect() throws InterruptedException {
        assertEquals("quorum", NightRabbitMQ.queueType(System.getenv()));
        NightRabbitMQ client = new NightRabbitMQ(queue());
        for (int retry = 0; retry < 40; retry++) {
            try {
                assertEquals(1, client.open());
                return client;
            } catch (IllegalStateException ex) {
                if (retry == 39) throw ex;
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException("RabbitMQ quorum queue unavailable");
    }

    private static NightConsumerBridge.Result await(NightConsumerBridge bridge)
            throws InterruptedException {
        for (int i = 0; i < 60; i++) {
            var result = bridge.consumeOne();
            if (result != NightConsumerBridge.Result.IDLE) return result;
            Thread.sleep(250);
        }
        fail("RabbitMQ quorum failed to redeliver within bounded window");
        return NightConsumerBridge.Result.IDLE;
    }

    private static List<String> storedIds(NightJdbc jdbc) {
        return jdbc.query("SELECT event_id FROM night_inbox WHERE inbox_namespace=? ORDER BY event_id",
                List.of(namespace())).stream().map(x -> (String) x.get("event_id")).toList();
    }

    @Test void validateQueueTypeAndOptInSafety() {
        assertEquals("classic", NightRabbitMQ.queueType(java.util.Map.of()));
        assertEquals("quorum", NightRabbitMQ.queueType(
                java.util.Map.of("NIGHT_RABBITMQ_QUEUE_TYPE", "quorum")));
        assertThrows(IllegalArgumentException.class, () -> NightRabbitMQ.queueType(
                java.util.Map.of("NIGHT_RABBITMQ_QUEUE_TYPE", "stream")));
    }

    @Test void beforeLeaderLossPublishesAndLeavesUnackedQuorumDelivery() throws Exception {
        Assumptions.assumeTrue(phase("before"));
        try (NightJdbc jdbc = database(); NightRabbitMQ rabbit = connect()) {
            for (int i = 1; i <= 3; i++)
                assertEquals(1, rabbit.publish(event(i), "{\"number\":" + i + "}"));
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.rabbitMq(rabbit, queue()), namespace());
            assertEquals(NightConsumerBridge.Result.STORED, await(bridge));
            assertEquals(List.of(event(1)), storedIds(jdbc));
            var pending = rabbit.poll();
            assertEquals(1, pending.get("present"));
            assertEquals(event(2), pending.get("message_id"));
            // On close, the pending quorum delivery is requeued.
        }
    }

    @Test void remainingQuorumMajorityElectsLeaderAndPreservesEvents() throws Exception {
        Assumptions.assumeTrue(phase("after"));
        try (NightJdbc jdbc = database(); NightRabbitMQ rabbit = connect()) {
            assertEquals(List.of(event(1)), storedIds(jdbc));
            // Verified new writes after leader loss prove live quorum progress.
            assertEquals(1, rabbit.publish(event(4), "{\"number\":4}"));
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.rabbitMq(rabbit, queue()), namespace());
            for (int i = 2; i <= 4; i++)
                assertEquals(NightConsumerBridge.Result.STORED, await(bridge));
            assertEquals(List.of(event(1), event(2), event(3), event(4)), storedIds(jdbc));
            rabbit.publish(event(2), "{\"forged\":true}");
            assertEquals(NightConsumerBridge.Result.DUPLICATE, await(bridge));
            assertEquals(4, storedIds(jdbc).size());
            var record = jdbc.query("""
                    SELECT payload FROM night_inbox WHERE inbox_namespace=? AND event_id=?
                    """, List.of(namespace(), event(2)));
            assertEquals("{\"number\":2}", record.get(0).get("payload"));
        }
    }

    @Test void recoveredFormerLeaderJoinsAndConfirmsNewEvent() throws Exception {
        Assumptions.assumeTrue(phase("return"));
        try (NightJdbc jdbc = database(); NightRabbitMQ rabbit = connect()) {
            assertEquals(1, rabbit.publish(event(5), "{\"number\":5}"));
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.rabbitMq(rabbit, queue()), namespace());
            assertEquals(NightConsumerBridge.Result.STORED, await(bridge));
            assertEquals(List.of(event(1), event(2), event(3), event(4), event(5)), storedIds(jdbc));
        }
    }
    @Test void minorityCannotConfirmNewQuorumMessage() throws Exception {
        Assumptions.assumeTrue(phase("minority"));
        // Both rmq-2 and rmq-3 are stopped, leaving only node 1. Depending
        // on which member last led the queue, declaration may itself fail or
        // publication may time out. In either case there must be no confirm.
        try (NightJdbc jdbc = database()) {
            assertEquals(List.of(event(1), event(2), event(3), event(4), event(5)),
                    storedIds(jdbc));
            boolean published = false;
            try (NightRabbitMQ rabbit = new NightRabbitMQ(queue())) {
                rabbit.open();
                rabbit.publish(event(6), "{\"number\":6}");
                published = true;
            } catch (IllegalStateException expected) {
                // Quorum unavailable: fail closed without accepting work.
            }
            assertFalse(published, "A single quorum replica must never confirm a write");
            assertEquals(List.of(event(1), event(2), event(3), event(4), event(5)),
                    storedIds(jdbc));
        }
    }

    @Test void restoredQuorumContinuesWithStableMessageIdAndNoDuplicateInboxRow()
            throws Exception {
        Assumptions.assumeTrue(phase("healed"));
        try (NightJdbc jdbc = database(); NightRabbitMQ rabbit = connect()) {
            // RabbitMQ publisher timeout is ambiguous: retry with the SAME
            // application ID, then assert PostgreSQL idempotent acceptance.
            assertEquals(1, rabbit.publish(event(6), "{\"number\":6}"));
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.rabbitMq(rabbit, queue()), namespace());
            var result = await(bridge);
            assertTrue(result == NightConsumerBridge.Result.STORED
                    || result == NightConsumerBridge.Result.DUPLICATE);
            assertEquals(List.of(event(1), event(2), event(3), event(4), event(5), event(6)),
                    storedIds(jdbc));
            var rows = jdbc.query("SELECT payload FROM night_inbox WHERE inbox_namespace=? AND event_id=?",
                    List.of(namespace(), event(6)));
            assertEquals(1, rows.size());
            assertEquals("{\"number\":6}", rows.get(0).get("payload"));
        }
    }

}
