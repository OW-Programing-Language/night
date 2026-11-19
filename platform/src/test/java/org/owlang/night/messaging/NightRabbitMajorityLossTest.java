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
 * Controlled RabbitMQ quorum-queue majority loss on three actual broker nodes.
 * CI retains the current queue leader but kills both replicas before testing
 * that no new write is confirmed. This is a minority partition, not a
 * production cross-host security or network-partition certification.
 */
class NightRabbitMajorityLossTest {
    private static boolean phase(String name) {
        return name.equals(System.getProperty("night.majority.phase"));
    }
    private static String runId() {
        String value = System.getenv("NIGHT_HA_RUN_ID");
        if (value == null || !value.matches("[0-9]{1,20}"))
            throw new IllegalStateException("Missing numeric NIGHT_HA_RUN_ID");
        return value;
    }
    private static String queue() { return "night_qq_" + runId(); }
    private static String namespace() { return "rabbit_ha_inbox_" + runId(); }
    private static String event(int index) { return "event-" + runId() + "-" + index; }
    private static List<String> firstFive() {
        return List.of(event(1), event(2), event(3), event(4), event(5));
    }

    private static NightJdbc database() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        Path path = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(path)) path = Path.of("../examples/production/db/migration");
        jdbc.migrate("filesystem:" + path.toAbsolutePath());
        return jdbc;
    }

    private static List<String> ids(NightJdbc jdbc) {
        return jdbc.query("""
                SELECT event_id FROM night_inbox WHERE inbox_namespace = ?
                ORDER BY event_id
                """, List.of(namespace()))
                .stream().map(row -> (String) row.get("event_id")).toList();
    }

    @Test void remainingSingleQuorumMemberCannotConfirmWrites() {
        Assumptions.assumeTrue(phase("minority"));
        assertEquals("quorum", NightRabbitMQ.queueType(System.getenv()));
        try (NightJdbc jdbc = database(); NightRabbitMQ rabbit = new NightRabbitMQ(queue())) {
            assertEquals(firstFive(), ids(jdbc));
            // A quorum declaration can fail before publish if there is no
            // majority available; either way the caller must not see success.
            // Night uses a bounded socket timeout, so this cannot hang CI.
            assertThrows(IllegalStateException.class, () -> {
                rabbit.open();
                rabbit.publish(event(6), "{\"number\":6}");
            }, "Quorum minority must never acknowledge a durable write");
            assertEquals(firstFive(), ids(jdbc),
                    "Failed or uncertain broker publication must not mutate PostgreSQL inbox");
        }
    }

    @Test void healedQuorumRestoresConfirmedPublishAndDurableDeduplication() throws Exception {
        Assumptions.assumeTrue(phase("healed"));
        assertEquals("quorum", NightRabbitMQ.queueType(System.getenv()));
        try (NightJdbc jdbc = database(); NightRabbitMQ rabbit = new NightRabbitMQ(queue())) {
            assertEquals(firstFive(), ids(jdbc));
            boolean ready = false;
            for (int attempt = 0; attempt < 60 && !ready; attempt++) {
                try {
                    rabbit.open();
                    ready = true;
                } catch (IllegalStateException ex) {
                    Thread.sleep(500);
                }
            }
            assertTrue(ready, "Quorum did not recover after both replicas returned");
            assertEquals(1, rabbit.publish(event(7), "{\"number\":7}"));
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.rabbitMq(rabbit, queue()), namespace());
            boolean reached = false;
            for (int i = 0; i < 80 && !reached; i++) {
                var result = bridge.consumeOne();
                if (result == NightConsumerBridge.Result.IDLE) Thread.sleep(250);
                reached = ids(jdbc).contains(event(7));
            }
            assertTrue(reached, "Confirmed post-heal RabbitMQ message not ingested");
            var stored = ids(jdbc);
            for (String prior : firstFive()) assertTrue(stored.contains(prior));
            assertTrue(stored.contains(event(7)));
            assertEquals(stored.size(), stored.stream().distinct().count());
            assertTrue(stored.size() == 6 || stored.size() == 7,
                    "Only the minority-unconfirmed event 6 may be additionally present");
            rabbit.publish(event(7), "{\"forged\":true}");
            assertEquals(NightConsumerBridge.Result.DUPLICATE, bridge.consumeOne());
            var original = jdbc.query("""
                    SELECT payload FROM night_inbox WHERE inbox_namespace=? AND event_id=?
                    """, List.of(namespace(), event(7)));
            assertEquals("{\"number\":7}", original.get(0).get("payload"));
        }
    }
}
