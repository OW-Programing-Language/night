package org.owlang.night.messaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightInbox;
import org.owlang.night.data.NightJdbc;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Controlled 3 -> 1 -> 3 Kafka/KRaft majority loss, with distinct test JVMs.
 * The workflow keeps the CURRENT partition leader running and SIGKILLs both
 * other broker/controllers. With min.insync.replicas=2, an acks=all producer
 * must not receive a successful confirmation on the minority.
 */
class NightKafkaMajorityLossTest {
    private static boolean phase(String value) {
        return value.equals(System.getProperty("night.majority.phase"));
    }
    private static String runId() {
        String value = System.getenv("NIGHT_HA_RUN_ID");
        if (value == null || !value.matches("[0-9]{1,20}"))
            throw new IllegalStateException("Missing numeric NIGHT_HA_RUN_ID");
        return value;
    }
    private static String topic() { return "night_ha_" + runId(); }
    private static String namespace() { return "night_ha_inbox_" + runId(); }
    private static String group() { return "night_ha_group_" + runId(); }
    private static String event(int index) { return "event-" + runId() + "-" + index; }

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

    private static List<String> firstFive() {
        return List.of(event(1), event(2), event(3), event(4), event(5));
    }

    private static int currentLeaderAndIsr(int expected) throws Exception {
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            for (int attempt = 0; attempt < 90; attempt++) {
                try {
                    TopicDescription desc = admin.describeTopics(List.of(topic()))
                            .allTopicNames().get(10, TimeUnit.SECONDS).get(topic());
                    assertEquals(1, desc.partitions().size());
                    var partition = desc.partitions().get(0);
                    if (partition.leader() != null && partition.leader().id() > 0
                            && partition.isr().size() == expected)
                        return partition.leader().id();
                } catch (java.util.concurrent.ExecutionException ex) {
                    Throwable cause = ex.getCause();
                    if (!(cause instanceof org.apache.kafka.common.errors.LeaderNotAvailableException)
                            && !(cause instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException))
                        throw ex;
                }
                Thread.sleep(1000);
            }
        }
        fail("Kafka never restored expected ISR=" + expected);
        return -1;
    }

    @Test void prepareRecordsCurrentLeaderWithoutMutatingBrokerOrInbox() throws Exception {
        Assumptions.assumeTrue(phase("prepare"));
        int leader = currentLeaderAndIsr(3);
        String path = System.getProperty("night.majority.leaderFile");
        if (path == null || !Path.of(path).isAbsolute())
            throw new IllegalArgumentException("night.majority.leaderFile must be absolute");
        try (NightJdbc jdbc = database()) {
            assertEquals(firstFive(), ids(jdbc));
        }
        Files.writeString(Path.of(path), Integer.toString(leader));
    }

    @Test void minorityMustNotConfirmDurablePublication() {
        Assumptions.assumeTrue(phase("minority"));
        try (NightJdbc jdbc = database(); NightKafka producer = new NightKafka()) {
            assertEquals(firstFive(), ids(jdbc));
            producer.open();
            // One live replica cannot satisfy min.insync.replicas=2. Timeout
            // does NOT prove the record was never written: an uncertain ACK
            // must be reconciled by stable event ID after quorum restoration.
            assertThrows(IllegalStateException.class, () -> producer.publish(
                    topic(), event(6), "{\"number\":6}"),
                    "Minority must never provide a confirmed acks=all publication");
            assertEquals(firstFive(), ids(jdbc),
                    "No uncommitted/uncertain broker event may mutate Night inbox");
        }
    }

    @Test void healedMajorityRestoresConfirmationAndExactlyOneInboxRowPerEvent() throws Exception {
        Assumptions.assumeTrue(phase("healed"));
        currentLeaderAndIsr(3);
        try (NightJdbc jdbc = database(); NightKafka producer = new NightKafka();
             NightKafka consumer = new NightKafka()) {
            assertEquals(firstFive(), ids(jdbc));
            producer.open();
            // Event 6 was unconfirmed; it may still have reached the broker.
            // Do not assert loss, and do not automatically replay the uncertain
            // event. Use a new event ID to prove confirmed progress.
            producer.publish(topic(), event(7), "{\"number\":7}");
            consumer.subscribe(topic(), group());
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), namespace());
            boolean confirmedSeen = false;
            for (int attempt = 0; attempt < 60 && !confirmedSeen; attempt++) {
                var result = bridge.consumeOne();
                if (result == NightConsumerBridge.Result.STORED
                        || result == NightConsumerBridge.Result.DUPLICATE)
                    confirmedSeen = ids(jdbc).contains(event(7));
            }
            assertTrue(confirmedSeen, "Confirmed event 7 was lost after majority restored");
            List<String> records = ids(jdbc);
            for (String existing : firstFive())
                assertTrue(records.contains(existing), "Lost committed event: " + existing);
            assertTrue(records.contains(event(7)));
            assertEquals(records.size(), records.stream().distinct().count());
            assertTrue(records.size() == 6 || records.size() == 7,
                    "Only the optional unconfirmed event 6 may be present");
        }
    }
}
