package org.owlang.night.messaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightInbox;
import org.owlang.night.data.NightJdbc;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests consumer-group transfer to a second worker after the first closes
 * with an uncommitted record, while PostgreSQL remains the durable inbox.
 * This is not a broker node outage or multi-node cluster certification.
 */
class NightKafkaGroupHandoffTest {
    private static String unique(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "");
    }

    private static NightJdbc db() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        Path migrations = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(migrations))
            migrations = Path.of("../examples/production/db/migration");
        assertTrue(Files.isDirectory(migrations));
        jdbc.migrate("filesystem:" + migrations.toAbsolutePath());
        return jdbc;
    }

    @Test void secondLiveGroupMemberTakesOverUncommittedPartitionWithInboxDedup() throws Exception {
        Assumptions.assumeTrue(System.getenv("NIGHT_KAFKA_BOOTSTRAP_SERVERS") != null
                && System.getenv().getOrDefault("NIGHT_JDBC_URL", "")
                    .startsWith("jdbc:postgresql:"));
        String topic = unique("night_handoff_");
        String group = unique("night_handoff_group_");
        String namespace = unique("handoff_");
        String messageId = unique("event-");
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        try (NightJdbc jdbc = db();
             NightKafka producer = new NightKafka();
             NightKafka first = new NightKafka();
             NightKafka second = new NightKafka()) {
            producer.open();
            producer.publish(topic, messageId, "{\"handoff\":true}");
            first.subscribe(topic, group);
            dev.oreo.Interpreter.OwRecord pending = null;
            for (int i = 0; i < 30; i++) {
                var next = first.poll(500);
                if (((Number) next.get("present")).intValue() == 1) {
                    pending = next;
                    break;
                }
            }
            assertNotNull(pending, "First worker did not receive its assigned partition");
            assertEquals(messageId, pending.get("message_id"));
            second.subscribe(topic, group);
            // The first group member exits while holding an uncommitted
            // record. The second must take over its partition and redeliver.
            first.close();
            NightConsumerBridge bridge = new NightConsumerBridge(
                    new NightInbox(jdbc), NightConsumerBridge.kafka(second, 500), namespace);
            NightConsumerBridge.Result result = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 60 && result == NightConsumerBridge.Result.IDLE; i++)
                result = bridge.consumeOne();
            assertEquals(NightConsumerBridge.Result.STORED, result);
            assertEquals(1, jdbc.query(
                    "SELECT event_id FROM night_inbox WHERE inbox_namespace=? AND event_id=?",
                    List.of(namespace, messageId)).size());

            // Kafka publication is at-least-once: a retry of the stable event
            // ID must not create a second PostgreSQL inbox entry.
            producer.publish(topic, messageId, "{\"handoff\":false}");
            result = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 60 && result == NightConsumerBridge.Result.IDLE; i++)
                result = bridge.consumeOne();
            assertEquals(NightConsumerBridge.Result.DUPLICATE, result);
            var stored = jdbc.query("SELECT payload FROM night_inbox WHERE inbox_namespace=?",
                    List.of(namespace));
            assertEquals(1, stored.size());
            assertEquals("{\"handoff\":true}", stored.get(0).get("payload"));
        }
        // Further restart preserves the offset committed by the second worker.
        try (NightKafka restart = new NightKafka()) {
            restart.subscribe(topic, group);
            for (int i = 0; i < 3; i++)
                assertEquals(0, restart.poll(500).get("present"));
        }
    }
}
