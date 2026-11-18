package org.owlang.night.messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightInbox;
import org.owlang.night.data.NightJdbc;
import static org.junit.jupiter.api.Assertions.*;

class NightPoisonDlqTest {
    private static String unique(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "");
    }

    private static boolean postgres() {
        return System.getenv().getOrDefault("NIGHT_JDBC_URL", "").startsWith("jdbc:postgresql:");
    }

    private static NightJdbc database() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        Path migrations = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(migrations))
            migrations = Path.of("../examples/production/db/migration");
        jdbc.migrate("filesystem:" + migrations.toAbsolutePath());
        return jdbc;
    }

    @Test void destinationsAreExplicitAndCannotLoopBackOrCrossTransport() {
        assertNull(NightPoisonDlq.configuredDestination(Map.of(), "kafka", "orders"));
        assertNull(NightPoisonDlq.configuredDestination(Map.of(), "rabbitmq", "orders"));
        assertEquals("night_errors", NightPoisonDlq.configuredDestination(
                Map.of("NIGHT_CONSUMER_DLQ_TOPIC", "night_errors"), "kafka", "orders"));
        assertEquals("night_poison", NightPoisonDlq.configuredDestination(
                Map.of("NIGHT_CONSUMER_DLQ_QUEUE", "night_poison"), "rabbitmq", "orders"));
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.configuredDestination(
                Map.of("NIGHT_CONSUMER_DLQ_TOPIC", "orders"), "kafka", "orders"));
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.configuredDestination(
                Map.of("NIGHT_CONSUMER_DLQ_TOPIC", "invalid/topic"), "kafka", "orders"));
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.configuredDestination(
                Map.of("NIGHT_CONSUMER_DLQ_QUEUE", "rabbit_queue"), "kafka", "orders"));
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.configuredDestination(
                Map.of("NIGHT_CONSUMER_DLQ_TOPIC", "kafka_topic"), "rabbitmq", "orders"));
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.configuredDestination(
                Map.of(), "http", "orders"));
    }

    @Test void publishesOnlyStableReferenceAndNeverRawSensitivePayload() {
        var event = new NightConsumerBridge.Message("orders.created", "invalid",
                "{\"password\":\"secret-value\"}", "kafka", "orders.created:0:42", () -> {});
        String body = NightPoisonDlq.referenceBody(event, "INVALID_EVENT_ID");
        assertTrue(body.contains("\"reason\":\"INVALID_EVENT_ID\""));
        assertTrue(body.contains("\"source_topic\":\"orders.created\""));
        assertTrue(body.contains(NightPoisonDlq.fingerprint(event)));
        assertEquals(64, NightPoisonDlq.fingerprint(event).length());
        assertFalse(body.contains("secret-value"));
        assertFalse(body.contains("invalid"));
        assertEquals(body, NightPoisonDlq.referenceBody(event, "INVALID_EVENT_ID"));
        assertThrows(IllegalArgumentException.class,
                () -> NightPoisonDlq.referenceBody(event, "UNTRUSTED_REASON"));
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.kafka(
                new NightKafka(), "bad/topic"));
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.rabbitMq(
                new NightRabbitMQ(), "bad/topic"));
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.configuredDestination(
                Map.of("NIGHT_CONSUMER_DLQ_QUEUE", "A".repeat(65)), "rabbitmq", "orders"));
        assertThrows(IllegalStateException.class, () -> new NightRabbitMQ("other_queue")
                .publishConfirmedTo("expected_dlq", "event-12345678", "{}"));
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.kafka(
                new NightKafka(), "orders.created").publish(event, "INVALID_EVENT_ID"));
        var rabbitEvent = new NightConsumerBridge.Message("orders.created", "", "{}",
                "rabbitmq", "queue:identity", () -> {});
        assertThrows(IllegalArgumentException.class, () -> NightPoisonDlq.rabbitMq(
                new NightRabbitMQ(), "orders.created").publish(rabbitEvent, "INVALID_EVENT_ID"));
        assertThrows(IllegalArgumentException.class, () -> new NightConsumerBridge(
                new NightInbox(new NightJdbc()), null, () -> null, "orders",
                null, (message, reason) -> {}));
    }

    @Test void dlqFailurePreservesUnackedSourceAndAllowsIdempotentRetry() {
        Assumptions.assumeTrue(postgres());
        String ns = unique("dlq_retry_");
        AtomicInteger acknowledgements = new AtomicInteger();
        AtomicInteger attempts = new AtomicInteger();
        var event = new NightConsumerBridge.Message("orders", "", "{\"private\":true}",
                "kafka", "orders:0:123", acknowledgements::incrementAndGet);
        try (NightJdbc jdbc = database()) {
            var source = (NightConsumerBridge.Source) () -> event;
            var failed = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc), source, ns, null,
                    (msg, reason) -> {
                        if (attempts.incrementAndGet() == 1)
                            throw new IllegalStateException("DLQ publisher unavailable");
                    });
            assertThrows(IllegalStateException.class, failed::consumeOne);
            assertEquals(0, acknowledgements.get());
            assertEquals(1, jdbc.query(
                    "SELECT fingerprint FROM night_broker_quarantine WHERE inbox_namespace=?",
                    List.of(ns)).size());
            assertEquals(NightConsumerBridge.Result.QUARANTINED, failed.consumeOne());
            assertEquals(1, acknowledgements.get());
            var rows = jdbc.query("""
                    SELECT fingerprint, redeliveries, payload
                      FROM night_broker_quarantine WHERE inbox_namespace=?
                    """, List.of(ns));
            assertEquals(1, rows.size());
            assertEquals(2, ((Number) rows.get(0).get("redeliveries")).intValue());
            assertEquals("{\"private\":true}", rows.get(0).get("payload"));
            assertEquals(NightPoisonDlq.fingerprint(event), rows.get(0).get("fingerprint"));
        }
    }

    @Test void rabbitMqBrokerDlqReferencePublishedBeforeSourceAck() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_RABBITMQ_URL") != null);
        String sourceQueue = unique("night_dlq_source_");
        String dlqQueue = unique("night_dlq_target_");
        String ns = unique("rabbit_dlq_");
        try (NightJdbc jdbc = database();
             NightRabbitMQ source = new NightRabbitMQ(sourceQueue);
             NightRabbitMQ target = new NightRabbitMQ(dlqQueue)) {
            source.open();
            target.open();
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(System.getenv("NIGHT_RABBITMQ_URL"));
            try (com.rabbitmq.client.Connection conn = factory.newConnection();
                 com.rabbitmq.client.Channel channel = conn.createChannel()) {
                channel.basicPublish("", sourceQueue, true,
                        new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                        "{\"sensitive\":\"body\"}".getBytes(StandardCharsets.UTF_8));
            }
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    NightConsumerBridge.rabbitMq(source, sourceQueue),
                    ns, null, NightPoisonDlq.rabbitMq(target, dlqQueue));
            assertEquals(NightConsumerBridge.Result.QUARANTINED, bridge.consumeOne());
            var record = target.poll();
            assertEquals(1, record.get("present"));
            String referenceId = (String) record.get("message_id");
            assertEquals(64, referenceId.length());
            assertTrue(((String) record.get("body")).contains(referenceId));
            assertFalse(((String) record.get("body")).contains("sensitive"));
            assertEquals(referenceId, jdbc.query(
                    "SELECT fingerprint FROM night_broker_quarantine WHERE inbox_namespace=?",
                    List.of(ns)).get(0).get("fingerprint"));
            target.ack(((Number) record.get("delivery_tag")).longValue());
            assertEquals(0, source.poll().get("present"));
        }
    }

    @Test void kafkaDlqReferenceConfirmedBeforeSourceOffsetCommit() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_KAFKA_BOOTSTRAP_SERVERS") != null);
        String sourceTopic = unique("night_dlq_source_");
        String dlqTopic = unique("night_dlq_target_");
        String group = unique("night_dlq_group_");
        String ns = unique("kafka_dlq_");
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            admin.createTopics(List.of(
                    new NewTopic(sourceTopic, 1, (short) 1),
                    new NewTopic(dlqTopic, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        Properties props = NightKafka.config(System.getenv());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> raw = new KafkaProducer<>(props);
             NightJdbc jdbc = database();
             NightKafka source = new NightKafka();
             NightKafka dlq = new NightKafka();
             NightKafka reader = new NightKafka()) {
            raw.send(new ProducerRecord<String, String>(sourceTopic, null,
                    "{\"secret\":\"confidential\"}")).get(20, TimeUnit.SECONDS);
            dlq.open();
            source.subscribe(sourceTopic, group);
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    NightConsumerBridge.kafka(source, 500),
                    ns, null, NightPoisonDlq.kafka(dlq, dlqTopic));
            NightConsumerBridge.Result outcome = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 30 && outcome == NightConsumerBridge.Result.IDLE; i++)
                outcome = bridge.consumeOne();
            assertEquals(NightConsumerBridge.Result.QUARANTINED, outcome);
            reader.subscribe(dlqTopic, unique("night_dlq_readers_"));
            dev.oreo.Interpreter.OwRecord record = null;
            for (int i = 0; i < 30; i++) {
                var next = reader.poll(500);
                if (Integer.valueOf(1).equals(next.get("present"))) {
                    record = next;
                    break;
                }
            }
            assertNotNull(record);
            String fingerprint = (String) record.get("message_id");
            assertTrue(((String) record.get("body")).contains(fingerprint));
            assertFalse(((String) record.get("body")).contains("confidential"));
            assertEquals(fingerprint, jdbc.query(
                    "SELECT fingerprint FROM night_broker_quarantine WHERE inbox_namespace=?",
                    List.of(ns)).get(0).get("fingerprint"));
            reader.acknowledge(dlqTopic,
                    ((Number) record.get("partition")).intValue(),
                    ((Number) record.get("offset")).longValue(), fingerprint);
        }
        // Source group offset is committed only after DB and DLQ success.
        try (NightKafka restart = new NightKafka()) {
            restart.subscribe(sourceTopic, group);
            for (int i = 0; i < 3; i++)
                assertEquals(0, restart.poll(500).get("present"));
        }
    }
}
