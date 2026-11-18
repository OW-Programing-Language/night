package org.owlang.night.messaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.charset.StandardCharsets;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
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

/** Real-broker and failure-injection tests of ACK-after-quarantine ordering. */
class NightBrokerQuarantineTest {
    private static boolean postgres() {
        return System.getenv().getOrDefault("NIGHT_JDBC_URL", "").startsWith("jdbc:postgresql:");
    }

    private static String unique(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "");
    }

    private static NightJdbc database() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        Path migrations = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(migrations))
            migrations = Path.of("../examples/production/db/migration");
        assertTrue(Files.isDirectory(migrations));
        jdbc.migrate("filesystem:" + migrations.toAbsolutePath());
        return jdbc;
    }

    private static List<dev.oreo.Interpreter.OwRecord> quarantined(NightJdbc jdbc, String ns) {
        return jdbc.query("""
                SELECT broker, topic, fingerprint, reason_code, original_event_id,
                       payload, redeliveries
                  FROM night_broker_quarantine WHERE inbox_namespace = ?
                """, List.of(ns));
    }

    @Test void validatesQuarantineInputWithoutExternalConnection() {
        assertEquals(64, NightBrokerQuarantine.fingerprint("broker:topic:0:2").length());
        assertEquals("INVALID_EVENT_ID", NightConsumerBridge.invalidReason(null, "{}"));
        assertEquals("INVALID_EVENT_ID", NightConsumerBridge.invalidReason("bad", "{}"));
        assertEquals("INVALID_PAYLOAD", NightConsumerBridge.invalidReason("event-12345678", null));
        assertEquals("PAYLOAD_TOO_LARGE", NightConsumerBridge.invalidReason(
                "event-12345678", "x".repeat(262145)));
        assertNull(NightConsumerBridge.invalidReason("event-12345678", "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> NightBrokerQuarantine.fingerprint(""));
        try (NightJdbc jdbc = new NightJdbc()) {
            var store = new NightBrokerQuarantine(jdbc);
            assertThrows(IllegalArgumentException.class,
                    () -> store.capture("ns", "http", "events", "source", "INVALID_EVENT_ID", "", "{}"));
            assertThrows(IllegalArgumentException.class,
                    () -> store.capture("ns", "kafka", "events", "source", "BAD_REASON", "", "{}"));
            assertThrows(IllegalArgumentException.class,
                    () -> store.capture("ns", "kafka", "events", "source", "INVALID_EVENT_ID",
                            "bad", "x".repeat(NightBrokerQuarantine.MAX_CAPTURE_BYTES + 1)));
        }
    }

    @Test void quarantinePersistsCompleteEnvelopeAndSurvivesDuplicateRedelivery() {
        Assumptions.assumeTrue(postgres());
        String ns = unique("poison_");
        AtomicInteger acks = new AtomicInteger();
        try (NightJdbc jdbc = database()) {
            var quarantine = new NightBrokerQuarantine(jdbc);
            var inbox = new NightInbox(jdbc);
            var src = (NightConsumerBridge.Source) () -> new NightConsumerBridge.Message(
                    "orders.created", "bad", "{\"x\":1}", "kafka", "orders.created:0:11",
                    acks::incrementAndGet);
            var bridge = new NightConsumerBridge(inbox, quarantine, src, ns);
            assertEquals(NightConsumerBridge.Result.QUARANTINED, bridge.consumeOne());
            assertEquals(1, acks.get());
            assertEquals(NightConsumerBridge.Result.QUARANTINED, bridge.consumeOne());
            assertEquals(2, acks.get());
            var rows = quarantined(jdbc, ns);
            assertEquals(1, rows.size());
            assertEquals("INVALID_EVENT_ID", rows.get(0).get("reason_code"));
            assertEquals("bad", rows.get(0).get("original_event_id"));
            assertEquals("{\"x\":1}", rows.get(0).get("payload"));
            assertEquals(2, rows.get(0).get("redeliveries"));
            assertTrue(jdbc.query("SELECT event_id FROM night_inbox WHERE inbox_namespace=?",
                    List.of(ns)).isEmpty());
        }
    }

    @Test void quarantineDatabaseFailureDoesNotAcknowledgeBrokerOrDiscardEnvelope() {
        AtomicInteger acks = new AtomicInteger();
        try (NightJdbc disconnected = new NightJdbc()) {
            var src = (NightConsumerBridge.Source) () -> new NightConsumerBridge.Message(
                    "events", "", "{}", "rabbitmq", "payload-hash", acks::incrementAndGet);
            var bridge = new NightConsumerBridge(new NightInbox(disconnected),
                    new NightBrokerQuarantine(disconnected), src, "testns");
            assertThrows(IllegalStateException.class, bridge::consumeOne);
            assertEquals(0, acks.get());
        }
    }

    @Test void quarantineTooLargeToRetainDoesNotAcknowledgeBroker() {
        AtomicInteger acks = new AtomicInteger();
        Assumptions.assumeTrue(postgres());
        try (NightJdbc jdbc = database()) {
            var src = (NightConsumerBridge.Source) () -> new NightConsumerBridge.Message(
                    "events", "event-12345678",
                    "x".repeat(NightBrokerQuarantine.MAX_CAPTURE_BYTES + 1),
                    "kafka", "events:0:2", acks::incrementAndGet);
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc), src, unique("oversize_"));
            assertThrows(IllegalArgumentException.class, bridge::consumeOne);
            assertEquals(0, acks.get());
        }
    }

    @Test void realKafkaMalformedKeyIsQuarantinedThenValidRecordContinues() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_KAFKA_BOOTSTRAP_SERVERS") != null);
        String topic = unique("night_quarantine_");
        String group = unique("night_qgroup_");
        String ns = unique("kafkapoison_");
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        var props = NightKafka.config(System.getenv());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> raw = new KafkaProducer<>(props);
             NightJdbc jdbc = database();
             NightKafka consumer = new NightKafka();
             NightKafka validProducer = new NightKafka()) {
            raw.send(new ProducerRecord<String, String>(topic, null, "{\"bad\":true}"))
                    .get(20, TimeUnit.SECONDS);
            validProducer.open();
            String valid = unique("valid-");
            validProducer.publish(topic, valid, "{\"ok\":true}");
            consumer.subscribe(topic, group);
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), ns);
            NightConsumerBridge.Result first = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 30 && first == NightConsumerBridge.Result.IDLE; i++)
                first = bridge.consumeOne();
            assertEquals(NightConsumerBridge.Result.QUARANTINED, first);
            assertEquals(1, quarantined(jdbc, ns).size());
            assertNull(quarantined(jdbc, ns).get(0).get("original_event_id"));
            NightConsumerBridge.Result second = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 30 && second == NightConsumerBridge.Result.IDLE; i++)
                second = bridge.consumeOne();
            assertEquals(NightConsumerBridge.Result.STORED, second);
            assertEquals(valid, jdbc.query(
                    "SELECT event_id FROM night_inbox WHERE inbox_namespace=?",
                    List.of(ns)).get(0).get("event_id"));
        }
        // Consumer group commits the quarantine offset: no poison replay after restart.
        try (NightJdbc jdbc = database(); NightKafka restart = new NightKafka()) {
            restart.subscribe(topic, group);
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    NightConsumerBridge.kafka(restart, 500), ns);
            for (int i = 0; i < 3; i++)
                assertEquals(NightConsumerBridge.Result.IDLE, bridge.consumeOne());
        }
    }

    @Test void realRabbitMqMissingMessageIdIsQuarantinedAndValidEventContinues() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_RABBITMQ_URL") != null);
        String queue = unique("night_poison_queue_");
        String ns = unique("rabbitpoison_");
        try (NightJdbc jdbc = database(); NightRabbitMQ rabbit = new NightRabbitMQ(queue)) {
            rabbit.open(); // Declares dedicated durable queue.
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(System.getenv("NIGHT_RABBITMQ_URL"));
            try (com.rabbitmq.client.Connection conn = factory.newConnection();
                 com.rabbitmq.client.Channel channel = conn.createChannel()) {
                channel.basicPublish("", queue, true,
                        new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                        "{\"missingId\":true}".getBytes(StandardCharsets.UTF_8));
            }
            String valid = unique("valid-");
            rabbit.publish(valid, "{\"ok\":true}");
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    NightConsumerBridge.rabbitMq(rabbit, queue), ns);
            assertEquals(NightConsumerBridge.Result.QUARANTINED, bridge.consumeOne());
            assertEquals(NightConsumerBridge.Result.STORED, bridge.consumeOne());
            var poison = quarantined(jdbc, ns);
            assertEquals(1, poison.size());
            assertEquals("INVALID_EVENT_ID", poison.get(0).get("reason_code"));
            assertEquals("{\"missingId\":true}", poison.get(0).get("payload"));
            assertEquals(valid, jdbc.query(
                    "SELECT event_id FROM night_inbox WHERE inbox_namespace=?",
                    List.of(ns)).get(0).get("event_id"));
        }
    }
    @Test void rabbitDisconnectAfterQuarantineCommitRequeuesAndDeduplicates() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_RABBITMQ_URL") != null);
        String queue = unique("night_restart_queue_");
        String ns = unique("rabbit_restart_");
        try (NightJdbc jdbc = database(); NightRabbitMQ first = new NightRabbitMQ(queue)) {
            first.open();
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(System.getenv("NIGHT_RABBITMQ_URL"));
            try (com.rabbitmq.client.Connection conn = factory.newConnection();
                 com.rabbitmq.client.Channel channel = conn.createChannel()) {
                channel.basicPublish("", queue, true,
                        new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                        "{\"retry\":true}".getBytes(StandardCharsets.UTF_8));
            }

            // First consumer persists quarantine, but loses the AMQP
            // connection before broker ACK. The broker must redeliver.
            NightConsumerBridge.Source original = NightConsumerBridge.rabbitMq(first, queue);
            NightConsumerBridge.Source dropAfterInsert = () -> {
                NightConsumerBridge.Message msg = original.poll();
                if (msg == null) return null;
                return new NightConsumerBridge.Message(msg.topic(), msg.eventId(),
                        msg.payload(), msg.broker(), msg.sourceIdentity(), () -> {
                    first.close();
                    throw new IllegalStateException("Injected connection loss before ACK");
                });
            };
            var broken = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc), dropAfterInsert, ns);
            assertThrows(IllegalStateException.class, broken::consumeOne);
            assertEquals(1, quarantined(jdbc, ns).size());

            try (NightRabbitMQ recovered = new NightRabbitMQ(queue)) {
                recovered.open();
                var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                        new NightBrokerQuarantine(jdbc),
                        NightConsumerBridge.rabbitMq(recovered, queue), ns);
                NightConsumerBridge.Result outcome = NightConsumerBridge.Result.IDLE;
                for (int i = 0; i < 30 && outcome == NightConsumerBridge.Result.IDLE; i++) {
                    outcome = bridge.consumeOne();
                    if (outcome == NightConsumerBridge.Result.IDLE) Thread.sleep(50);
                }
                assertEquals(NightConsumerBridge.Result.QUARANTINED, outcome);
                assertEquals(2, quarantined(jdbc, ns).get(0).get("redeliveries"));
                assertEquals(NightConsumerBridge.Result.IDLE, bridge.consumeOne());
            }
        }
    }

    @Test void kafkaCrashAfterQuarantineCommitRedeliversWithoutLosingOffset() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_KAFKA_BOOTSTRAP_SERVERS") != null);
        String topic = unique("night_restart_");
        String group = unique("night_group_");
        String ns = unique("kafka_restart_");
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        var props = NightKafka.config(System.getenv());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<String, String>(topic, null, "{\"retry\":true}"))
                    .get(20, TimeUnit.SECONDS);
        }

        try (NightJdbc jdbc = database(); NightKafka first = new NightKafka()) {
            first.subscribe(topic, group);
            NightConsumerBridge.Source raw = NightConsumerBridge.kafka(first, 500);
            NightConsumerBridge.Source dropAfterInsert = () -> {
                NightConsumerBridge.Message msg = raw.poll();
                if (msg == null) return null;
                return new NightConsumerBridge.Message(msg.topic(), msg.eventId(),
                        msg.payload(), msg.broker(), msg.sourceIdentity(), () -> {
                    first.close();
                    throw new IllegalStateException("Injected Kafka crash before commit");
                });
            };
            var broken = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc), dropAfterInsert, ns);
            boolean found = false;
            for (int i = 0; i < 30 && !found; i++) {
                try {
                    assertEquals(NightConsumerBridge.Result.IDLE, broken.consumeOne());
                } catch (IllegalStateException expected) {
                    assertEquals("Injected Kafka crash before commit", expected.getMessage());
                    found = true;
                }
            }
            assertTrue(found);
            assertEquals(1, quarantined(jdbc, ns).size());
        }

        try (NightJdbc jdbc = database(); NightKafka recovered = new NightKafka()) {
            recovered.subscribe(topic, group);
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    NightConsumerBridge.kafka(recovered, 500), ns);
            NightConsumerBridge.Result outcome = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 30 && outcome == NightConsumerBridge.Result.IDLE; i++)
                outcome = bridge.consumeOne();
            assertEquals(NightConsumerBridge.Result.QUARANTINED, outcome);
            assertEquals(2, quarantined(jdbc, ns).get(0).get("redeliveries"));
            for (int i = 0; i < 3; i++)
                assertEquals(NightConsumerBridge.Result.IDLE, bridge.consumeOne());
        }
    }

}
