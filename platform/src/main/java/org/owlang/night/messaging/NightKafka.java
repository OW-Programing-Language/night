package org.owlang.night.messaging;

import dev.oreo.Interpreter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Experimental OW Kafka adapter. One synchronous in-flight event per consumer
 * protects manual commits from skipping an unacknowledged partition offset.
 *
 * Does not implement exactly-once semantics, worker supervision, consumer
 * fencing, retries with dead letters, or transactional outbox dispatch.
 * OW callers should run this in a dedicated worker, not in an HTTP handler.
 */
public final class NightKafka implements AutoCloseable {
    private static final int MAX_PAYLOAD_BYTES = 262_144;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private ConsumerRecord<String, String> pending;
    private String subscribedTopic;
    private String subscribedGroup;

    public static String identifier(String raw) {
        if (raw == null || !raw.matches("[A-Za-z][A-Za-z0-9_.-]{0,127}"))
            throw new IllegalArgumentException("Invalid Night Kafka topic/group name");
        return raw;
    }

    public static String messageId(String raw) {
        if (raw == null || !raw.matches("[A-Za-z0-9_-]{8,80}"))
            throw new IllegalArgumentException("Invalid Night Kafka message ID");
        return raw;
    }

    /** Construct a fail-closed Kafka transport configuration (no external I/O). */
    static Properties config(Map<String, String> env) {
        String bootstrap = env.get("NIGHT_KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrap == null || bootstrap.length() > 1024 || bootstrap.isBlank())
            throw new IllegalStateException("NIGHT_KAFKA_BOOTSTRAP_SERVERS is required");
        String[] endpoints = bootstrap.split(",", -1);
        if (endpoints.length > 8)
            throw new IllegalArgumentException("Too many Kafka bootstrap servers");
        boolean allLoopback = true;
        for (String endpoint : endpoints) {
            String candidate = endpoint.strip();
            if (!candidate.matches("(\\[[0-9a-fA-F:]+\\]|[A-Za-z0-9][A-Za-z0-9.-]{0,253}):[0-9]{1,5}"))
                throw new IllegalArgumentException("Invalid Kafka bootstrap endpoint");
            int separator = candidate.lastIndexOf(':');
            int port = Integer.parseInt(candidate.substring(separator + 1));
            if (port < 1 || port > 65535)
                throw new IllegalArgumentException("Invalid Kafka bootstrap port");
            String host = candidate.substring(0, separator);
            if (!("localhost".equalsIgnoreCase(host)
                    || "127.0.0.1".equals(host) || "[::1]".equalsIgnoreCase(host)))
                allLoopback = false;
        }
        String protocol = env.getOrDefault("NIGHT_KAFKA_SECURITY_PROTOCOL", "SSL");
        if (!"SSL".equals(protocol) && !"SASL_SSL".equals(protocol)
                && !"PLAINTEXT".equals(protocol))
            throw new IllegalArgumentException("Kafka requires SSL or SASL_SSL");
        if ("PLAINTEXT".equals(protocol) && (!allLoopback
                || !"true".equals(env.get("NIGHT_KAFKA_ALLOW_INSECURE_LOOPBACK"))))
            throw new IllegalArgumentException("Plaintext Kafka is allowed only on explicit loopback tests");

        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "night-ow");
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, "250");
        if (!"PLAINTEXT".equals(protocol)) {
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
            String truststore = env.get("NIGHT_KAFKA_SSL_TRUSTSTORE_LOCATION");
            if (truststore != null && !truststore.isBlank()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
                String password = env.get("NIGHT_KAFKA_SSL_TRUSTSTORE_PASSWORD");
                if (password != null) props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, password);
            }
            if ("SASL_SSL".equals(protocol)) {
                String mechanism = env.get("NIGHT_KAFKA_SASL_MECHANISM");
                String jaas = env.get("NIGHT_KAFKA_SASL_JAAS_CONFIG");
                if (!("PLAIN".equals(mechanism) || "SCRAM-SHA-256".equals(mechanism)
                        || "SCRAM-SHA-512".equals(mechanism))
                        || jaas == null || jaas.isBlank())
                    throw new IllegalArgumentException("Kafka SASL_SSL requires mechanism and JAAS credentials");
                props.put(SaslConfigs.SASL_MECHANISM, mechanism);
                props.put(SaslConfigs.SASL_JAAS_CONFIG, jaas);
            }
        }
        return props;
    }

    public synchronized int open() {
        if (producer != null) return 1;
        Properties props = config(System.getenv());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, "10");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "300000");
        producer = new KafkaProducer<>(props);
        return 1;
    }

    public synchronized Interpreter.OwRecord publish(String topic, String id, String json) {
        identifier(topic);
        messageId(id);
        if (json == null || json.getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES)
            throw new IllegalArgumentException("Invalid Kafka payload");
        if (producer == null) throw new IllegalStateException("Night Kafka producer is not open");
        try {
            var metadata = producer.send(new ProducerRecord<>(topic, id, json)).get(20, TimeUnit.SECONDS);
            Interpreter.OwRecord result = new Interpreter.OwRecord();
            result.put("topic", metadata.topic());
            result.put("partition", metadata.partition());
            result.put("offset", metadata.offset());
            result.put("message_id", id);
            return result;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Night Kafka publish interrupted", ex);
        } catch (ExecutionException | TimeoutException ex) {
            // A timeout may occur after broker delivery; retry with message-id deduplication.
            throw new IllegalStateException("Night Kafka publish was not confirmed", ex);
        }
    }

    /** Subscribe one runtime instance to exactly one topic and consumer group. */
    public synchronized int subscribe(String topic, String group) {
        identifier(topic);
        identifier(group);
        if (consumer != null) {
            if (topic.equals(subscribedTopic) && group.equals(subscribedGroup)) return 1;
            throw new IllegalStateException("Night Kafka already subscribed; close before changing groups");
        }
        Properties props = config(System.getenv());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "524288");
        KafkaConsumer<String, String> created = new KafkaConsumer<>(props);
        try {
            created.subscribe(Collections.singletonList(topic));
            consumer = created;
            subscribedTopic = topic;
            subscribedGroup = group;
        } catch (RuntimeException ex) {
            created.close(Duration.ofSeconds(2));
            throw ex;
        }
        return 1;
    }

    private KafkaConsumer<String, String> consumer() {
        if (consumer == null) throw new IllegalStateException("Night Kafka consumer is not subscribed");
        return consumer;
    }

    /**
     * Never fetch beyond an unacknowledged entry. Kafka group offsets are
     * cumulative, so this adapter intentionally permits one pending record.
     */
    public synchronized Interpreter.OwRecord poll(int waitMillis) {
        ConsumerRecord<String, String> value = next(waitMillis);
        if (value != null) return record(value);
        Interpreter.OwRecord empty = new Interpreter.OwRecord();
        empty.put("present", 0);
        return empty;
    }

    /** Internal bridge API: exposes untrusted envelopes without throwing on bad IDs. */
    synchronized Interpreter.OwRecord pollForIngestion(int waitMillis) {
        ConsumerRecord<String, String> value = next(waitMillis);
        if (value == null) {
            Interpreter.OwRecord empty = new Interpreter.OwRecord();
            empty.put("present", 0);
            return empty;
        }
        Interpreter.OwRecord record = new Interpreter.OwRecord();
        record.put("present", 1);
        record.put("topic", value.topic());
        record.put("partition", value.partition());
        record.put("offset", value.offset());
        record.put("message_id", value.key());
        record.put("body", value.value());
        return record;
    }

    private ConsumerRecord<String, String> next(int waitMillis) {
        if (waitMillis < 0 || waitMillis > 2000)
            throw new IllegalArgumentException("Kafka poll wait must be between 0 and 2000ms");
        KafkaConsumer<String, String> client = consumer();
        if (pending != null) return pending;
        ConsumerRecords<String, String> records = client.poll(Duration.ofMillis(waitMillis));
        if (records.isEmpty()) return null;
        pending = records.iterator().next();
        return pending;
    }

    private static Interpreter.OwRecord record(ConsumerRecord<String, String> value) {
        // Refuse malformed application events before offering them as trusted OW data.
        messageId(value.key());
        if (value.value() == null
                || value.value().getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES)
            throw new IllegalStateException("Invalid Kafka payload received");
        Interpreter.OwRecord result = new Interpreter.OwRecord();
        result.put("present", 1);
        result.put("topic", value.topic());
        result.put("partition", value.partition());
        result.put("offset", value.offset());
        result.put("message_id", value.key());
        result.put("body", value.value());
        return result;
    }

    public synchronized int acknowledge(String topic, int partition, long offset, String id) {
        ConsumerRecord<String, String> value = pending;
        if (value == null || !value.topic().equals(topic)
                || value.partition() != partition || value.offset() != offset
                || !value.key().equals(id))
            throw new IllegalArgumentException("Kafka ACK must identify the exact pending event");
        // Committing offset+1 is safe because we never expose later records
        // before this one is acknowledged. A broker exception retains pending.
        consumer().commitSync(Map.of(new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1)), Duration.ofSeconds(10));
        pending = null;
        return 1;
    }

    /**
     * The ingest bridge can ACK a quarantined malformed key only after its
     * complete envelope was durably written to the quarantine table.
     * Package-private: not exposed to OW application callers.
     */
    synchronized int acknowledgeIngestion(String topic, int partition, long offset) {
        ConsumerRecord<String, String> value = pending;
        if (value == null || !value.topic().equals(topic)
                || value.partition() != partition || value.offset() != offset)
            throw new IllegalArgumentException("Kafka ingestion ACK does not match pending offset");
        consumer().commitSync(Map.of(new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1)), Duration.ofSeconds(10));
        pending = null;
        return 1;
    }

    /** Seek the uncommitted record again, without committing its offset. */
    public synchronized int retryPending() {
        if (pending == null) return 0;
        consumer().seek(new TopicPartition(pending.topic(), pending.partition()), pending.offset());
        pending = null;
        return 1;
    }

    @Override public synchronized void close() {
        try {
            if (consumer != null) consumer.close(Duration.ofSeconds(3));
        } finally {
            consumer = null;
            pending = null;
            subscribedGroup = null;
            subscribedTopic = null;
            if (producer != null) {
                producer.close(Duration.ofSeconds(3));
                producer = null;
            }
        }
    }
}
