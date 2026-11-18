package org.owlang.night.messaging;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.owlang.night.data.NightInbox;
import org.owlang.night.validation.NightObjectValidation;

/**
 * Strict, opt-in, versioned per-topic Night field rules for message bodies.
 *
 * This is deliberately NOT general JSON Schema and does not perform remote
 * references, polymorphic Java deserialization or arbitrary OW callbacks.
 * Only configured topics are accepted when schemas are enabled.
 */
public final class NightMessageSchemas {
    private static final int MAX_CONFIG_BYTES = 65_536;
    private static final int MAX_BODY_BYTES = 262_144;
    private static final int MAX_TOPICS = 32;

    private static final ObjectMapper JSON = new ObjectMapper(
            JsonFactory.builder()
                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                    .streamReadConstraints(StreamReadConstraints.builder()
                            .maxNestingDepth(16)
                            .maxStringLength(MAX_BODY_BYTES)
                            .maxNumberLength(64)
                            .build())
                    .build()).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final Map<String, List<?>> rulesByTopic;
    private final NightObjectValidation validator = new NightObjectValidation();

    private NightMessageSchemas(Map<String, List<?>> rulesByTopic) {
        this.rulesByTopic = Map.copyOf(rulesByTopic);
    }

    /** Empty or absent path preserves legacy no-schema-validation mode. */
    public static NightMessageSchemas fromEnvironment(Map<String, String> env) {
        Objects.requireNonNull(env);
        String path = env.get("NIGHT_CONSUMER_SCHEMAS_FILE");
        if (path == null || path.isBlank()) return null;
        return load(Path.of(path));
    }

    /** Read an explicit local, regular, size-limited file once on worker startup. */
    public static NightMessageSchemas load(Path path) {
        Objects.requireNonNull(path);
        if (!path.isAbsolute() || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
            throw new IllegalArgumentException("Message schema must be an absolute regular file");
        try {
            if (Files.size(path) > MAX_CONFIG_BYTES)
                throw new IllegalArgumentException("Night message schema file too large");
            return parse(Files.readString(path, StandardCharsets.UTF_8));
        } catch (IOException ex) {
            throw new IllegalArgumentException("Cannot read Night message schemas", ex);
        }
    }

    /** For tests and trusted configuration loading only. */
    static NightMessageSchemas parse(String source) {
        if (source == null || source.getBytes(StandardCharsets.UTF_8).length > MAX_CONFIG_BYTES)
            throw new IllegalArgumentException("Night message schema document too large");
        final Object decoded;
        try {
            decoded = JSON.readValue(source, Object.class);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Invalid Night message schema JSON", ex);
        }
        if (!(decoded instanceof Map<?, ?> root)
                || root.size() != 2 || !(root.get("version") instanceof Integer version)
                || version != 1 || !(root.get("topics") instanceof Map<?, ?> topics)
                || topics.isEmpty() || topics.size() > MAX_TOPICS)
            throw new IllegalArgumentException("Invalid Night message schema configuration");

        Map<String, List<?>> compiled = new HashMap<>();
        NightObjectValidation validator = new NightObjectValidation();
        for (var entry : topics.entrySet()) {
            if (!(entry.getKey() instanceof String topic)
                    || !(entry.getValue() instanceof List<?> rules) || rules.isEmpty())
                throw new IllegalArgumentException("Invalid Night message topic rule");
            NightInbox.topic(topic);
            try {
                // Validate rule syntax and resource budgets at startup, not
                // after a broker delivery has entered the processing loop.
                validator.validate(Map.of(), rules);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Invalid rules for configured topic", ex);
            }
            compiled.put(topic, List.copyOf(rules));
        }
        return new NightMessageSchemas(compiled);
    }

    /**
     * Returns a quarantine reason or null. Unknown topics are a deployment
     * misconfiguration, so they fail without advancing broker acknowledgments.
     */
    public String invalidReason(String topic, String body) {
        NightInbox.topic(topic);
        List<?> rules = rulesByTopic.get(topic);
        if (rules == null)
            throw new IllegalStateException("No Night message schema for broker topic");
        if (body == null)
            return "INVALID_JSON";
        if (body.getBytes(StandardCharsets.UTF_8).length > MAX_BODY_BYTES)
            return "PAYLOAD_TOO_LARGE";
        final Object document;
        try {
            document = JSON.readValue(body, Object.class);
        } catch (JsonProcessingException ex) {
            return "INVALID_JSON";
        }
        if (!(document instanceof Map<?, ?>))
            return "SCHEMA_VIOLATION";
        try {
            Object report = validator.validate(document, rules).get("valid");
            return Integer.valueOf(1).equals(report) ? null : "SCHEMA_VIOLATION";
        } catch (IllegalArgumentException ex) {
            // Deep/oversized objects are invalid data, not broken rule config
            // (configuration was prevalidated on startup).
            return "SCHEMA_VIOLATION";
        }
    }
}
