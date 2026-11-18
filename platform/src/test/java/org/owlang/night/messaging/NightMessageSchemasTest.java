package org.owlang.night.messaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NightMessageSchemasTest {
    private static final String SPEC = """
            {
              "version": 1,
              "topics": {
                "orders.created": [
                  ["orderId","integer",1,100000],
                  ["customer","object",1,1,[["city","string",1,64]]],
                  ["items","array",0,4,["string",16]]
                ]
              }
            }
            """;

    @TempDir Path tmp;

    @Test void validatesNestedObjectsArraysAndRequiredFields() {
        var rules = NightMessageSchemas.parse(SPEC);
        assertNull(rules.invalidReason("orders.created",
                "{\"orderId\":42,\"customer\":{\"city\":\"Paris\"},\"items\":[\"one\",\"two\"]}"));
        assertEquals("SCHEMA_VIOLATION",
                rules.invalidReason("orders.created", "{\"orderId\":42}"));
        assertEquals("SCHEMA_VIOLATION",
                rules.invalidReason("orders.created",
                        "{\"orderId\":\"42\",\"customer\":{\"city\":\"Paris\"}}"));
        assertEquals("SCHEMA_VIOLATION",
                rules.invalidReason("orders.created",
                        "{\"orderId\":42,\"customer\":{\"city\":\"Paris\",\"admin\":true}}"));
        assertEquals("SCHEMA_VIOLATION",
                rules.invalidReason("orders.created",
                        "{\"orderId\":42,\"customer\":{\"city\":\"Paris\"},\"items\":[\"ok\",123]}"));
        assertEquals("SCHEMA_VIOLATION",
                rules.invalidReason("orders.created",
                        "{\"orderId\":42,\"customer\":{\"city\":\"Paris\"},\"extra\":1}"));
        assertEquals("SCHEMA_VIOLATION", rules.invalidReason("orders.created", "[]"));
    }

    @Test void rejectsMalformedJsonDuplicateKeysAndDeepPayloads() {
        var rules = NightMessageSchemas.parse(SPEC);
        assertEquals("INVALID_JSON", rules.invalidReason("orders.created", "{"));
        assertEquals("INVALID_JSON", rules.invalidReason("orders.created",
                "{\"orderId\":1,\"orderId\":2}"));
        assertEquals("INVALID_JSON", rules.invalidReason("orders.created",
                "{\"customer\":{\"city\":\"x\",\"city\":\"y\"}}"));
        String deeplyNested = "{\"x\":".repeat(18) + "1" + "}".repeat(18);
        assertEquals("INVALID_JSON", rules.invalidReason("orders.created", deeplyNested));
        assertEquals("PAYLOAD_TOO_LARGE",
                rules.invalidReason("orders.created", "x".repeat(262145)));
    }

    @Test void failsClosedForUnknownTopicsOrMissingRules() {
        var rules = NightMessageSchemas.parse(SPEC);
        assertThrows(IllegalStateException.class,
                () -> rules.invalidReason("payments.received", "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.parse("{\"version\":1,\"topics\":{}}"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.parse("{\"version\":2,\"topics\":{\"a\":[[\"x\",\"string\",1,10]]}}"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.parse("{\"version\":1,\"topics\":{\"bad/topic\":[[\"x\",\"string\",1,10]]}}"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.parse("{\"version\":1,\"topics\":{\"ok\":[[\"x\",\"unknown\",1,10]]}}"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.parse("{\"version\":1,\"version\":1,\"topics\":{}}"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.parse("{\"version\":1,\"topics\":{\"a\":[[\"x\",\"string\",1,10]],\"a\":[[\"x\",\"string\",1,10]]}}"));
    }

    @Test void loadsLocalSchemasAndRejectsMissingOrOversizedFiles() throws Exception {
        assertNull(NightMessageSchemas.fromEnvironment(Map.of()));
        Path rules = tmp.resolve("message-schemas.json");
        Files.writeString(rules, SPEC);
        var loaded = NightMessageSchemas.fromEnvironment(
                Map.of("NIGHT_CONSUMER_SCHEMAS_FILE", rules.toString()));
        assertNull(loaded.invalidReason("orders.created",
                "{\"orderId\":10,\"customer\":{\"city\":\"Nice\"}}"));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.load(Path.of("relative-file.json")));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.load(tmp.resolve("missing.json")));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.load(tmp));
        Files.writeString(rules, " ".repeat(65_537));
        assertThrows(IllegalArgumentException.class,
                () -> NightMessageSchemas.load(rules));
    }
}
