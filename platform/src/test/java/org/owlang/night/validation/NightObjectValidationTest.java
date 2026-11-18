package org.owlang.night.validation;

import dev.oreo.Interpreter;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NightObjectValidationTest {
    private final NightObjectValidation validator = new NightObjectValidation();
    private final List<?> schema = List.of(
            List.of("name", "string", 1, 80),
            List.of("age", "integer", 0, 150),
            List.of("active", "boolean", 0, 1));

    @Test void acceptsValidTypedRecordsWithoutExtraFields() {
        Interpreter.OwRecord request = new Interpreter.OwRecord();
        request.put("name", "Ada");
        request.put("age", 25);
        request.put("active", true);
        var result = validator.validate(request, schema);
        assertEquals(1, result.get("valid"));
        assertTrue(((List<?>) result.get("errors")).isEmpty());
    }

    @Test void deniesMissingInvalidAndUnknownFieldsWithoutEchoingUserInput() {
        Interpreter.OwRecord request = new Interpreter.OwRecord();
        request.put("age", "not-a-number");
        request.put("isAdmin", true);
        var result = validator.validate(request, schema);
        assertEquals(0, result.get("valid"));
        List<?> errors = (List<?>) result.get("errors");
        assertTrue(errors.contains("name:required"));
        assertTrue(errors.contains("age:type"));
        assertTrue(errors.contains("unknown_field"));
        assertFalse(result.toString().contains("not-a-number"));
    }

    @Test void rejectsDuplicateAndMalformedSchemas() {
        Interpreter.OwRecord body = new Interpreter.OwRecord();
        assertThrows(IllegalArgumentException.class,
                () -> validator.validate(body, List.of(List.of("value;DROP", "string", 1, 20))));
        assertThrows(IllegalArgumentException.class,
                () -> validator.validate(body, List.of(List.of("x", "string", 1, 20), List.of("x", "string", 1, 20))));
        assertThrows(IllegalArgumentException.class,
                () -> validator.validate("not-an-object", schema));
    }

    private final List<?> nestedSchema = List.of(
            List.of("profile", "object", 1, 1, List.of(
                    List.of("city", "string", 1, 32),
                    List.of("age", "integer", 0, 150))),
            List.of("tags", "array", 0, 3, List.of("string", 10)),
            List.of("orders", "array", 0, 2, List.of("object", List.of(
                    List.of("sku", "string", 1, 16),
                    List.of("quantity", "integer", 1, 100)))));

    @Test void validatesNestedObjectsArraysAndObjectItems() {
        var input = java.util.Map.of(
                "profile", java.util.Map.of("city", "Paris", "age", 32),
                "tags", List.of("ow", "night"),
                "orders", List.of(java.util.Map.of("sku", "A42", "quantity", 2)));
        var result = validator.validate(input, nestedSchema);
        assertEquals(1, result.get("valid"));
        assertEquals(List.of(), result.get("errors"));
    }

    @Test void reportsStablePathsAndDoesNotLeakUnknownFieldValues() {
        var input = java.util.Map.of(
                "profile", java.util.Map.of("age", "not-an-integer", "secret", "private"),
                "tags", List.of("valid", "overly-long-tag"),
                "orders", List.of(java.util.Map.of("sku", 42, "quantity", 0)));
        var result = validator.validate(input, nestedSchema);
        assertEquals(0, result.get("valid"));
        var errors = (List<?>) result.get("errors");
        assertTrue(errors.contains("profile.city:required"));
        assertTrue(errors.contains("profile.age:type"));
        assertTrue(errors.contains("profile:unknown_field"));
        assertTrue(errors.contains("tags[1]:max_length"));
        assertTrue(errors.contains("orders[0].sku:type"));
        assertFalse(result.toString().contains("private"));
        assertFalse(result.toString().contains("not-an-integer"));
    }

    @Test void boundsArraysAndDeniesUntrustedSchemaShapes() {
        var result = validator.validate(java.util.Map.of(
                "profile", java.util.Map.of("city", "Paris"),
                "tags", List.of("one", "two", "three", "four")), nestedSchema);
        assertTrue(((List<?>) result.get("errors")).contains("tags:max_items"));
        assertThrows(IllegalArgumentException.class,
                () -> validator.validate(java.util.Map.of(),
                        List.of(List.of("nested", "object", 0, 1))));
        assertThrows(IllegalArgumentException.class,
                () -> validator.validate(java.util.Map.of(),
                        List.of(List.of("nested", "object", 0, 1, List.of(
                                List.of("x", "string", 1, 5),
                                List.of("x", "string", 1, 5))))));
        assertThrows(IllegalArgumentException.class,
                () -> validator.validate(java.util.Map.of(),
                        List.of(List.of("items", "array", 0, 2, List.of("array", 2)))));
        assertThrows(IllegalArgumentException.class,
                () -> validator.validate(java.util.Map.of(),
                        List.of(List.of("items", "array", 0, 257, List.of("string", 10)))));
    }

    @Test void capsReportedErrorsForLargeButBoundedArrays() {
        var schema = List.of(List.of("values", "array", 1, 200, List.of("integer", 10)));
        var values = java.util.Collections.nCopies(200, "invalid");
        var result = validator.validate(java.util.Map.of("values", values), schema);
        assertEquals(0, result.get("valid"));
        assertEquals(64, ((List<?>) result.get("errors")).size());
    }

}
