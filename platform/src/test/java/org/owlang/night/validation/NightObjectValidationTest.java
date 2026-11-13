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
}
