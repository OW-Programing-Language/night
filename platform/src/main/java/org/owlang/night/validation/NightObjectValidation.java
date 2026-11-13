package org.owlang.night.validation;

import dev.oreo.Interpreter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Strict object-validation boundary. Schema rules are explicit compile-time
 * lists: [name, type, required (0/1), maxSize]. Unknown input fields are
 * rejected to protect against accidental mass assignment.
 */
public final class NightObjectValidation {
    public Interpreter.OwRecord validate(Object input, List<?> spec) {
        if (!(input instanceof Map<?, ?> body))
            throw new IllegalArgumentException("Night validation input must be an object");
        if (body.size() > 128 || spec == null || spec.size() > 64)
            throw new IllegalArgumentException("Night validation document too large");

        List<String> errors = new ArrayList<>();
        Set<String> allowed = new HashSet<>();
        for (Object rawRule : spec) {
            if (!(rawRule instanceof List<?> rule) || rule.size() != 4
                    || !(rule.get(0) instanceof String name)
                    || !name.matches("[A-Za-z_][A-Za-z0-9_]{0,63}")
                    || !(rule.get(1) instanceof String type)
                    || !Set.of("string", "integer", "boolean").contains(type)
                    || !(rule.get(2) instanceof Number req)
                    || (req.intValue() != 0 && req.intValue() != 1)
                    || !(rule.get(3) instanceof Number bound)
                    || bound.intValue() < 1 || bound.intValue() > 1_048_576)
                throw new IllegalArgumentException("Invalid Night validation rule");
            if (!allowed.add(name)) throw new IllegalArgumentException("Duplicate validation rule");
            Object value = body.get(name);
            if (value == null) {
                if (req.intValue() == 1) errors.add(name + ":required");
                continue;
            }
            switch (type) {
                case "string" -> {
                    if (!(value instanceof String s)) errors.add(name + ":type");
                    else if (s.length() > bound.intValue()) errors.add(name + ":max_length");
                }
                case "integer" -> {
                    if (!(value instanceof Byte || value instanceof Short
                            || value instanceof Integer || value instanceof Long))
                        errors.add(name + ":type");
                    else if (Math.abs(((Number) value).doubleValue()) > bound.doubleValue())
                        errors.add(name + ":range");
                }
                case "boolean" -> {
                    if (!(value instanceof Boolean)) errors.add(name + ":type");
                }
                default -> throw new IllegalStateException("Invalid type");
            }
        }
        for (Object raw : body.keySet()) {
            if (!(raw instanceof String name) || !allowed.contains(name))
                errors.add("unknown_field");
        }
        Interpreter.OwRecord out = new Interpreter.OwRecord();
        out.put("valid", errors.isEmpty() ? 1 : 0);
        out.put("errors", errors);
        return out;
    }
}
