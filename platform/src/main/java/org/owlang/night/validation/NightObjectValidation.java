package org.owlang.night.validation;

import dev.oreo.Interpreter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fail-closed validation for OW records. Four-field primitive rules remain
 * backwards compatible. Object and array rules add a fifth schema descriptor.
 * Schema and data traversal are bounded, and errors never echo user values.
 */
public final class NightObjectValidation {
    private static final int MAX_FIELDS = 128;
    private static final int MAX_RULES_PER_OBJECT = 64;
    private static final int MAX_SCHEMA_RULES = 256;
    private static final int MAX_DEPTH = 8;
    private static final int MAX_ERRORS = 64;
    private static final int MAX_ARRAY_ITEMS = 256;

    private record ItemRule(String type, int bound, List<FieldRule> children) {}
    private record FieldRule(String name, String type, boolean required,
                             int bound, List<FieldRule> children, ItemRule item) {}
    private static final class Count { int value; }

    public Interpreter.OwRecord validate(Object input, List<?> spec) {
        if (!(input instanceof Map<?, ?> body))
            throw new IllegalArgumentException("Night validation input must be an object");
        if (body.size() > MAX_FIELDS)
            throw new IllegalArgumentException("Night validation document too large");
        List<FieldRule> schema = parseRules(spec, 0, new Count());
        List<String> errors = new ArrayList<>();
        validateObject(body, schema, "", 0, errors);

        Interpreter.OwRecord out = new Interpreter.OwRecord();
        out.put("valid", errors.isEmpty() ? 1 : 0);
        out.put("errors", errors);
        return out;
    }

    private static int positiveBound(Object raw, int maximum) {
        if (!(raw instanceof Number n) || !Double.isFinite(n.doubleValue())
                || n.doubleValue() != n.intValue()
                || n.intValue() < 1 || n.intValue() > maximum)
            throw new IllegalArgumentException("Invalid Night validation bound");
        return n.intValue();
    }

    private static List<FieldRule> parseRules(List<?> spec, int depth, Count count) {
        if (spec == null || depth > MAX_DEPTH || spec.size() > MAX_RULES_PER_OBJECT)
            throw new IllegalArgumentException("Invalid Night validation schema depth or size");
        List<FieldRule> parsed = new ArrayList<>(spec.size());
        Set<String> names = new HashSet<>();
        for (Object raw : spec) {
            if (++count.value > MAX_SCHEMA_RULES || !(raw instanceof List<?> rule)
                    || (rule.size() != 4 && rule.size() != 5)
                    || !(rule.get(0) instanceof String name)
                    || !name.matches("[A-Za-z_][A-Za-z0-9_]{0,63}")
                    || !(rule.get(1) instanceof String type)
                    || !Set.of("string", "integer", "boolean", "object", "array").contains(type)
                    || !(rule.get(2) instanceof Number required)
                    || (required.doubleValue() != 0 && required.doubleValue() != 1))
                throw new IllegalArgumentException("Invalid Night validation rule");
            if (!names.add(name))
                throw new IllegalArgumentException("Duplicate validation rule");
            int bound = positiveBound(rule.get(3),
                    "array".equals(type) ? MAX_ARRAY_ITEMS : 1_048_576);
            boolean complex = "object".equals(type) || "array".equals(type);
            if ((complex && rule.size() != 5) || (!complex && rule.size() != 4))
                throw new IllegalArgumentException("Invalid Night validation rule shape");
            List<FieldRule> children = List.of();
            ItemRule item = null;
            if ("object".equals(type)) {
                if (!(rule.get(4) instanceof List<?> nested))
                    throw new IllegalArgumentException("Object schema must be a list of field rules");
                children = parseRules(nested, depth + 1, count);
            } else if ("array".equals(type)) {
                if (!(rule.get(4) instanceof List<?> descriptor)
                        || descriptor.size() != 2
                        || !(descriptor.get(0) instanceof String elementType))
                    throw new IllegalArgumentException("Invalid array item schema");
                if ("object".equals(elementType)) {
                    if (!(descriptor.get(1) instanceof List<?> nested))
                        throw new IllegalArgumentException("Array object schema must be a list");
                    item = new ItemRule("object", 1, parseRules(nested, depth + 1, count));
                } else if (Set.of("string", "integer", "boolean").contains(elementType)) {
                    item = new ItemRule(elementType,
                            positiveBound(descriptor.get(1), 1_048_576), List.of());
                } else {
                    throw new IllegalArgumentException("Unsupported array element type");
                }
            }
            parsed.add(new FieldRule(name, type, required.intValue() == 1,
                    bound, children, item));
        }
        return List.copyOf(parsed);
    }

    private static void addError(List<String> errors, String value) {
        if (errors.size() < MAX_ERRORS) errors.add(value);
    }

    private static void validateObject(Map<?, ?> body, List<FieldRule> schema,
                                       String prefix, int depth, List<String> errors) {
        if (depth > MAX_DEPTH) {
            addError(errors, prefix + ":too_deep");
            return;
        }
        if (body.size() > MAX_FIELDS) {
            addError(errors, prefix + ":max_fields");
            return;
        }
        Set<String> allowed = new HashSet<>();
        for (FieldRule field : schema) {
            if (errors.size() >= MAX_ERRORS) return;
            allowed.add(field.name);
            String path = prefix.isEmpty() ? field.name : prefix + "." + field.name;
            Object value = body.get(field.name);
            if (value == null) {
                if (field.required) addError(errors, path + ":required");
                continue;
            }
            if ("object".equals(field.type)) {
                if (value instanceof Map<?, ?> nested)
                    validateObject(nested, field.children, path, depth + 1, errors);
                else addError(errors, path + ":type");
            } else if ("array".equals(field.type)) {
                if (value instanceof List<?> items)
                    validateArray(items, field, path, depth + 1, errors);
                else addError(errors, path + ":type");
            } else {
                validatePrimitive(value, field.type, field.bound, path, errors);
            }
        }
        for (Object key : body.keySet()) {
            if (!(key instanceof String name) || !allowed.contains(name)) {
                addError(errors, prefix.isEmpty() ? "unknown_field" : prefix + ":unknown_field");
                break;
            }
        }
    }

    private static void validateArray(List<?> items, FieldRule field,
                                      String path, int depth, List<String> errors) {
        if (items.size() > field.bound) {
            addError(errors, path + ":max_items");
            return;
        }
        if (depth > MAX_DEPTH) {
            addError(errors, path + ":too_deep");
            return;
        }
        for (int i = 0; i < items.size() && errors.size() < MAX_ERRORS; i++) {
            String itemPath = path + "[" + i + "]";
            Object element = items.get(i);
            if ("object".equals(field.item.type)) {
                if (element instanceof Map<?, ?> record)
                    validateObject(record, field.item.children, itemPath, depth + 1, errors);
                else addError(errors, itemPath + ":type");
            } else {
                validatePrimitive(element, field.item.type, field.item.bound, itemPath, errors);
            }
        }
    }

    private static void validatePrimitive(Object value, String type, int bound,
                                          String path, List<String> errors) {
        switch (type) {
            case "string" -> {
                if (!(value instanceof String s)) addError(errors, path + ":type");
                else if (s.length() > bound) addError(errors, path + ":max_length");
            }
            case "integer" -> {
                if (!(value instanceof Byte || value instanceof Short
                        || value instanceof Integer || value instanceof Long))
                    addError(errors, path + ":type");
                else if (Math.abs(((Number) value).doubleValue()) > bound)
                    addError(errors, path + ":range");
            }
            case "boolean" -> {
                if (!(value instanceof Boolean)) addError(errors, path + ":type");
            }
            default -> throw new IllegalStateException("Unexpected validation type");
        }
    }
}
