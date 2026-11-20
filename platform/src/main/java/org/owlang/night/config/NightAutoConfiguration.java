package org.owlang.night.config;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Strict, opt-in Night starter selection from OW's dependencies.yml.
 *
 * OW owns dependency installation and the lockfile. Night only interprets
 * the optional 'night:' section and known Night dependency aliases. No
 * external source, secret, or class is loaded by this parser.
 */
public final class NightAutoConfiguration {
    public static final List<String> START_ORDER = List.of(
            "core", "web", "config", "validation", "cache", "actuator", "openapi",
            "http", "mail", "scheduler", "data", "redis", "kafka", "rabbitmq", "security");

    private static final Map<String, String> DEPENDENCY_STARTERS = Map.ofEntries(
            Map.entry("night", "core"),
            Map.entry("night-core", "core"),
            Map.entry("night-web", "web"),
            Map.entry("night-config", "config"),
            Map.entry("night-validation", "validation"),
            Map.entry("night-cache", "cache"),
            Map.entry("night-actuator", "actuator"),
            Map.entry("night-openapi", "openapi"),
            Map.entry("night-http", "http"),
            Map.entry("night-mail", "mail"),
            Map.entry("night-scheduler", "scheduler"),
            Map.entry("night-data", "data"),
            Map.entry("night-redis", "redis"),
            Map.entry("night-kafka", "kafka"),
            Map.entry("night-rabbitmq", "rabbitmq"),
            Map.entry("night-security", "security"));

    public record Plan(Set<String> starters, String migrations) {
        public Plan {
            starters = Collections.unmodifiableSet(new LinkedHashSet<>(starters));
        }
        public boolean has(String name) { return starters.contains(name); }
        public List<String> ordered() {
            return START_ORDER.stream().filter(starters::contains).toList();
        }
    }

    private NightAutoConfiguration() {}

    public static Plan read(Path path) throws IOException {
        if (!"dependencies.yml".equals(path.getFileName().toString()))
            throw new IllegalArgumentException("Night auto-configuration expects dependencies.yml");
        if (!Files.isRegularFile(path))
            throw new IllegalArgumentException("Night dependency manifest not found: " + path);
        LoaderOptions limits = new LoaderOptions();
        limits.setAllowDuplicateKeys(false);
        limits.setMaxAliasesForCollections(0);
        limits.setCodePointLimit(200_000);
        limits.setNestingDepthLimit(20);
        Object document;
        try (InputStream in = Files.newInputStream(path)) {
            document = new Yaml(new SafeConstructor(limits)).load(in);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Invalid Night dependencies.yml: " + ex.getMessage(), ex);
        }
        Map<?, ?> root = mapping(document, "dependencies.yml");
        if (!"1".equals(String.valueOf(root.getOrDefault("format", 1))))
            throw new IllegalArgumentException("Unsupported OW dependencies.yml format");
        LinkedHashSet<String> detected = new LinkedHashSet<>();
        for (Object item : sequence(root.get("dependencies"), "dependencies")) {
            Map<?, ?> dependency = mapping(item, "dependency");
            Object name = dependency.get("name");
            if (!(name instanceof String alias) || alias.isBlank())
                throw new IllegalArgumentException("Invalid OW dependency name");
            String starter = DEPENDENCY_STARTERS.get(alias);
            if (starter != null) detected.add(starter);
        }

        Object nightSpec = root.get("night");
        if (nightSpec == null) return new Plan(detected, "");
        Map<?, ?> night = mapping(nightSpec, "night");
        for (Object key : night.keySet())
            if (!Set.of("starters", "disable", "migrations").contains(key))
                throw new IllegalArgumentException("Unknown night configuration field: " + key);

        LinkedHashSet<String> explicit = starters(night.get("starters"), "night.starters");
        LinkedHashSet<String> disabled = starters(night.get("disable"), "night.disable");
        for (String name : explicit) {
            if (disabled.contains(name))
                throw new IllegalArgumentException("Night starter " + name + " is both enabled and disabled");
        }
        detected.addAll(explicit);
        detected.removeAll(disabled);
        String migrations = "";
        if (night.containsKey("migrations")) {
            Object value = night.get("migrations");
            if (!(value instanceof String pathValue) || pathValue.isBlank() ||
                    (!pathValue.startsWith("filesystem:") && !pathValue.startsWith("classpath:")) ||
                    pathValue.contains(".."))
                throw new IllegalArgumentException("night.migrations requires a safe filesystem: or classpath: path");
            if (!detected.contains("data"))
                throw new IllegalArgumentException("night.migrations requires the data starter");
            migrations = pathValue;
        }
        return new Plan(detected, migrations);
    }

    private static LinkedHashSet<String> starters(Object value, String field) {
        LinkedHashSet<String> result = new LinkedHashSet<>();
        for (Object raw : sequence(value, field)) {
            if (!(raw instanceof String name) || !START_ORDER.contains(name))
                throw new IllegalArgumentException("Unknown Night starter in " + field + ": " + raw);
            if (!result.add(name))
                throw new IllegalArgumentException("Duplicate Night starter in " + field + ": " + name);
        }
        return result;
    }

    private static Map<?, ?> mapping(Object value, String field) {
        if (!(value instanceof Map<?, ?> result))
            throw new IllegalArgumentException(field + " must be a YAML mapping");
        return result;
    }

    private static List<?> sequence(Object value, String field) {
        if (value == null) return List.of();
        if (!(value instanceof List<?> result))
            throw new IllegalArgumentException(field + " must be a YAML sequence");
        return result;
    }
}
