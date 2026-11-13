package org.owlang.night.web;

import dev.oreo.Interpreter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Explicit route-metadata registry that generates an OpenAPI 3.0 document.
 * OW's runtime does not yet expose route/schema introspection: callers must
 * register metadata at startup and separately define the actual route.
 */
public final class NightOpenApi {
    private static final Set<String> METHODS =
            Set.of("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS");
    private static final String SEGMENT = "(?:[A-Za-z0-9_.-]+|:[A-Za-z_][A-Za-z0-9_]*)";
    private static final String PATH = "^/(?:(" + SEGMENT + ")(?:/" + SEGMENT + ")*)?$";

    private final Map<String, Map<String, Interpreter.OwRecord>> paths = new LinkedHashMap<>();
    private int routeCount;

    public synchronized int register(String method, String route, String summary, int authenticated) {
        if (method == null || !METHODS.contains(method))
            throw new IllegalArgumentException("Unsupported OpenAPI HTTP method");
        if (route == null || route.length() > 256 || !route.matches(PATH))
            throw new IllegalArgumentException("Invalid OpenAPI path");
        if (summary == null || summary.isBlank() || summary.length() > 160)
            throw new IllegalArgumentException("OpenAPI summary must be 1..160 characters");
        if (authenticated != 0 && authenticated != 1)
            throw new IllegalArgumentException("OpenAPI authenticated flag must be 0 or 1");
        if (routeCount >= 1_000)
            throw new IllegalStateException("Night OpenAPI route limit exceeded");

        String normalized = route.replaceAll(":([A-Za-z_][A-Za-z0-9_]*)", "{$1}");
        Map<String, Interpreter.OwRecord> methods =
                paths.computeIfAbsent(normalized, unused -> new LinkedHashMap<>());
        String key = method.toLowerCase(Locale.ROOT);
        if (methods.containsKey(key))
            throw new IllegalArgumentException("Duplicate Night OpenAPI method/path");
        Interpreter.OwRecord operation = new Interpreter.OwRecord();
        operation.put("summary", summary);
        operation.put("operationId", key + "_" + normalized
                .replaceAll("[^A-Za-z0-9_]+", "_").replaceAll("^_+|_+$", ""));
        Interpreter.OwRecord ok = new Interpreter.OwRecord();
        ok.put("description", "Successful response");
        Interpreter.OwRecord responses = new Interpreter.OwRecord();
        responses.put("200", ok);
        operation.put("responses", responses);

        List<Interpreter.OwRecord> params = new ArrayList<>();
        for (String fragment : route.split("/")) {
            if (!fragment.startsWith(":")) continue;
            Interpreter.OwRecord p = new Interpreter.OwRecord();
            p.put("name", fragment.substring(1));
            p.put("in", "path");
            p.put("required", true);
            Interpreter.OwRecord schema = new Interpreter.OwRecord();
            schema.put("type", "string");
            p.put("schema", schema);
            params.add(p);
        }
        if (!params.isEmpty()) operation.put("parameters", params);
        if (authenticated == 1) {
            Interpreter.OwRecord requirement = new Interpreter.OwRecord();
            requirement.put("bearerAuth", List.of());
            operation.put("security", List.of(requirement));
        } else {
            operation.put("security", List.of());
        }
        methods.put(key, operation);
        routeCount++;
        return 1;
    }

    public synchronized Interpreter.OwRecord spec(String title, String version) {
        if (title == null || title.isBlank() || title.length() > 120
                || version == null || version.isBlank() || version.length() > 48)
            throw new IllegalArgumentException("OpenAPI title/version must be nonblank and bounded");
        Interpreter.OwRecord root = new Interpreter.OwRecord();
        root.put("openapi", "3.0.3");
        Interpreter.OwRecord info = new Interpreter.OwRecord();
        info.put("title", title);
        info.put("version", version);
        root.put("info", info);
        Interpreter.OwRecord tree = new Interpreter.OwRecord();
        for (var entry : paths.entrySet()) {
            Interpreter.OwRecord methods = new Interpreter.OwRecord();
            entry.getValue().forEach(methods::put);
            tree.put(entry.getKey(), methods);
        }
        root.put("paths", tree);
        Interpreter.OwRecord bearer = new Interpreter.OwRecord();
        bearer.put("type", "http");
        bearer.put("scheme", "bearer");
        bearer.put("bearerFormat", "JWT or opaque token");
        Interpreter.OwRecord securitySchemes = new Interpreter.OwRecord();
        securitySchemes.put("bearerAuth", bearer);
        Interpreter.OwRecord components = new Interpreter.OwRecord();
        components.put("securitySchemes", securitySchemes);
        root.put("components", components);
        return root;
    }
}
