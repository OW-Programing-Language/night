package org.owlang.night.security;

import com.sun.net.httpserver.HttpExchange;
import dev.oreo.RuntimeExtension;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Stateless Authorization header authentication. Tokens are never stored as
 * plaintext by Night: operators provide SHA-256 digests in NIGHT_BEARER_KEYS.
 * Every request is authenticated by default once enabled, unless explicitly
 * granted public access. No Cookie/session auth means CSRF is not applicable
 * to this particular deployment mode.
 */
public final class BearerSecurity {
    private record Credential(String role, byte[] digest) {}
    private record Rule(String method, List<String> segments, String role) {}
    private final List<Credential> credentials = new ArrayList<>();
    private final List<Rule> rules = new ArrayList<>();
    private boolean enabled;

    public synchronized boolean isEnabled() { return enabled; }

    public synchronized void enable() {
        if (enabled) return;
        String configuration = System.getenv("NIGHT_BEARER_KEYS");
        if (configuration == null || configuration.isBlank()) {
            throw new IllegalStateException(
                "Night Security needs NIGHT_BEARER_KEYS (ROLE:SHA256_HEX;...)");
        }
        List<Credential> loaded = new ArrayList<>();
        for (String token : configuration.split(";")) {
            String[] entry = token.strip().split(":", 2);
            if (entry.length != 2 || !entry[0].matches("[A-Z][A-Z0-9_]{1,31}")
                    || !entry[1].matches("[0-9a-fA-F]{64}")) {
                throw new IllegalStateException("Invalid NIGHT_BEARER_KEYS entry");
            }
            loaded.add(new Credential(entry[0], hexDecode(entry[1])));
        }
        if (loaded.isEmpty()) throw new IllegalStateException("No bearer credentials installed");
        credentials.addAll(loaded);
        enabled = true;
    }

    public synchronized void publicRoute(String spec) { define(spec, "PUBLIC"); }
    public synchronized void requireRole(String spec, String role) {
        if (!role.matches("[A-Z][A-Z0-9_]{1,31}"))
            throw new IllegalArgumentException("Invalid role name");
        define(spec, role);
    }

    private void define(String spec, String role) {
        String[] parts = spec.strip().split(" ", 2);
        if (parts.length != 2 || !parts[0].matches("[A-Z]+")
                || !parts[1].startsWith("/"))
            throw new IllegalArgumentException("Expected METHOD /path policy");
        Rule next = new Rule(parts[0], segments(parts[1]), role);
        for (Rule rule : rules) {
            if (rule.method.equals(next.method) && rule.segments.equals(next.segments))
                throw new IllegalArgumentException("Duplicate Night security rule: " + spec);
        }
        rules.add(next);
    }

    public synchronized RuntimeExtension.Response before(HttpExchange exchange) {
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.getResponseHeaders().set("X-Frame-Options", "DENY");
        exchange.getResponseHeaders().set("Referrer-Policy", "no-referrer");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        if (!enabled) return null;

        String method = exchange.getRequestMethod().toUpperCase(Locale.ROOT);
        List<String> requested = segments(exchange.getRequestURI().getPath());
        String required = "AUTHENTICATED";
        for (Rule rule : rules) {
            if (rule.method.equals(method) && routeMatches(rule.segments, requested)) {
                required = rule.role;
                break;
            }
        }
        if ("PUBLIC".equals(required)) return null;

        List<String> headers = exchange.getRequestHeaders().get("Authorization");
        if (headers == null || headers.size() != 1) return unauthorized(exchange);
        String authorization = headers.get(0);
        if (!authorization.startsWith("Bearer ")) return unauthorized(exchange);
        String token = authorization.substring(7);
        if (token.isBlank() || token.length() > 4096) return unauthorized(exchange);
        byte[] digest = hash(token);
        String role = null;
        for (Credential credential : credentials) {
            if (MessageDigest.isEqual(digest, credential.digest))
                role = credential.role;
        }
        if (role == null) return unauthorized(exchange);
        if ("AUTHENTICATED".equals(required) || required.equals(role) ||
                "ADMIN".equals(role)) return null;
        return json(403, "forbidden");
    }

    private static RuntimeExtension.Response unauthorized(HttpExchange exchange) {
        exchange.getResponseHeaders().set("WWW-Authenticate", "Bearer");
        return json(401, "unauthorized");
    }

    private static RuntimeExtension.Response json(int status, String code) {
        return new RuntimeExtension.Response(status, "application/json; charset=utf-8",
                "{\"error\":\"" + code + "\"}");
    }

    private static boolean routeMatches(List<String> rule, List<String> path) {
        if (rule.size() != path.size()) return false;
        for (int i = 0; i < rule.size(); i++) {
            if (!rule.get(i).startsWith(":") && !rule.get(i).equals(path.get(i)))
                return false;
        }
        return true;
    }

    private static List<String> segments(String path) {
        List<String> parts = new ArrayList<>();
        for (String item : path.split("/")) if (!item.isEmpty()) parts.add(item);
        return parts;
    }

    private static byte[] hexDecode(String input) {
        byte[] bytes = new byte[input.length() / 2];
        for (int i = 0; i < bytes.length; i++)
            bytes[i] = (byte) Integer.parseInt(input.substring(i * 2, i * 2 + 2), 16);
        return bytes;
    }

    public static byte[] hash(String token) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(
                    token.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
