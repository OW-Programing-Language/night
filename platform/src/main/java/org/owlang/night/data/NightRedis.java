package org.owlang.night.data;

import redis.clients.jedis.JedisPooled;

import java.net.URI;

/**
 * Bounded, namespaced Redis string cache. Redis persists data across Night
 * processes, unlike NightCache's in-memory TTL cache.
 *
 * Configure NIGHT_REDIS_URL (rediss:// for TLS), optionally
 * NIGHT_REDIS_NAMESPACE. Plaintext redis:// is permitted only for loopback
 * integration tests with NIGHT_REDIS_ALLOW_INSECURE_LOOPBACK=true.
 */
public final class NightRedis implements AutoCloseable {
    private JedisPooled client;
    private String namespace;

    public synchronized int open() {
        if (client != null) return 1;
        String url = System.getenv("NIGHT_REDIS_URL");
        if (url == null || url.isBlank())
            throw new IllegalStateException("NIGHT_REDIS_URL is required");
        validateUrl(url, "true".equals(System.getenv("NIGHT_REDIS_ALLOW_INSECURE_LOOPBACK")));
        String configured = System.getenv().getOrDefault("NIGHT_REDIS_NAMESPACE", "night");
        if (!configured.matches("[a-zA-Z][a-zA-Z0-9_-]{0,31}"))
            throw new IllegalArgumentException("Invalid Night Redis namespace");
        JedisPooled next = new JedisPooled(url);
        try {
            if (!"PONG".equals(next.ping()))
                throw new IllegalStateException("Night Redis health check failed");
        } catch (RuntimeException ex) {
            next.close();
            throw ex;
        }
        namespace = configured;
        client = next;
        return 1;
    }

    static void validateUrl(String raw, boolean allowTestLoopback) {
        URI uri = URI.create(raw);
        if (uri.getHost() == null || uri.getRawFragment() != null
                || uri.getRawQuery() != null || uri.getPort() < 0)
            throw new IllegalArgumentException("Invalid Redis URI");
        if ("rediss".equals(uri.getScheme())) return;
        String host = uri.getHost();
        if (!"redis".equals(uri.getScheme()) || !allowTestLoopback
                || !("localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host)
                    || "::1".equals(host)))
            throw new IllegalArgumentException("Night Redis requires TLS (rediss://)");
    }

    private static String key(String input) {
        if (input == null || !input.matches("[a-zA-Z0-9][a-zA-Z0-9:_-]{0,127}"))
            throw new IllegalArgumentException("Invalid Redis key");
        return input;
    }

    private JedisPooled ready() {
        if (client == null) throw new IllegalStateException("Night Redis is not open");
        return client;
    }

    private String fullKey(String input) { return namespace + ":" + key(input); }

    public int put(String key, String value, int ttlSeconds) {
        if (ttlSeconds < 1 || ttlSeconds > 86400 || value == null
                || value.length() > 1_048_576)
            throw new IllegalArgumentException("Invalid Redis TTL or value size");
        return "OK".equals(ready().setex(fullKey(key), ttlSeconds, value)) ? 1 : 0;
    }

    public String get(String key) {
        String found = ready().get(fullKey(key));
        return found == null ? "" : found;
    }

    public int evict(String key) {
        return (int) ready().del(fullKey(key));
    }

    public int health() {
        try {
            return "PONG".equals(ready().ping()) ? 1 : 0;
        } catch (RuntimeException ex) {
            return 0;
        }
    }

    @Override public synchronized void close() {
        if (client != null) client.close();
        client = null;
    }
}
