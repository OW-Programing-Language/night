package org.owlang.night.cache;

import java.util.concurrent.ConcurrentHashMap;

/** Local bounded TTL cache. It does not claim distributed Spring Cache semantics. */
public final class NightCache {
    private record Entry(Object value, long expiresAt) {}
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private static final int MAX_ENTRIES = 1024;

    public int put(String key, Object value, int ttlSeconds) {
        if (key.length() > 256 || ttlSeconds < 1 || ttlSeconds > 86_400)
            throw new IllegalArgumentException("Invalid cache key or TTL");
        if (entries.size() >= MAX_ENTRIES && !entries.containsKey(key))
            throw new IllegalStateException("Night cache is full");
        entries.put(key, new Entry(value, System.nanoTime() +
                ttlSeconds * 1_000_000_000L));
        return 1;
    }

    public Object get(String key) {
        Entry entry = entries.get(key);
        if (entry == null) return null;
        if (System.nanoTime() - entry.expiresAt >= 0) {
            entries.remove(key, entry);
            return null;
        }
        return entry.value;
    }

    public int evict(String key) { return entries.remove(key) == null ? 0 : 1; }
    public int clear() { entries.clear(); return 1; }
}
