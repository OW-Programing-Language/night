package org.owlang.night.security;

import com.sun.net.httpserver.HttpExchange;
import dev.oreo.RuntimeExtension;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Per-process fixed-window IP quota. Uses only the TCP peer address: forwarded
 * headers are deliberately ignored until a trusted-proxy policy exists.
 * This is not a distributed quota or a DDoS mitigation layer.
 */
public final class NightIpQuota {
    private record Window(long start, int used) {}
    private final Map<String, Window> windows = new HashMap<>();
    private final Clock clock;
    private final int requests;
    private final long windowMillis;
    private final int maxEntries;

    public NightIpQuota(int requests, long windowSeconds, int maxEntries, Clock clock) {
        if (requests < 1 || requests > 1_000_000 || windowSeconds < 1
                || windowSeconds > 86400 || maxEntries < 1 || maxEntries > 1_000_000)
            throw new IllegalArgumentException("Invalid Night IP quota configuration");
        this.requests = requests;
        this.windowMillis = windowSeconds * 1000;
        this.maxEntries = maxEntries;
        this.clock = clock;
    }

    public static NightIpQuota fromEnvironment() {
        String raw = System.getenv("NIGHT_IP_QUOTA_PER_MINUTE");
        if (raw == null || raw.isBlank()) return null;
        try {
            return new NightIpQuota(Integer.parseInt(raw), 60, 100_000, Clock.systemUTC());
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("NIGHT_IP_QUOTA_PER_MINUTE must be an integer", ex);
        }
    }

    public RuntimeExtension.Response before(HttpExchange exchange) {
        InetSocketAddress address = exchange.getRemoteAddress();
        if (address == null || address.getAddress() == null)
            return rejected(503, "peer_unavailable");
        return allow(address.getAddress().getHostAddress())
                ? null : rejected(429, "rate_limited");
    }

    /** True when this request may proceed; synchronized across HTTP worker threads. */
    public synchronized boolean allow(String peer) {
        if (peer == null || peer.isBlank()) return false;
        long now = clock.millis();
        Window previous = windows.get(peer);
        if (previous == null || now < previous.start || now - previous.start >= windowMillis) {
            if (previous == null && windows.size() >= maxEntries) {
                Iterator<Map.Entry<String, Window>> iter = windows.entrySet().iterator();
                while (iter.hasNext()) {
                    Window w = iter.next().getValue();
                    if (now < w.start || now - w.start >= windowMillis) iter.remove();
                }
                // Fail closed when quota tracking capacity is exhausted.
                if (windows.size() >= maxEntries) return false;
            }
            windows.put(peer, new Window(now, 1));
            return true;
        }
        if (previous.used >= requests) return false;
        windows.put(peer, new Window(previous.start, previous.used + 1));
        return true;
    }

    private static RuntimeExtension.Response rejected(int status, String error) {
        return new RuntimeExtension.Response(status, "application/json; charset=utf-8",
                "{\"error\":\"" + error + "\"}");
    }
}
