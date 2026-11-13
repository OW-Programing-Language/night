package org.owlang.night.http;

import dev.oreo.Interpreter;

import java.io.IOException;
import java.io.InputStream;
import java.net.IDN;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Outbound HTTPS GET client. Requires exact host allowlisting. No redirects,
 * arbitrary headers, cookie persistence, request body, or credentials.
 *
 * This is a defensive HTTP client foundation, not a formally complete SSRF
 * barrier: DNS can change between name resolution and the TLS connection.
 * For high assurance route requests through an egress proxy/firewall.
 */
public final class NightHttpClient {
    private static final int MAX_RESPONSE_BYTES = 1_048_576;
    private final Set<String> allowedHosts;
    private final HttpClient client;
    private int consecutiveFailures;
    private long openUntil;

    public NightHttpClient(String hosts) {
        if (hosts == null || hosts.isBlank())
            throw new IllegalStateException("NIGHT_HTTP_ALLOWED_HOSTS must specify HTTPS hosts");
        Set<String> parsed = Arrays.stream(hosts.split(","))
                .map(String::strip).map(String::toLowerCase)
                .collect(Collectors.toUnmodifiableSet());
        for (String host : parsed) {
            if (!host.matches("[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?")
                    || host.contains("..") || host.indexOf('.') < 0
                    || host.matches("[0-9.]+"))
                throw new IllegalArgumentException("Invalid allowlisted DNS host");
        }
        this.allowedHosts = parsed;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    public static NightHttpClient fromEnvironment() {
        return new NightHttpClient(System.getenv("NIGHT_HTTP_ALLOWED_HOSTS"));
    }

    public URI validate(String rawUrl) {
        if (rawUrl == null || rawUrl.length() > 2048)
            throw new IllegalArgumentException("Invalid remote URL length");
        URI uri = URI.create(rawUrl);
        String host = uri.getHost();
        if (!"https".equalsIgnoreCase(uri.getScheme()) || host == null
                || uri.getRawUserInfo() != null || uri.getRawFragment() != null
                || (uri.getPort() != -1 && uri.getPort() != 443)
                || !allowedHosts.contains(IDN.toASCII(host).toLowerCase(Locale.ROOT)))
            throw new IllegalArgumentException("Remote URL is not in the HTTPS allowlist");
        return uri;
    }

    public Interpreter.OwRecord get(String rawUrl) {
        URI uri = validate(rawUrl);
        synchronized (this) {
            if (System.nanoTime() < openUntil)
                throw new IllegalStateException("Night outbound HTTP circuit is open");
        }
        // Defense in depth, not a DNS-pinning guarantee.
        rejectPrivateAddresses(uri.getHost());
        HttpRequest request = HttpRequest.newBuilder(uri)
                .GET().timeout(Duration.ofSeconds(8))
                .header("Accept", "application/json, text/plain; q=0.9")
                .build();
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                HttpResponse<InputStream> response = client.send(
                        request, HttpResponse.BodyHandlers.ofInputStream());
                byte[] body;
                try (InputStream stream = response.body()) {
                    body = stream.readNBytes(MAX_RESPONSE_BYTES + 1);
                }
                if (body.length > MAX_RESPONSE_BYTES)
                    throw new IOException("Night outbound response size exceeded 1 MiB");
                int status = response.statusCode();
                if ((status == 502 || status == 503 || status == 504) && attempt == 0) {
                    failure();
                    continue;
                }
                success();
                Interpreter.OwRecord result = new Interpreter.OwRecord();
                result.put("status", status);
                result.put("body", new String(body, StandardCharsets.UTF_8));
                return result;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Night outbound HTTP interrupted", ex);
            } catch (IOException ex) {
                failure();
                if (attempt > 0) throw new IllegalStateException("Night outbound HTTP failed", ex);
            }
        }
        throw new IllegalStateException("Night outbound HTTP retry budget exhausted");
    }

    private synchronized void failure() {
        if (++consecutiveFailures >= 5) {
            openUntil = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            consecutiveFailures = 0;
        }
    }
    private synchronized void success() {
        consecutiveFailures = 0;
        openUntil = 0;
    }

    private static void rejectPrivateAddresses(String hostname) {
        try {
            for (InetAddress address : InetAddress.getAllByName(hostname)) {
                if (address.isAnyLocalAddress() || address.isLoopbackAddress()
                        || address.isLinkLocalAddress() || address.isSiteLocalAddress()
                        || address.isMulticastAddress() || isReserved(address))
                    throw new IllegalArgumentException("Private or reserved DNS target blocked");
            }
        } catch (java.net.UnknownHostException ex) {
            throw new IllegalStateException("Outbound host does not resolve", ex);
        }
    }

    private static boolean isReserved(InetAddress address) {
        byte[] ip = address.getAddress();
        if (address instanceof Inet6Address) {
            return (ip[0] & 0xfe) == 0xfc; // IPv6 Unique Local Address
        }
        if (address instanceof Inet4Address) {
            int a = ip[0] & 255, b = ip[1] & 255, c = ip[2] & 255;
            return a == 0 || a >= 224 || a == 100 && b >= 64 && b <= 127
                    || a == 192 && b == 0 && c == 0
                    || a == 192 && b == 0 && c == 2
                    || a == 198 && b == 51 && c == 100
                    || a == 203 && b == 0 && c == 113
                    || a == 198 && (b == 18 || b == 19);
        }
        return true;
    }
}
