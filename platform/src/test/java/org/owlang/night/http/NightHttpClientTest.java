package org.owlang.night.http;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NightHttpClientTest {
    @Test void requiresExplicitHostsAndBlocksHttpAndBadAuthorities() {
        assertThrows(IllegalStateException.class, () -> new NightHttpClient(""));
        assertThrows(IllegalArgumentException.class, () -> new NightHttpClient("*.example.com"));
        NightHttpClient client = new NightHttpClient("api.example.com,other.example.com");
        assertEquals("api.example.com", client.validate("https://api.example.com/v1").getHost());
        assertThrows(IllegalArgumentException.class, () -> client.validate("http://api.example.com/v1"));
        assertThrows(IllegalArgumentException.class, () -> client.validate("https://api.example.com:8443/v1"));
        assertThrows(IllegalArgumentException.class, () -> client.validate("https://evil.example.com/v1"));
        assertThrows(IllegalArgumentException.class, () -> client.validate("https://api.example.com@evil.example/v1"));
        assertThrows(IllegalArgumentException.class, () -> client.validate("https://user:pass@api.example.com/v1"));
        assertThrows(IllegalArgumentException.class, () -> client.validate("https://api.example.com/v1#fragment"));
        assertThrows(IllegalArgumentException.class, () -> client.validate("https://127.0.0.1/"));
    }

    @Test void cannotUseLocalhostOrLiteralIpAsAllowlistedHost() {
        assertThrows(IllegalArgumentException.class, () -> new NightHttpClient("localhost"));
        assertThrows(IllegalArgumentException.class, () -> new NightHttpClient("127.0.0.1"));
        assertThrows(IllegalArgumentException.class, () -> new NightHttpClient("192.168.1.1"));
    }
}
