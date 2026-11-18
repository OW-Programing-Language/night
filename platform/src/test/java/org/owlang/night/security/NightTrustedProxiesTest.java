package org.owlang.night.security;

import org.junit.jupiter.api.Test;
import java.net.InetAddress;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NightTrustedProxiesTest {
    private static InetAddress ip(String value) throws Exception {
        return InetAddress.getByName(value);
    }

    @Test void neverTrustsForwardingFromUnlistedSocketPeer() throws Exception {
        var policy = NightTrustedProxies.fromCidrs("192.0.2.0/24");
        assertEquals(ip("203.0.113.4"), policy.clientAddress(
                ip("203.0.113.4"), List.of("1.2.3.4")));
        assertEquals(ip("203.0.113.4"), NightTrustedProxies.none().clientAddress(
                ip("203.0.113.4"), List.of("1.2.3.4")));
    }

    @Test void selectsLastUntrustedHopNotUserControlledLeftmost() throws Exception {
        var policy = NightTrustedProxies.fromCidrs("192.0.2.10/32,198.51.100.0/24");
        assertEquals(ip("203.0.113.11"), policy.clientAddress(ip("192.0.2.10"),
                List.of("6.6.6.6, 203.0.113.11, 198.51.100.22")));
        assertEquals(ip("203.0.113.11"), policy.clientAddress(ip("192.0.2.10"),
                List.of("203.0.113.11, 198.51.100.22")));
        assertEquals(ip("203.0.113.11"), policy.clientAddress(ip("192.0.2.10"),
                List.of("203.0.113.11")));
    }

    @Test void handlesIpv6CidrsAndUsesActualPeerWithoutHeader() throws Exception {
        var policy = NightTrustedProxies.fromCidrs("2001:db8::/48");
        assertEquals(ip("2001:db9::8"), policy.clientAddress(ip("2001:db8::1"),
                List.of("2001:db9::8")));
        assertEquals(ip("2001:db8::1"), policy.clientAddress(ip("2001:db8::1"), null));
    }

    @Test void rejectsMalformedForwardingOnlyFromTrustedPeer() throws Exception {
        var policy = NightTrustedProxies.fromCidrs("192.0.2.10");
        for (String invalid : List.of("example.com", "", "1.2.3.4,", "1.2.3.4:8443",
                "01.2.3.4", "999.2.3.4", "1.2.3.4, not-an-ip")) {
            assertThrows(IllegalArgumentException.class, () ->
                    policy.clientAddress(ipUnchecked("192.0.2.10"), List.of(invalid)));
        }
        assertThrows(IllegalArgumentException.class, () ->
                policy.clientAddress(ip("192.0.2.10"), List.of("1.2.3.4", "5.6.7.8")));
        String tooMany = String.join(",", java.util.Collections.nCopies(17, "192.0.2.10"));
        assertThrows(IllegalArgumentException.class, () ->
                policy.clientAddress(ip("192.0.2.10"), List.of(tooMany)));
    }

    private static InetAddress ipUnchecked(String literal) {
        try { return InetAddress.getByName(literal); }
        catch (Exception ex) { throw new AssertionError(ex); }
    }

    @Test void failsConfigurationClosedOnAmbiguousOrDangerouslyBroadCidrs() {
        for (String invalid : List.of("0.0.0.0/0", "::/0", "192.0.2.0/33",
                "2001:db8::/129", "192.0.2.1/abc", "example.com", "1.2.3.4,",
                "01.2.3.4", "192.0.2.1/-1")) {
            assertThrows(IllegalArgumentException.class,
                    () -> NightTrustedProxies.fromCidrs(invalid));
        }
    }

    @Test void quotaTracksResolvedClientInsteadOfSharedProxyAddress() throws Exception {
        var policy = NightTrustedProxies.fromCidrs("192.0.2.10");
        var clock = new NightIpQuotaTest.MutableClock(100_000);
        var quota = new NightIpQuota(1, 60, 10, clock, policy);
        String alice = policy.clientAddress(ip("192.0.2.10"), List.of("203.0.113.1")).getHostAddress();
        String bob = policy.clientAddress(ip("192.0.2.10"), List.of("203.0.113.2")).getHostAddress();
        assertTrue(quota.allow(alice));
        assertFalse(quota.allow(alice));
        assertTrue(quota.allow(bob));
    }
}
