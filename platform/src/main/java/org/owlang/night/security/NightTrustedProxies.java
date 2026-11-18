package org.owlang.night.security;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves client IPs only through explicitly trusted TCP peer/proxy hops.
 * Never resolves hostnames and never trusts arbitrary inbound forwarding headers.
 * This is an application quota primitive, not a replacement for edge enforcement.
 */
public final class NightTrustedProxies {
    private record Network(byte[] address, int prefixBits) {
        boolean contains(InetAddress candidate) {
            byte[] target = candidate.getAddress();
            if (target.length != address.length) return false;
            int full = prefixBits / 8, remainder = prefixBits % 8;
            for (int i = 0; i < full; i++) {
                if (address[i] != target[i]) return false;
            }
            if (remainder == 0) return true;
            int mask = 0xff << (8 - remainder);
            return (address[full] & mask) == (target[full] & mask);
        }
    }

    private final List<Network> networks;

    private NightTrustedProxies(List<Network> networks) {
        this.networks = List.copyOf(networks);
    }

    public static NightTrustedProxies none() {
        return new NightTrustedProxies(List.of());
    }

    /**
     * Comma-separated CIDRs. Explicitly configured proxy addresses only.
     * A /0 is rejected because it would let any remote peer spoof its IP.
     */
    public static NightTrustedProxies fromCidrs(String cidrs) {
        if (cidrs == null || cidrs.isBlank()) return none();
        if (cidrs.length() > 4096) throw new IllegalArgumentException("Trusted proxy configuration too large");
        String[] entries = cidrs.split(",", -1);
        if (entries.length > 32) throw new IllegalArgumentException("Too many trusted proxy CIDRs");
        List<Network> networks = new ArrayList<>(entries.length);
        for (String entry : entries) {
            String[] parts = entry.strip().split("/", -1);
            if (parts.length > 2 || parts[0].isBlank())
                throw new IllegalArgumentException("Invalid trusted proxy CIDR");
            InetAddress address = literalAddress(parts[0]);
            int bits = address.getAddress().length * 8;
            int prefix = bits;
            if (parts.length == 2) {
                if (!parts[1].matches("(0|[1-9][0-9]{0,2})"))
                    throw new IllegalArgumentException("Invalid trusted proxy prefix");
                prefix = Integer.parseInt(parts[1]);
            }
            if (prefix < 1 || prefix > bits)
                throw new IllegalArgumentException("Trusted proxy prefix must be nonzero and fit its IP family");
            networks.add(new Network(address.getAddress(), prefix));
        }
        return new NightTrustedProxies(networks);
    }

    private boolean trusted(InetAddress address) {
        for (Network network : networks) {
            if (network.contains(address)) return true;
        }
        return false;
    }

    /**
     * Start with the actual TCP peer and walk X-Forwarded-For from right to
     * left while the current hop is trusted. A forged leftmost address cannot
     * override the first untrusted hop. Forwarded/X-Real-IP are ignored.
     */
    public InetAddress clientAddress(InetAddress peer, List<String> forwardedFor) {
        if (peer == null) throw new IllegalArgumentException("Missing TCP peer");
        if (!trusted(peer) || forwardedFor == null || forwardedFor.isEmpty()) return peer;
        if (forwardedFor.size() != 1 || forwardedFor.get(0) == null
                || forwardedFor.get(0).length() > 1024)
            throw new IllegalArgumentException("Invalid X-Forwarded-For header");
        String[] hops = forwardedFor.get(0).split(",", -1);
        if (hops.length == 0 || hops.length > 16)
            throw new IllegalArgumentException("Invalid X-Forwarded-For hop count");
        InetAddress selected = peer;
        for (int i = hops.length - 1; i >= 0 && trusted(selected); i--) {
            selected = literalAddress(hops[i].strip());
        }
        return selected;
    }

    /** Strict numeric address parsing, with no DNS lookups or URI/port syntax. */
    private static InetAddress literalAddress(String value) {
        if (value == null || value.length() > 45 || value.isBlank() || value.contains("%"))
            throw new IllegalArgumentException("Invalid IP literal");
        if (value.contains(":")) {
            if (!value.matches("[0-9a-fA-F:]+"))
                throw new IllegalArgumentException("Invalid IPv6 literal");
        } else {
            String[] octets = value.split("\\.", -1);
            if (octets.length != 4) throw new IllegalArgumentException("Invalid IPv4 literal");
            for (String octet : octets) {
                if (!octet.matches("(0|[1-9][0-9]{0,2})")
                        || Integer.parseInt(octet) > 255)
                    throw new IllegalArgumentException("Invalid IPv4 literal");
            }
        }
        try {
            return InetAddress.getByName(value);
        } catch (UnknownHostException ex) {
            throw new IllegalArgumentException("Invalid numeric IP address", ex);
        }
    }
}
