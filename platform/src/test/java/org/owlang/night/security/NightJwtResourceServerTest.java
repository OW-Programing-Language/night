package org.owlang.night.security;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NightJwtResourceServerTest {
    private static final Instant NOW = Instant.parse("2026-11-13T12:00:00Z");

    private record Fixture(KeyPair pair, NightJwtResourceServer server) {}

    private static Fixture fixture() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair pair = generator.generateKeyPair();
        RSAKey key = new RSAKey.Builder((RSAPublicKey) pair.getPublic()).keyID("key-1").build();
        var keys = new ImmutableJWKSet<SecurityContext>(new JWKSet(key));
        return new Fixture(pair, new NightJwtResourceServer(
                "https://issuer.example", "night-api", keys, Clock.fixed(NOW, ZoneOffset.UTC)));
    }

    private static String token(KeyPair pair, String issuer, String audience,
            Instant expiration, Instant notBefore, Object roles, String kid) throws Exception {
        var builder = new JWTClaimsSet.Builder()
                .issuer(issuer).audience(audience).subject("user-123")
                .issueTime(Date.from(NOW.minusSeconds(1)));
        if (expiration != null) builder.expirationTime(Date.from(expiration));
        if (notBefore != null) builder.notBeforeTime(Date.from(notBefore));
        if (roles != null) builder.claim("night_roles", roles);
        SignedJWT jwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(kid).build(), builder.build());
        jwt.sign(new RSASSASigner(pair.getPrivate()));
        return jwt.serialize();
    }

    @Test void acceptsPinnedIssuerAudienceSignatureAndRoles() throws Exception {
        Fixture f = fixture();
        String good = token(f.pair(), "https://issuer.example", "night-api",
                NOW.plusSeconds(300), NOW.minusSeconds(5), List.of("WRITER"), "key-1");
        assertTrue(f.server().roles(good).contains("WRITER"));
        assertTrue(f.server().roles(good).contains("AUTHENTICATED"));
        assertFalse(f.server().roles(good).contains("ADMIN"));
    }

    @Test void rejectsWrongIssuerAudienceExpiredMissingExpiryAndFutureNbf() throws Exception {
        Fixture f = fixture();
        assertThrows(Exception.class, () -> f.server().roles(token(f.pair(),
                "https://evil.example", "night-api", NOW.plusSeconds(300), null, null, "key-1")));
        assertThrows(Exception.class, () -> f.server().roles(token(f.pair(),
                "https://issuer.example", "other-api", NOW.plusSeconds(300), null, null, "key-1")));
        assertThrows(Exception.class, () -> f.server().roles(token(f.pair(),
                "https://issuer.example", "night-api", NOW.minusSeconds(120), null, null, "key-1")));
        assertThrows(Exception.class, () -> f.server().roles(token(f.pair(),
                "https://issuer.example", "night-api", null, null, null, "key-1")));
        assertThrows(Exception.class, () -> f.server().roles(token(f.pair(),
                "https://issuer.example", "night-api", NOW.plusSeconds(300),
                NOW.plusSeconds(180), null, "key-1")));
    }

    @Test void rejectsUnknownKidForgedSignatureMalformedRolesAndUntrustedKeyUrl() throws Exception {
        Fixture f = fixture();
        assertThrows(Exception.class, () -> f.server().roles(token(f.pair(),
                "https://issuer.example", "night-api", NOW.plusSeconds(300), null, null, "attacker")));
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair attacker = gen.generateKeyPair();
        assertThrows(Exception.class, () -> f.server().roles(token(attacker,
                "https://issuer.example", "night-api", NOW.plusSeconds(300), null, List.of("ADMIN"), "key-1")));
        assertThrows(Exception.class, () -> f.server().roles(token(f.pair(),
                "https://issuer.example", "night-api", NOW.plusSeconds(300), null, "ADMIN", "key-1")));
        assertThrows(IllegalArgumentException.class, () -> new NightJwtResourceServer(
                "https://issuer.example", "night-api", "http://private.example/jwks"));
    }
}
