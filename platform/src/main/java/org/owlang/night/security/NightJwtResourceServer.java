package org.owlang.night.security;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.KeyType;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.JWKMatcher;
import com.nimbusds.jose.jwk.JWKSelector;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * OW-native OAuth2 RS256 JWT resource-server verifier.
 *
 * Trust comes from an operator-pinned issuer, audience and HTTPS JWKS URL.
 * Only signed RSA SHA-256 JWTs with explicit kid are accepted. No token-provided
 * key URLs or algorithms are followed. RemoteJWKSet caches trusted JWKS keys
 * and re-fetches them when an unknown key ID is presented.
 */
public final class NightJwtResourceServer {
    private static final int MAX_TOKEN_CHARS = 16_384;
    private static final int CLOCK_SKEW_SECONDS = 60;
    private final String issuer;
    private final String audience;
    private final JWKSource<SecurityContext> keys;
    private final Clock clock;

    public NightJwtResourceServer(String issuer, String audience, String jwksUri) {
        this(issuer, audience, remoteKeys(jwksUri), Clock.systemUTC());
    }

    // Injection seam for deterministic cryptographic tests, never exposed to OW.
    NightJwtResourceServer(String issuer, String audience,
            JWKSource<SecurityContext> source, Clock clock) {
        if (issuer == null || issuer.isBlank() || audience == null || audience.isBlank()
                || source == null || clock == null)
            throw new IllegalArgumentException("Issuer, audience and JWKS are required");
        this.issuer = issuer;
        this.audience = audience;
        this.keys = source;
        this.clock = clock;
    }

    public static NightJwtResourceServer fromEnvironment() {
        String issuer = System.getenv("NIGHT_JWT_ISSUER");
        String audience = System.getenv("NIGHT_JWT_AUDIENCE");
        String jwks = System.getenv("NIGHT_JWT_JWKS_URI");
        if (issuer == null && audience == null && jwks == null) return null;
        if (issuer == null || issuer.isBlank() || audience == null || audience.isBlank()
                || jwks == null || jwks.isBlank())
            throw new IllegalStateException(
                    "NIGHT_JWT_ISSUER, NIGHT_JWT_AUDIENCE, and NIGHT_JWT_JWKS_URI must all be set");
        return new NightJwtResourceServer(issuer, audience, jwks);
    }

    private static JWKSource<SecurityContext> remoteKeys(String jwksUri) {
        try {
            URI uri = URI.create(jwksUri);
            if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null
                    || uri.getRawUserInfo() != null || uri.getRawFragment() != null)
                throw new IllegalArgumentException("JWKS endpoint must be a pinned HTTPS URL");
            URL url = uri.toURL();
            // Nimbus handles bounded remote retrieval and refresh on previously unknown kids.
            return new RemoteJWKSet<>(url);
        } catch (java.net.MalformedURLException ex) {
            throw new IllegalArgumentException("Invalid JWKS URL", ex);
        }
    }

    /** Returns only issuer-vouched Night role names after full signature/claim verification. */
    public Set<String> roles(String token) throws ParseException, JOSEException {
        if (token == null || token.length() > MAX_TOKEN_CHARS || token.isBlank())
            throw new JOSEException("Invalid JWT length");
        SignedJWT jwt = SignedJWT.parse(token);
        if (!JWSAlgorithm.RS256.equals(jwt.getHeader().getAlgorithm())
                || (jwt.getHeader().getCriticalParams() != null
                    && !jwt.getHeader().getCriticalParams().isEmpty()))
            throw new JOSEException("Unsupported JWT algorithm or critical header");
        String kid = jwt.getHeader().getKeyID();
        if (kid == null || kid.isBlank() || kid.length() > 128)
            throw new JOSEException("Missing or invalid JWT key ID");

        var matcher = new JWKMatcher.Builder().keyType(KeyType.RSA).keyID(kid).build();
        List<JWK> candidates = keys.get(new JWKSelector(matcher), null);
        boolean verified = false;
        for (JWK key : candidates) {
            if (key instanceof RSAKey rsa
                    && jwt.verify(new RSASSAVerifier(rsa.toRSAPublicKey()))) {
                verified = true;
                break;
            }
        }
        if (!verified) throw new JOSEException("JWT signature verification failed");

        JWTClaimsSet claims = jwt.getJWTClaimsSet();
        Instant now = clock.instant();
        Date exp = claims.getExpirationTime();
        Date nbf = claims.getNotBeforeTime();
        Date iat = claims.getIssueTime();
        if (!issuer.equals(claims.getIssuer()) || !claims.getAudience().contains(audience)
                || claims.getSubject() == null || claims.getSubject().isBlank()
                || exp == null || !exp.toInstant().isAfter(now.minusSeconds(CLOCK_SKEW_SECONDS))
                || (nbf != null && nbf.toInstant().isAfter(now.plusSeconds(CLOCK_SKEW_SECONDS)))
                || (iat != null && iat.toInstant().isAfter(now.plusSeconds(CLOCK_SKEW_SECONDS))))
            throw new JOSEException("Invalid JWT claims");

        // No implicit role grant from 'scope', 'groups', or user-controlled headers.
        // An authorized issuer may provide the dedicated 'night_roles' array.
        Set<String> roles = new HashSet<>();
        Object roleClaim = claims.getClaim("night_roles");
        if (roleClaim instanceof List<?> items) {
            for (Object role : items) {
                if (!(role instanceof String name) || !name.matches("[A-Z][A-Z0-9_]{1,31}"))
                    throw new JOSEException("Malformed role claim");
                roles.add(name);
            }
        } else if (roleClaim != null) {
            throw new JOSEException("Malformed role claim");
        }
        roles.add("AUTHENTICATED");
        return Set.copyOf(roles);
    }
}
