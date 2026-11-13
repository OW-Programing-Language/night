package org.owlang.night.security;

import org.mindrot.jbcrypt.BCrypt;
import org.owlang.night.data.NightJdbc;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/**
 * Database-backed password credentials with bcrypt, fixed lockout policy,
 * single-use expiring reset tokens, and atomic state updates.
 *
 * Reset token issuance is a privileged service operation: callers MUST deliver
 * the token out of band, enforce request quotas, and never return it from a
 * public unauthenticated HTTP route.
 */
public final class NightCredentialStore {
    private static final int BCRYPT_COST = 12;
    private static final int MAX_FAILURES = 5;
    private static final long LOCK_MS = 15 * 60_000L;
    private static final long RESET_MS = 15 * 60_000L;
    private static final String DUMMY = BCrypt.hashpw("night-dummy-credential", BCrypt.gensalt(BCRYPT_COST));

    private final NightJdbc jdbc;
    private final Clock clock;
    private final SecureRandom random;

    public NightCredentialStore(NightJdbc jdbc) {
        this(jdbc, Clock.systemUTC(), new SecureRandom());
    }

    NightCredentialStore(NightJdbc jdbc, Clock clock, SecureRandom random) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.random = random;
    }

    private static String validateUsername(String username) {
        if (username == null || !username.matches("[A-Za-z0-9][A-Za-z0-9._@-]{2,127}"))
            throw new IllegalArgumentException("Invalid account identifier");
        return username;
    }

    private static String validatePassword(String password) {
        if (password == null || password.length() < 12
                || password.getBytes(StandardCharsets.UTF_8).length > 72)
            throw new IllegalArgumentException("Password must be at least 12 characters and at most 72 UTF-8 bytes");
        return password;
    }

    public int register(String username, String password) {
        validateUsername(username);
        validatePassword(password);
        return jdbc.execute("""
                INSERT INTO night_credentials
                  (username, password_hash, failed_attempts, locked_until, reset_expires_at)
                VALUES (?, ?, 0, 0, 0)
                """, List.of(username, BCrypt.hashpw(password, BCrypt.gensalt(BCRYPT_COST))));
    }

    public int verify(String username, String password) {
        validateUsername(username);
        if (password == null || password.length() > 1024) return 0;
        var matches = jdbc.query("""
                SELECT password_hash AS "password_hash", locked_until AS "locked_until" FROM night_credentials WHERE username = ?
                """, List.of(username));
        if (matches.isEmpty()) {
            BCrypt.checkpw(password, DUMMY);
            return 0;
        }

        var row = matches.get(0);
        String hash = (String) row.get("password_hash");
        long lockedUntil = ((Number) row.get("locked_until")).longValue();
        long now = clock.millis();
        // Bcrypt work remains constant on locked accounts, without leaking their status.
        boolean valid = BCrypt.checkpw(password, hash);
        if (lockedUntil > now) return 0;

        if (lockedUntil != 0) {
            jdbc.execute("""
                    UPDATE night_credentials SET failed_attempts = 0, locked_until = 0
                    WHERE username = ? AND locked_until > 0 AND locked_until <= ?
                    """, List.of(username, now));
        }

        if (valid) {
            return jdbc.execute("""
                    UPDATE night_credentials SET failed_attempts = 0, locked_until = 0
                    WHERE username = ? AND password_hash = ? AND locked_until <= ?
                    """, List.of(username, hash, now));
        }
        jdbc.execute("""
                UPDATE night_credentials
                SET failed_attempts = failed_attempts + 1,
                    locked_until = CASE WHEN failed_attempts + 1 >= ? THEN ? ELSE 0 END
                WHERE username = ? AND password_hash = ? AND locked_until <= ?
                """, List.of(MAX_FAILURES, now + LOCK_MS, username, hash, now));
        return 0;
    }

    /**
     * Produce a one-time, 256-bit URL-safe token for OUT-OF-BAND delivery.
     * Only its SHA-256 digest is stored in SQL. The caller must protect the
     * plaintext token until it is delivered, and not log it.
     */
    public String issueReset(String username) {
        validateUsername(username);
        byte[] tokenBytes = new byte[32];
        random.nextBytes(tokenBytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(tokenBytes);
        String digest = digest(token);
        int changed = jdbc.execute("""
                UPDATE night_credentials SET reset_hash = ?, reset_expires_at = ?
                WHERE username = ?
                """, List.of(digest, clock.millis() + RESET_MS, username));
        return changed == 1 ? token : "";
    }

    public int completeReset(String username, String token, String newPassword) {
        validateUsername(username);
        validatePassword(newPassword);
        if (token == null || !token.matches("[A-Za-z0-9_-]{43}")) return 0;
        String hash = BCrypt.hashpw(newPassword, BCrypt.gensalt(BCRYPT_COST));
        return jdbc.execute("""
                UPDATE night_credentials
                SET password_hash = ?, failed_attempts = 0, locked_until = 0,
                    reset_hash = NULL, reset_expires_at = 0
                WHERE username = ? AND reset_hash = ? AND reset_expires_at > ?
                """, List.of(hash, username, digest(token), clock.millis()));
    }

    private static String digest(String token) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256")
                    .digest(token.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
