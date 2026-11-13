package org.owlang.night.security;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightJdbc;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class NightCredentialStoreTest {
    private static final class MutableClock extends Clock {
        Instant current = Instant.parse("2026-11-13T12:00:00Z");
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return current; }
        void advance(long seconds) { current = current.plusSeconds(seconds); }
    }

    private static String username() {
        return "account-" + UUID.randomUUID();
    }

    private static void schema(NightJdbc jdbc) {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS night_credentials (
                  username VARCHAR(128) PRIMARY KEY,
                  password_hash VARCHAR(128) NOT NULL,
                  failed_attempts INTEGER NOT NULL DEFAULT 0,
                  locked_until BIGINT NOT NULL DEFAULT 0,
                  reset_hash CHAR(64),
                  reset_expires_at BIGINT NOT NULL DEFAULT 0
                )
                """, List.of());
    }

    @Test void registrationLoginLockoutAndExpiry() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        MutableClock clock = new MutableClock();
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            schema(jdbc);
            NightCredentialStore store = new NightCredentialStore(jdbc, clock, new SecureRandom());
            String name = username();
            String password = "Correct+Password-Only-2026";
            assertEquals(1, store.register(name, password));
            var rows = jdbc.query("SELECT password_hash FROM night_credentials WHERE username = ?", List.of(name));
            assertFalse(rows.get(0).values().iterator().next().toString().contains(password));
            assertEquals(1, store.verify(name, password));
            assertEquals(0, store.verify("nonexistent-abc123", password));
            for (int i = 0; i < 5; i++)
                assertEquals(0, store.verify(name, "DefinitelyWrongPassword"));
            assertEquals(0, store.verify(name, password), "lockout denies even a correct password");
            clock.advance(16 * 60);
            assertEquals(1, store.verify(name, password));
            assertThrows(IllegalArgumentException.class, () -> store.register(username(), "short"));
        }
    }

    @Test void resetIsOneTimeAndExpires() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        MutableClock clock = new MutableClock();
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            schema(jdbc);
            NightCredentialStore store = new NightCredentialStore(jdbc, clock, new SecureRandom());
            String name = username();
            String oldPassword = "Original+Secure-Password";
            String newPassword = "New+Secure-Password-2026";
            assertEquals(1, store.register(name, oldPassword));
            assertEquals("", store.issueReset("unknown-account-0123"));
            String reset = store.issueReset(name);
            assertEquals(43, reset.length());
            assertEquals(0, store.completeReset(name, "bad", newPassword));
            assertEquals(1, store.completeReset(name, reset, newPassword));
            assertEquals(0, store.completeReset(name, reset, oldPassword));
            assertEquals(0, store.verify(name, oldPassword));
            assertEquals(1, store.verify(name, newPassword));
            String expired = store.issueReset(name);
            clock.advance(16 * 60);
            assertEquals(0, store.completeReset(name, expired, oldPassword));
            assertEquals(1, store.verify(name, newPassword));
        }
    }
}
