package org.owlang.night.data;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NightIsolationTest {
    @Test void rejectsInvalidIsolationAndNestedTransactions() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            assertThrows(IllegalArgumentException.class, () -> jdbc.begin("UNSAFE"));
            assertEquals(1, jdbc.begin("READ_COMMITTED"));
            assertThrows(IllegalStateException.class, () -> jdbc.begin("SERIALIZABLE"));
            jdbc.rollback();
            assertEquals(1, jdbc.begin("SERIALIZABLE"));
            jdbc.cleanupRequest();
            assertThrows(IllegalStateException.class, jdbc::commit);
        }
    }

    @Test void selectedIsolationPersistsUntilTransactionEnds() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            for (String level : List.of("READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE")) {
                assertEquals(1, jdbc.begin(level));
                var rows = jdbc.query("SELECT 1 AS probe", List.of());
                assertEquals(1, rows.size());
                assertEquals(1, jdbc.commit());
            }
        }
    }

    @Test void postgresRollbackWorksOnRealDatabase() {
        Assumptions.assumeTrue(System.getenv().getOrDefault("NIGHT_JDBC_URL", "").startsWith("jdbc:postgresql:"));
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            jdbc.execute("CREATE TABLE IF NOT EXISTS night_pg_rollback (id INTEGER PRIMARY KEY)", List.of());
            jdbc.execute("DELETE FROM night_pg_rollback", List.of());
            jdbc.begin("SERIALIZABLE");
            jdbc.execute("INSERT INTO night_pg_rollback (id) VALUES (?)", List.of(931));
            jdbc.cleanupRequest();
            assertTrue(jdbc.query("SELECT id FROM night_pg_rollback WHERE id = ?", List.of(931)).isEmpty());
            jdbc.begin("READ_COMMITTED");
            jdbc.execute("INSERT INTO night_pg_rollback (id) VALUES (?)", List.of(932));
            jdbc.commit();
            assertEquals(1, jdbc.query("SELECT id FROM night_pg_rollback WHERE id = ?", List.of(932)).size());
            jdbc.execute("DROP TABLE night_pg_rollback", List.of());
        }
    }
}
