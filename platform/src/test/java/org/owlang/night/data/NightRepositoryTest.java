package org.owlang.night.data;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NightRepositoryTest {
    @Test void pagedQueriesAndOptimisticVersionConflict() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            NightRepository repository = new NightRepository(jdbc);
            jdbc.execute("CREATE TABLE IF NOT EXISTS night_repo_test (id INTEGER PRIMARY KEY, value INTEGER, version INTEGER NOT NULL)", List.of());
            jdbc.execute("DELETE FROM night_repo_test", List.of());
            jdbc.execute("INSERT INTO night_repo_test (id, value, version) VALUES (?, ?, ?)", List.of(1, 10, 0));
            jdbc.execute("INSERT INTO night_repo_test (id, value, version) VALUES (?, ?, ?)", List.of(2, 20, 0));
            assertEquals(1, repository.page("night_repo_test", "id", 1, 0).size());
            assertEquals(1, repository.page("night_repo_test", "id", 1, 1).size());
            assertEquals(1, repository.updateVersioned("night_repo_test", "id", "value", 1, 50, 0));
            assertEquals(0, repository.updateVersioned("night_repo_test", "id", "value", 1, 80, 0));
            assertEquals(1, repository.updateVersioned("night_repo_test", "id", "value", 1, 80, 1));
            assertEquals(80, ((Number) jdbc.query("SELECT value FROM night_repo_test WHERE id = ?", List.of(1))
                .get(0).values().iterator().next()).intValue());
            jdbc.execute("DROP TABLE night_repo_test", List.of());
        }
    }

    @Test void rejectsSqlIdentifiersAndUnboundedPages() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            NightRepository repo = new NightRepository(jdbc);
            assertThrows(IllegalArgumentException.class, () -> repo.page("users;DROP TABLE users", "id", 10, 0));
            assertThrows(IllegalArgumentException.class, () -> repo.page("users", "id DESC", 10, 0));
            assertThrows(IllegalArgumentException.class, () -> repo.page("users", "id", 1000, 0));
            assertThrows(IllegalArgumentException.class, () -> repo.updateVersioned("users", "id", "name", 1, "x", -1));
            assertThrows(IllegalArgumentException.class, () -> repo.updateVersioned("users", "id; DROP", "name", 1, "x", 0));
        }
    }
}
