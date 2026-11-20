package org.owlang.night.data;

import dev.oreo.Interpreter;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NightCrudRepositoryTest {
    private static final String TABLE = "night_generated_crud_test";

    private static Interpreter.OwRecord user(int id, String username, int score) {
        Interpreter.OwRecord record = new Interpreter.OwRecord();
        record.put("id", id);
        record.put("username", username);
        record.put("score", score);
        return record;
    }

    @Test void createsListsGetsPagesSortsAndDeletes() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc db = new NightJdbc()) {
            db.open();
            db.execute("CREATE TABLE IF NOT EXISTS " + TABLE +
                    " (id INTEGER PRIMARY KEY, username VARCHAR(100), score INTEGER)", List.of());
            db.execute("DELETE FROM " + TABLE, List.of());
            try {
                NightCrudRepository repo = new NightCrudRepository(db);
                assertEquals(1, repo.save(TABLE, "id", user(3, "gamma", 5)));
                assertEquals(1, repo.save(TABLE, "id", user(1, "alpha", 10)));
                assertEquals(1, repo.save(TABLE, "id", user(2, "beta", 20)));
                assertEquals(1, repo.existsById(TABLE, "id", 1));
                assertEquals(0, repo.existsById(TABLE, "id", 999));
                assertEquals(0, repo.findById(TABLE, "id", 999));
                assertEquals("beta", ((Interpreter.OwRecord) repo.findById(TABLE, "id", 2)).get("username"));
                assertEquals(1, ((Number) repo.findAll(TABLE, "id").get(0).get("id")).intValue());
                assertEquals(3, repo.findAll(TABLE, "id").size());
                assertEquals(20, ((Number) repo.page(TABLE, "id", "-score", 1, 0)
                        .get(0).get("score")).intValue());
                assertEquals(10, ((Number) repo.page(TABLE, "id", "-score", 1, 1)
                        .get(0).get("score")).intValue());
                assertEquals(1, repo.deleteById(TABLE, "id", 2));
                assertEquals(0, repo.deleteById(TABLE, "id", 2));
                assertEquals(2, repo.findAll(TABLE, "id").size());
                assertThrows(IllegalStateException.class, () -> repo.save(TABLE, "id", user(1, "dupe", 0)));
            } finally {
                db.execute("DROP TABLE " + TABLE, List.of());
            }
        }
    }

    @Test void generatedRepositoryParticipatesInJdbcTransactions() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc db = new NightJdbc()) {
            db.open();
            db.execute("CREATE TABLE IF NOT EXISTS " + TABLE +
                    " (id INTEGER PRIMARY KEY, username VARCHAR(100), score INTEGER)", List.of());
            db.execute("DELETE FROM " + TABLE, List.of());
            try {
                NightCrudRepository repo = new NightCrudRepository(db);
                assertEquals(1, db.begin());
                repo.save(TABLE, "id", user(51, "rollback", 0));
                assertEquals(1, repo.existsById(TABLE, "id", 51));
                assertEquals(1, db.rollback());
                assertEquals(0, repo.existsById(TABLE, "id", 51));
                assertEquals(1, db.begin());
                repo.save(TABLE, "id", user(52, "commit", 0));
                assertEquals(1, db.commit());
                assertEquals(1, repo.existsById(TABLE, "id", 52));
            } finally {
                db.execute("DROP TABLE " + TABLE, List.of());
            }
        }
    }

    @Test void refusesSqlInjectionAndUnsafeRecordValues() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc db = new NightJdbc()) {
            db.open();
            NightCrudRepository repo = new NightCrudRepository(db);
            assertThrows(IllegalArgumentException.class, () ->
                    repo.findById("users; DROP TABLE users", "id", 1));
            assertThrows(IllegalArgumentException.class, () ->
                    repo.findById("users", "id OR 1=1", 1));
            assertThrows(IllegalArgumentException.class, () ->
                    repo.page("users", "id", "score;DELETE", 1, 0));
            assertThrows(IllegalArgumentException.class, () ->
                    repo.page("users", "id", "id", 501, 0));
            assertThrows(IllegalArgumentException.class, () ->
                    repo.page("users", "id", "id", 20, -1));
            Interpreter.OwRecord record = user(1, "valid", 5);
            record.put("malicious SQL", 1);
            assertThrows(IllegalArgumentException.class, () -> repo.save("users", "id", record));
            Interpreter.OwRecord nullValue = user(1, "valid", 5);
            nullValue.put("optional", null);
            assertThrows(IllegalArgumentException.class, () -> repo.save("users", "id", nullValue));
            assertThrows(IllegalArgumentException.class, () ->
                    repo.save("users", "id", "not a record"));
        }
    }
}
