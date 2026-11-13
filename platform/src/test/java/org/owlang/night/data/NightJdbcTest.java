package org.owlang.night.data;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NightJdbcTest {
    @Test void transactionCommitAndRollback() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc jdbc = new NightJdbc()) {
            assertEquals(1, jdbc.open());
            jdbc.execute("CREATE TABLE IF NOT EXISTS night_test(id INT PRIMARY KEY, payload INT)", List.of());
            jdbc.execute("DELETE FROM night_test", List.of());

            jdbc.begin();
            jdbc.execute("INSERT INTO night_test(id, payload) VALUES (?, ?)", List.of(1, 99));
            jdbc.rollback();
            assertEquals(0, jdbc.query("SELECT * FROM night_test", List.of()).size());

            jdbc.begin();
            jdbc.execute("INSERT INTO night_test(id, payload) VALUES (?, ?)", List.of(2, 100));
            jdbc.commit();
            assertEquals(1, jdbc.query("SELECT * FROM night_test", List.of()).size());
            assertEquals(100, jdbc.query("SELECT payload FROM night_test WHERE id = ?",
                    List.of(2)).get(0).get("PAYLOAD"));
        }
    }

    @Test void failedRequestRollsBackUncommittedConnection() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            jdbc.execute("CREATE TABLE IF NOT EXISTS night_cleanup(id INT PRIMARY KEY)", List.of());
            jdbc.execute("DELETE FROM night_cleanup", List.of());
            jdbc.begin();
            jdbc.execute("INSERT INTO night_cleanup(id) VALUES (?)", List.of(123));
            jdbc.cleanupRequest();
            assertEquals(0, jdbc.query("SELECT * FROM night_cleanup", List.of()).size());
            assertThrows(IllegalStateException.class, jdbc::rollback);
        }
    }

    @Test void rejectsQueryParameterObjects() {
        Assumptions.assumeTrue(System.getenv("NIGHT_JDBC_URL") != null);
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            assertThrows(IllegalArgumentException.class, () ->
                jdbc.query("SELECT ? AS result", List.of(new Object())));
        }
    }
}
