package org.owlang.night.data;

import dev.oreo.Interpreter;
import java.util.List;

/**
 * Minimal safe SQL repository helper. Identifier names must be literal,
 * validated identifiers (never values supplied by an HTTP client).
 * Values are always bound by NightJdbc.
 */
public final class NightRepository {
    private final NightJdbc jdbc;

    public NightRepository(NightJdbc jdbc) { this.jdbc = jdbc; }

    private static String identifier(String value) {
        if (value == null || !value.matches("[A-Za-z_][A-Za-z_0-9]{0,62}"))
            throw new IllegalArgumentException("Unsafe SQL identifier");
        return value;
    }

    public List<Interpreter.OwRecord> page(
            String table, String orderColumn, int limit, int offset) {
        if (limit < 1 || limit > 500 || offset < 0 || offset > 10_000_000)
            throw new IllegalArgumentException("Invalid pagination bounds");
        return jdbc.query("SELECT * FROM " + identifier(table)
            + " ORDER BY " + identifier(orderColumn)
            + " LIMIT ? OFFSET ?", List.of(limit, offset));
    }

    /**
     * Updates one column only if the record version matches. The caller must
     * handle an update count of zero as an optimistic concurrency conflict.
     * version must be a numeric column.
     */
    public int updateVersioned(String table, String idColumn, String valueColumn,
            Object id, Object value, int expectedVersion) {
        if (expectedVersion < 0)
            throw new IllegalArgumentException("Expected version must not be negative");
        if (id == null || value == null)
            throw new IllegalArgumentException("Nullable repository values are not yet supported");
        return jdbc.execute("UPDATE " + identifier(table)
            + " SET " + identifier(valueColumn) + " = ?, version = version + 1"
            + " WHERE " + identifier(idColumn) + " = ? AND version = ?",
            List.of(value, id, expectedVersion));
    }
}
