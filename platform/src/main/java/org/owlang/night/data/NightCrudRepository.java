package org.owlang.night.data;

import dev.oreo.Interpreter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * OW @Repository storage adapter. Only trusted, prevalidated literal schema
 * names can become identifiers. Every data value is a JDBC bind parameter.
 *
 * All operations delegate to the same NightJdbc instance, so operations
 * inside an OW @Transactional method share its request-local connection.
 */
public final class NightCrudRepository {
    private final NightJdbc jdbc;

    public NightCrudRepository(NightJdbc jdbc) {
        this.jdbc = jdbc;
    }

    private static String identifier(String name) {
        if (name == null || !name.matches("[A-Za-z_][A-Za-z_0-9]{0,62}"))
            throw new IllegalArgumentException("Unsafe OW repository SQL identifier");
        return name;
    }

    private static Object value(Object value) {
        if (value == null || !(value instanceof String || value instanceof Number
                || value instanceof Boolean || value instanceof byte[]))
            throw new IllegalArgumentException("OW repository values must be non-null JDBC scalars");
        return value;
    }

    /** INSERT only: a duplicate primary key is an error, never a silent update. */
    public int save(String table, String idColumn, Object source) {
        String safeTable = identifier(table);
        String safeId = identifier(idColumn);
        if (!(source instanceof Interpreter.OwRecord record))
            throw new IllegalArgumentException("Repository.save expects an OW record");
        if (record.isEmpty() || record.size() > 32 || !record.containsKey(safeId))
            throw new IllegalArgumentException("Repository.save expects 1..32 columns including " + safeId);
        Map<String, Object> values = new TreeMap<>();
        for (Map.Entry<String, Object> field : record.entrySet()) {
            String column = identifier(field.getKey());
            values.put(column, value(field.getValue()));
        }
        String columns = String.join(", ", values.keySet());
        String placeholders = String.join(", ", java.util.Collections.nCopies(values.size(), "?"));
        return jdbc.execute("INSERT INTO " + safeTable + " (" + columns + ") VALUES (" +
                placeholders + ")", new ArrayList<>(values.values()));
    }

    /** Returns an OW record or the numeric sentinel 0 if the row is absent. */
    public Object findById(String table, String idColumn, Object id) {
        List<Interpreter.OwRecord> found = jdbc.query(
                "SELECT * FROM " + identifier(table) + " WHERE " +
                        identifier(idColumn) + " = ?",
                List.of(value(id)));
        return found.isEmpty() ? 0 : found.get(0);
    }

    public int existsById(String table, String idColumn, Object id) {
        List<Interpreter.OwRecord> found = jdbc.query(
                "SELECT 1 AS found FROM " + identifier(table) + " WHERE " +
                        identifier(idColumn) + " = ?",
                List.of(value(id)));
        return found.isEmpty() ? 0 : 1;
    }

    public int deleteById(String table, String idColumn, Object id) {
        return jdbc.execute("DELETE FROM " + identifier(table) + " WHERE " +
                identifier(idColumn) + " = ?", List.of(value(id)));
    }

    /** Deliberately bounded: callers must page to retrieve more than 500. */
    public List<Interpreter.OwRecord> findAll(String table, String idColumn) {
        return page(table, idColumn, idColumn, 500, 0);
    }

    /** The leading minus sign requests DESC; all identifier text is validated. */
    public List<Interpreter.OwRecord> page(
            String table, String idColumn, String order, int limit, int offset) {
        if (limit < 1 || limit > 500 || offset < 0 || offset > 10_000_000)
            throw new IllegalArgumentException("Repository pagination limit must be 1..500; offset 0..10000000");
        String safeTable = identifier(table);
        String safeId = identifier(idColumn);
        if (order == null || order.isBlank())
            throw new IllegalArgumentException("Repository sort column is required");
        boolean descending = order.startsWith("-");
        String safeSort = identifier(descending ? order.substring(1) : order);
        String direction = descending ? " DESC" : " ASC";
        String tieBreak = safeSort.equals(safeId) ? "" : ", " + safeId + " ASC";
        return jdbc.query("SELECT * FROM " + safeTable + " ORDER BY " +
                safeSort + direction + tieBreak + " LIMIT ? OFFSET ?", List.of(limit, offset));
    }
}
