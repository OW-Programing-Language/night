package org.owlang.night.data;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.oreo.Interpreter;
import org.flywaydb.core.Flyway;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * SQL module with a bounded HikariCP pool, parameter-bound JDBC calls,
 * Flyway migrations and explicit request-local transactions.
 *
 * Never interpolate untrusted strings into SQL; only bind placeholders.
 * A transaction belongs to the current request thread.
 */
public final class NightJdbc implements AutoCloseable {
    private HikariDataSource pool;
    private final ThreadLocal<Connection> transaction = new ThreadLocal<>();

    public synchronized int open() {
        if (pool != null) return 1;
        String url = require("NIGHT_JDBC_URL");
        if (!url.startsWith("jdbc:postgresql:") && !url.startsWith("jdbc:h2:"))
            throw new IllegalStateException("Night supports PostgreSQL or H2 JDBC URLs");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(System.getenv().getOrDefault("NIGHT_JDBC_USER", ""));
        config.setPassword(System.getenv().getOrDefault("NIGHT_JDBC_PASSWORD", ""));
        config.setPoolName("NightJdbc");
        config.setMaximumPoolSize(poolSize());
        config.setMinimumIdle(0);
        config.setConnectionTimeout(10_000);
        config.setValidationTimeout(3_000);
        config.setInitializationFailTimeout(10_000);
        pool = new HikariDataSource(config);
        return 1;
    }

    private static String require(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank())
            throw new IllegalStateException("Night requires environment variable " + name);
        return value;
    }

    private static int poolSize() {
        String setting = System.getenv().getOrDefault("NIGHT_DB_POOL_SIZE", "10");
        int size = Integer.parseInt(setting);
        if (size < 1 || size > 64)
            throw new IllegalStateException("NIGHT_DB_POOL_SIZE must be 1..64");
        return size;
    }

    private HikariDataSource datasource() {
        if (pool == null) throw new IllegalStateException("Night JDBC is not open");
        return pool;
    }

    public synchronized int migrate(String location) {
        datasource();
        if (!location.startsWith("filesystem:") && !location.startsWith("classpath:"))
            throw new IllegalArgumentException("Night migrations require a filesystem: or classpath: location");
        int migrations = Flyway.configure()
                .dataSource(pool)
                .locations(location)
                .baselineOnMigrate(false)
                .cleanDisabled(true)
                .validateOnMigrate(true)
                .load()
                .migrate()
                .migrationsExecuted;
        return migrations;
    }

    public List<Interpreter.OwRecord> query(String sql, List<?> params) {
        Connection connection = connection();
        boolean owned = transaction.get() == null;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, params);
            statement.setQueryTimeout(15);
            try (ResultSet rs = statement.executeQuery()) {
                ResultSetMetaData columns = rs.getMetaData();
                List<Interpreter.OwRecord> results = new ArrayList<>();
                while (rs.next()) {
                    Interpreter.OwRecord record = new Interpreter.OwRecord();
                    for (int column = 1; column <= columns.getColumnCount(); column++) {
                        Object value = rs.getObject(column);
                        record.put(columns.getColumnLabel(column), valueOf(value));
                    }
                    results.add(record);
                    if (results.size() > 10_000)
                        throw new IllegalStateException("Night JDBC query exceeds the 10000-row limit");
                }
                return results;
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Night JDBC query failed", ex);
        } finally {
            if (owned) closeConnection(connection);
        }
    }

    public int execute(String sql, List<?> params) {
        Connection connection = connection();
        boolean owned = transaction.get() == null;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, params);
            statement.setQueryTimeout(15);
            return statement.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException("Night JDBC update failed", ex);
        } finally {
            if (owned) closeConnection(connection);
        }
    }

    public int begin(String isolation) {
        final int level = switch (isolation) {
            case "READ_COMMITTED" -> Connection.TRANSACTION_READ_COMMITTED;
            case "REPEATABLE_READ" -> Connection.TRANSACTION_REPEATABLE_READ;
            case "SERIALIZABLE" -> Connection.TRANSACTION_SERIALIZABLE;
            default -> throw new IllegalArgumentException("Unsupported transaction isolation level");
        };
        if (transaction.get() != null)
            throw new IllegalStateException("Nested Night JDBC transactions are not supported");
        Connection connection = connection();
        try {
            connection.setTransactionIsolation(level);
            connection.setAutoCommit(false);
            transaction.set(connection);
            return 1;
        } catch (SQLException | RuntimeException ex) {
            closeConnection(connection);
            throw new IllegalStateException("Night JDBC begin failed", ex);
        }
    }

    public int begin() {
        if (transaction.get() != null)
            throw new IllegalStateException("Nested Night JDBC transactions are not supported");
        Connection connection = connection();
        try {
            connection.setAutoCommit(false);
            transaction.set(connection);
            return 1;
        } catch (SQLException ex) {
            closeConnection(connection);
            throw new IllegalStateException("Night JDBC begin failed", ex);
        }
    }

    public int commit() {
        Connection connection = transaction.get();
        if (connection == null) throw new IllegalStateException("No active Night transaction");
        try {
            connection.commit();
            return 1;
        } catch (SQLException ex) {
            throw new IllegalStateException("Night JDBC commit failed", ex);
        } finally {
            transaction.remove();
            closeConnection(connection);
        }
    }

    public int rollback() {
        Connection connection = transaction.get();
        if (connection == null) throw new IllegalStateException("No active Night transaction");
        try {
            connection.rollback();
            return 1;
        } catch (SQLException ex) {
            throw new IllegalStateException("Night JDBC rollback failed", ex);
        } finally {
            transaction.remove();
            closeConnection(connection);
        }
    }

    /** Ensure that a failed or unfinished route never leaks a transaction to the next request. */
    public void cleanupRequest() {
        Connection active = transaction.get();
        if (active == null) return;
        try {
            active.rollback();
        } catch (SQLException ignored) {
            // Connection is returned to Hikari; failed transactions are not reused.
        } finally {
            transaction.remove();
            closeConnection(active);
        }
    }

    public int health() {
        try {
            query("SELECT 1 AS result", List.of());
            return 1;
        } catch (RuntimeException ex) {
            return 0;
        }
    }

    private Connection connection() {
        Connection borrowed = transaction.get();
        if (borrowed != null) return borrowed;
        try {
            return datasource().getConnection();
        } catch (SQLException ex) {
            throw new IllegalStateException("Night cannot acquire a JDBC connection", ex);
        }
    }

    private static void bind(PreparedStatement statement, List<?> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Object value = params.get(i);
            if (value != null && !(value instanceof String || value instanceof Number
                    || value instanceof Boolean || value instanceof byte[]))
                throw new IllegalArgumentException("Unsupported JDBC parameter type");
            statement.setObject(i + 1, value);
        }
    }

    private static Object valueOf(Object value) {
        if (value == null) return null;
        if (value instanceof Long number && number >= Integer.MIN_VALUE
                && number <= Integer.MAX_VALUE) return number.intValue();
        if (value instanceof BigDecimal decimal) return decimal.doubleValue();
        if (value instanceof TemporalAccessor) return value.toString();
        if (value instanceof java.sql.Date || value instanceof java.sql.Timestamp)
            return value.toString();
        if (value instanceof Number || value instanceof String || value instanceof Boolean)
            return value;
        return value.toString();
    }

    private static void closeConnection(Connection connection) {
        try { connection.close(); }
        catch (SQLException ex) {
            throw new IllegalStateException("Could not return Night JDBC connection", ex);
        }
    }

    @Override public synchronized void close() {
        Connection active = transaction.get();
        if (active != null) {
            try { active.rollback(); } catch (SQLException ignored) {}
            transaction.remove();
            closeConnection(active);
        }
        if (pool != null) pool.close();
        pool = null;
    }
}
