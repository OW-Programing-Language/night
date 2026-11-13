package org.owlang.night;

import com.sun.net.httpserver.HttpExchange;
import dev.oreo.Interpreter;
import dev.oreo.RuntimeExtension;
import org.owlang.night.actuator.NightMetrics;
import org.owlang.night.cache.NightCache;
import org.owlang.night.data.NightJdbc;
import org.owlang.night.security.BearerSecurity;

import java.util.List;

/**
 * Night core starter: security middleware, external configuration, JDBC,
 * Flyway, local caching and metrics. Loaded by OW ServiceLoader.
 */
public final class NightExtension implements RuntimeExtension {
    private final BearerSecurity security = new BearerSecurity();
    private final NightJdbc jdbc = new NightJdbc();
    private final NightMetrics metrics = new NightMetrics();
    private final NightCache cache = new NightCache();

    @Override public boolean supports(String operation) {
        return operation.startsWith("night.");
    }

    @Override public Object invoke(String operation, List<Object> args) {
        return switch (operation) {
            case "night.security.enable" -> {
                arity(operation, args, 0);
                security.enable();
                yield 1;
            }
            case "night.security.public" -> {
                arity(operation, args, 1);
                security.publicRoute(string(args, 0));
                yield 1;
            }
            case "night.security.role" -> {
                arity(operation, args, 2);
                security.requireRole(string(args, 0), string(args, 1));
                yield 1;
            }
            case "night.config.get" -> {
                arity(operation, args, 1);
                yield System.getenv().getOrDefault(string(args, 0), "");
            }
            case "night.config.require" -> {
                arity(operation, args, 1);
                String name = string(args, 0);
                String result = System.getenv(name);
                if (result == null || result.isBlank())
                    throw new IllegalStateException("Missing Night configuration: " + name);
                yield result;
            }
            case "night.data.open" -> {
                arity(operation, args, 0);
                yield jdbc.open();
            }
            case "night.data.migrate" -> {
                arity(operation, args, 1);
                yield jdbc.migrate(string(args, 0));
            }
            case "night.data.query" -> {
                arity(operation, args, 2);
                yield jdbc.query(string(args, 0), list(args, 1));
            }
            case "night.data.execute" -> {
                arity(operation, args, 2);
                yield jdbc.execute(string(args, 0), list(args, 1));
            }
            case "night.data.begin" -> {
                arity(operation, args, 0);
                yield jdbc.begin();
            }
            case "night.data.commit" -> {
                arity(operation, args, 0);
                yield jdbc.commit();
            }
            case "night.data.rollback" -> {
                arity(operation, args, 0);
                yield jdbc.rollback();
            }
            case "night.data.health" -> {
                arity(operation, args, 0);
                yield jdbc.health();
            }
            case "night.actuator.metrics" -> {
                arity(operation, args, 0);
                yield metrics.snapshot();
            }
            case "night.cache.put" -> {
                arity(operation, args, 3);
                yield cache.put(string(args, 0), args.get(1), integer(args, 2));
            }
            case "night.cache.get" -> {
                arity(operation, args, 1);
                yield cache.get(string(args, 0));
            }
            case "night.cache.evict" -> {
                arity(operation, args, 1);
                yield cache.evict(string(args, 0));
            }
            case "night.cache.clear" -> {
                arity(operation, args, 0);
                yield cache.clear();
            }
            case "night.validation.required" -> {
                arity(operation, args, 1);
                Object value = args.get(0);
                yield value == null || (value instanceof String s && s.isBlank()) ? 0 : 1;
            }
            case "night.validation.positive_int" -> {
                arity(operation, args, 1);
                String raw = string(args, 0);
                yield raw.matches("[1-9][0-9]{0,8}") ? 1 : 0;
            }
            case "night.validation.unsigned_int" -> {
                arity(operation, args, 1);
                String raw = string(args, 0);
                yield raw.matches("(0|[1-9][0-9]{0,8})") ? 1 : 0;
            }
            case "night.validation.max_length" -> {
                arity(operation, args, 2);
                Object value = args.get(0);
                yield value instanceof String s && s.length() <= integer(args, 1) ? 1 : 0;
            }
            default -> throw new IllegalArgumentException(
                    "Unknown Night platform operation " + operation);
        };
    }

    private static void arity(String name, List<?> arguments, int expected) {
        if (arguments.size() != expected)
            throw new IllegalArgumentException(name + " expects " + expected + " arguments");
    }

    private static String string(List<?> args, int index) {
        if (!(args.get(index) instanceof String text))
            throw new IllegalArgumentException("Expected a Night string argument");
        return text;
    }

    private static int integer(List<?> args, int index) {
        if (!(args.get(index) instanceof Number number))
            throw new IllegalArgumentException("Expected a Night numeric argument");
        return number.intValue();
    }

    private static List<?> list(List<?> args, int index) {
        if (!(args.get(index) instanceof List<?> elements))
            throw new IllegalArgumentException("Expected a Night parameter array");
        return elements;
    }

    @Override public Response beforeRequest(HttpExchange exchange) {
        return security.before(exchange);
    }

    @Override public void afterRequest(HttpExchange exchange, int status, long elapsedNanos) {
        try {
            metrics.observe(status, elapsedNanos);
        } finally {
            // Uncommitted DB work may never survive into the next request.
            jdbc.cleanupRequest();
        }
    }

    @Override public void close() { jdbc.close(); }
}
