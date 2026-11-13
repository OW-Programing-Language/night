package org.owlang.night;

import com.sun.net.httpserver.HttpExchange;
import dev.oreo.Interpreter;
import dev.oreo.RuntimeExtension;
import org.owlang.night.actuator.NightMetrics;
import org.owlang.night.cache.NightCache;
import org.owlang.night.data.NightJdbc;
import org.owlang.night.data.NightRepository;
import org.owlang.night.data.NightRedis;
import org.owlang.night.http.NightHttpClient;
import org.owlang.night.data.NightOutbox;
import org.owlang.night.security.BearerSecurity;
import org.owlang.night.security.NightCredentialStore;
import org.owlang.night.web.NightOpenApi;
import org.owlang.night.mail.NightMail;
import org.owlang.night.validation.NightObjectValidation;
import org.owlang.night.jobs.NightScheduler;
import org.owlang.night.messaging.NightRabbitMQ;
import org.owlang.night.security.NightIpQuota;

import java.util.List;

/**
 * Night core starter: security middleware, external configuration, JDBC,
 * Flyway, local caching and metrics. Loaded by OW ServiceLoader.
 */
public final class NightExtension implements RuntimeExtension {
    private final BearerSecurity security = new BearerSecurity();
    private final NightIpQuota quota = NightIpQuota.fromEnvironment();
    private final NightJdbc jdbc = new NightJdbc();
    private final NightCredentialStore credentials = new NightCredentialStore(jdbc);
    private final NightOpenApi openapi = new NightOpenApi();
    private final NightMail mail = new NightMail(jdbc);
    private final NightObjectValidation objectValidation = new NightObjectValidation();
    private final NightScheduler scheduler = new NightScheduler(jdbc);
    private final NightRabbitMQ rabbitmq = new NightRabbitMQ();
    private final NightRepository repository = new NightRepository(jdbc);
    private final NightOutbox outbox = new NightOutbox(jdbc);
    private final NightMetrics metrics = new NightMetrics();
    private final NightCache cache = new NightCache();
    private final NightRedis redis = new NightRedis();
    private volatile NightHttpClient outboundHttp;

    private NightHttpClient outboundHttp() {
        NightHttpClient ready = outboundHttp;
        if (ready != null) return ready;
        synchronized (this) {
            if (outboundHttp == null) outboundHttp = NightHttpClient.fromEnvironment();
            return outboundHttp;
        }
    }

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
            case "night.rabbitmq.open" -> {
                arity(operation, args, 0);
                yield rabbitmq.open();
            }
            case "night.rabbitmq.publish" -> {
                arity(operation, args, 2);
                yield rabbitmq.publish(string(args, 0), string(args, 1));
            }
            case "night.rabbitmq.poll" -> {
                arity(operation, args, 0);
                yield rabbitmq.poll();
            }
            case "night.rabbitmq.ack" -> {
                arity(operation, args, 1);
                yield rabbitmq.ack(longValue(args, 0));
            }
            case "night.rabbitmq.reject" -> {
                arity(operation, args, 2);
                yield rabbitmq.reject(longValue(args, 0), integer(args, 1) == 1);
            }
            case "night.jobs.schedule" -> {
                arity(operation, args, 5);
                yield scheduler.schedule(string(args, 0), string(args, 1), string(args, 2),
                        longValue(args, 3), integer(args, 4));
            }
            case "night.jobs.claim" -> {
                arity(operation, args, 2);
                yield scheduler.claim(string(args, 0), integer(args, 1));
            }
            case "night.jobs.ack" -> {
                arity(operation, args, 2);
                yield scheduler.acknowledge(string(args, 0), string(args, 1));
            }
            case "night.jobs.fail" -> {
                arity(operation, args, 4);
                yield scheduler.fail(string(args, 0), string(args, 1), integer(args, 2), integer(args, 3));
            }
            case "night.jobs.replay_dead" -> {
                arity(operation, args, 1);
                yield scheduler.replayDead(string(args, 0));
            }
            case "night.mail.send" -> {
                arity(operation, args, 6);
                yield mail.send(string(args, 0), string(args, 1), string(args, 2), string(args, 3),
                        list(args, 4), list(args, 5));
            }
            case "night.openapi.register" -> {
                arity(operation, args, 4);
                yield openapi.register(string(args, 0), string(args, 1), string(args, 2), integer(args, 3));
            }
            case "night.openapi.spec" -> {
                arity(operation, args, 2);
                yield openapi.spec(string(args, 0), string(args, 1));
            }
            case "night.credentials.register" -> {
                arity(operation, args, 2);
                yield credentials.register(string(args, 0), string(args, 1));
            }
            case "night.credentials.verify" -> {
                arity(operation, args, 2);
                yield credentials.verify(string(args, 0), string(args, 1));
            }
            case "night.credentials.issue_reset" -> {
                arity(operation, args, 1);
                yield credentials.issueReset(string(args, 0));
            }
            case "night.credentials.complete_reset" -> {
                arity(operation, args, 3);
                yield credentials.completeReset(string(args, 0), string(args, 1), string(args, 2));
            }
            case "night.security.role" -> {
                arity(operation, args, 2);
                security.requireRole(string(args, 0), string(args, 1));
                yield 1;
            }
            case "night.http.get" -> {
                arity(operation, args, 1);
                yield outboundHttp().get(string(args, 0));
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
            case "night.data.page" -> {
                arity(operation, args, 4);
                yield repository.page(string(args, 0), string(args, 1), integer(args, 2), integer(args, 3));
            }
            case "night.data.update_versioned" -> {
                arity(operation, args, 6);
                yield repository.updateVersioned(string(args, 0), string(args, 1),
                        string(args, 2), args.get(3), args.get(4), integer(args, 5));
            }
            case "night.outbox.enqueue" -> {
                arity(operation, args, 3);
                yield outbox.enqueue(string(args, 0), string(args, 1), string(args, 2));
            }
            case "night.outbox.claim" -> {
                arity(operation, args, 2);
                yield outbox.claim(string(args, 0), integer(args, 1));
            }
            case "night.outbox.ack" -> {
                arity(operation, args, 2);
                yield outbox.acknowledge(string(args, 0), string(args, 1));
            }
            case "night.outbox.replay_dead" -> {
                arity(operation, args, 1);
                yield outbox.replayDead(string(args, 0));
            }
            case "night.outbox.fail" -> {
                arity(operation, args, 4);
                yield outbox.fail(string(args, 0), string(args, 1), integer(args, 2), integer(args, 3));
            }
            case "night.data.begin" -> {
                arity(operation, args, 0);
                yield jdbc.begin();
            }
            case "night.data.begin_isolation" -> {
                arity(operation, args, 1);
                yield jdbc.begin(string(args, 0));
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
            case "night.actuator.prometheus" -> {
                arity(operation, args, 0);
                yield metrics.prometheus();
            }
            case "night.actuator.metrics" -> {
                arity(operation, args, 0);
                yield metrics.snapshot();
            }
            case "night.redis.open" -> {
                arity(operation, args, 0);
                yield redis.open();
            }
            case "night.redis.put" -> {
                arity(operation, args, 3);
                yield redis.put(string(args, 0), string(args, 1), integer(args, 2));
            }
            case "night.redis.get" -> {
                arity(operation, args, 1);
                yield redis.get(string(args, 0));
            }
            case "night.redis.evict" -> {
                arity(operation, args, 1);
                yield redis.evict(string(args, 0));
            }
            case "night.redis.health" -> {
                arity(operation, args, 0);
                yield redis.health();
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
            case "night.validation.object" -> {
                arity(operation, args, 2);
                yield objectValidation.validate(args.get(0), list(args, 1));
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

    private static long longValue(List<?> args, int index) {
        if (!(args.get(index) instanceof Number number))
            throw new IllegalArgumentException("Expected a Night numeric argument");
        return number.longValue();
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
        // Throttle authentication failures as well as accepted requests.
        if (quota != null) {
            Response throttled = quota.before(exchange);
            if (throttled != null) return throttled;
        }
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

    @Override public void close() {
        try { rabbitmq.close(); }
        finally {
            try { redis.close(); }
            finally { jdbc.close(); }
        }
    }
}
