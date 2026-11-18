package org.owlang.night.data;

import dev.oreo.Interpreter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XAutoClaimParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

/**
 * Minimal Redis Streams adapter, with manual ACK and pending-entry recovery.
 *
 * Events are at-least-once: callers must perform idempotent business effects
 * and ACK only after they succeed. Stream persistence requires Redis operator
 * configuration (AOF/replication/backup); this API does not promise exactly once.
 * Redis I/O is synchronous and must not run on an OW HTTP request handler.
 */
public final class NightRedisStreams {
    private static final int MAX_BATCH = 32;
    private static final int MAX_PAYLOAD_BYTES = 262_144;
    private final NightRedis redis;

    public NightRedisStreams(NightRedis redis) {
        this.redis = Objects.requireNonNull(redis);
    }

    static String name(String input) {
        if (input == null || !input.matches("[A-Za-z][A-Za-z0-9_-]{0,63}"))
            throw new IllegalArgumentException("Invalid Night Redis Streams name");
        return input;
    }

    static StreamEntryID entryId(String input) {
        if (input == null || !input.matches("(0|[1-9][0-9]{0,18})-(0|[1-9][0-9]{0,18})"))
            throw new IllegalArgumentException("Invalid Redis stream entry ID");
        try {
            return new StreamEntryID(input);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid Redis stream entry ID", ex);
        }
    }

    private static int batchSize(int count) {
        if (count < 1 || count > MAX_BATCH)
            throw new IllegalArgumentException("Redis Streams batch size must be between 1 and 32");
        return count;
    }

    private String streamKey(String stream) {
        return redis.streamKey(name(stream));
    }

    /** Idempotent group creation from stream start: existing groups retain their cursor. */
    public int createGroup(String stream, String group) {
        String key = streamKey(stream);
        name(group);
        try {
            redis.streamClient().xgroupCreate(key, group, new StreamEntryID(0, 0), true);
            return 1;
        } catch (JedisDataException ex) {
            if (ex.getMessage() != null && ex.getMessage().startsWith("BUSYGROUP "))
                return 1;
            throw ex;
        }
    }

    /** Adds a bounded event without trimming unacknowledged entries. */
    public String publish(String stream, String messageId, String jsonBody) {
        String key = streamKey(stream);
        if (messageId == null || !messageId.matches("[A-Za-z0-9_-]{8,80}")
                || jsonBody == null
                || jsonBody.getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES)
            throw new IllegalArgumentException("Invalid Redis Streams event");
        StreamEntryID id = redis.streamClient().xadd(key, XAddParams.xAddParams(),
                Map.of("message_id", messageId, "body", jsonBody));
        return id.toString();
    }

    /**
     * Non-blocking new-entry consumer-group poll. Returned entries remain
     * pending until acknowledged, including if the worker process crashes.
     */
    public Interpreter.OwRecord poll(String stream, String group, String consumer, int count) {
        String key = streamKey(stream);
        name(group);
        name(consumer);
        List<Map.Entry<String, List<StreamEntry>>> batch = redis.streamClient().xreadGroup(
                group, consumer, XReadGroupParams.xReadGroupParams().count(batchSize(count)),
                Map.of(key, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY));
        List<StreamEntry> entries = batch == null || batch.isEmpty()
                ? List.of() : batch.get(0).getValue();
        return result(entries);
    }

    /**
     * Reassigns pending entries that have been idle for minIdleMillis.
     * Operators must supervise worker leases, enforce minimum idle thresholds,
     * and handle idempotency; this is not fenced exactly-once execution.
     *
     * Return next_cursor for a subsequent pass; "0-0" means the scan completed.
     */
    public Interpreter.OwRecord reclaim(String stream, String group, String consumer,
                                        long minIdleMillis, String cursor, int count) {
        String key = streamKey(stream);
        name(group);
        name(consumer);
        if (minIdleMillis < 1_000 || minIdleMillis > 86_400_000)
            throw new IllegalArgumentException("Invalid Redis Streams minimum idle time");
        StreamEntryID start = entryId(cursor);
        Map.Entry<StreamEntryID, List<StreamEntry>> batch = redis.streamClient().xautoclaim(
                key, group, consumer, minIdleMillis, start,
                XAutoClaimParams.xAutoClaimParams().count(batchSize(count)));
        Interpreter.OwRecord out = result(batch == null ? List.of() : batch.getValue());
        out.put("next_cursor", batch == null ? "0-0" : batch.getKey().toString());
        return out;
    }

    /** Returns 1 when an event was pending and acknowledged; 0 otherwise. */
    public int acknowledge(String stream, String group, String id) {
        String key = streamKey(stream);
        name(group);
        return (int) redis.streamClient().xack(key, group, entryId(id));
    }

    private static Interpreter.OwRecord result(List<StreamEntry> entries) {
        List<Interpreter.OwRecord> events = new ArrayList<>(entries.size());
        for (StreamEntry entry : entries) {
            Map<String, String> fields = entry.getFields();
            String id = fields.get("message_id");
            String body = fields.get("body");
            if (id == null || !id.matches("[A-Za-z0-9_-]{8,80}")
                    || body == null
                    || body.getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES)
                throw new IllegalStateException("Invalid Redis Streams event from broker");
            Interpreter.OwRecord event = new Interpreter.OwRecord();
            event.put("stream_id", entry.getID().toString());
            event.put("message_id", id);
            event.put("body", body);
            events.add(event);
        }
        Interpreter.OwRecord out = new Interpreter.OwRecord();
        out.put("present", events.isEmpty() ? 0 : 1);
        out.put("events", events);
        return out;
    }
}
