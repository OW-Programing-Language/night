package org.owlang.night.cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NightCacheTest {
    @Test void storesAndEvictsEntries() {
        NightCache cache = new NightCache();
        assertEquals(1, cache.put("one", "value", 10));
        assertEquals("value", cache.get("one"));
        assertEquals(1, cache.evict("one"));
        assertNull(cache.get("one"));
    }

    @Test void enforcesTtlAndSizeBounds() throws Exception {
        NightCache cache = new NightCache();
        cache.put("soon", 12, 1);
        Thread.sleep(1100);
        assertNull(cache.get("soon"));
        assertThrows(IllegalArgumentException.class, () -> cache.put("bad", 1, 0));
        assertThrows(IllegalArgumentException.class, () -> cache.put("bad", 1, 86401));
    }
}
