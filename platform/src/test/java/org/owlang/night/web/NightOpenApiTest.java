package org.owlang.night.web;

import dev.oreo.Interpreter;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class NightOpenApiTest {
    @Test void generatesDocumentWithPathParametersAndAuthentication() {
        NightOpenApi api = new NightOpenApi();
        assertEquals(1, api.register("GET", "/api/users/:id", "Fetch a user", 1));
        assertEquals(1, api.register("POST", "/api/users", "Create a user", 1));
        assertEquals(1, api.register("GET", "/_night/health", "Health check", 0));
        Interpreter.OwRecord doc = api.spec("Night Tests", "1.0");
        assertEquals("3.0.3", doc.get("openapi"));
        Map<?, ?> paths = (Map<?, ?>) doc.get("paths");
        assertTrue(paths.containsKey("/api/users/{id}"));
        var operation = (Map<?, ?>) ((Map<?, ?>) paths.get("/api/users/{id}")).get("get");
        assertEquals("Fetch a user", operation.get("summary"));
        assertEquals(1, ((List<?>) operation.get("parameters")).size());
        assertEquals("id", ((Map<?, ?>) ((List<?>) operation.get("parameters")).get(0)).get("name"));
        assertEquals(1, ((List<?>) operation.get("security")).size());
        var health = (Map<?, ?>) ((Map<?, ?>) paths.get("/_night/health")).get("get");
        assertTrue(((List<?>) health.get("security")).isEmpty());
        assertTrue(((Map<?, ?>) doc.get("components")).containsKey("securitySchemes"));
    }

    @Test void rejectsInvalidOrDuplicateRouteMetadata() {
        NightOpenApi api = new NightOpenApi();
        assertThrows(IllegalArgumentException.class,
                () -> api.register("TRACE", "/public", "tracing", 1));
        assertThrows(IllegalArgumentException.class,
                () -> api.register("GET", "/foo?admin=true", "unsafe", 1));
        assertThrows(IllegalArgumentException.class,
                () -> api.register("GET", "/users/:id", "valid", 2));
        assertThrows(IllegalArgumentException.class,
                () -> api.register("GET", "/users/:id", " ", 1));
        assertEquals(1, api.register("GET", "/users/:id", "Fetch user", 1));
        assertThrows(IllegalArgumentException.class,
                () -> api.register("GET", "/users/:id", "Duplicate", 1));
    }
}
