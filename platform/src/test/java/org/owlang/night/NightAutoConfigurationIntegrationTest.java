package org.owlang.night;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NightAutoConfigurationIntegrationTest {
    @TempDir Path folder;

    private Path manifest(String spec) throws Exception {
        Path file = folder.resolve("dependencies.yml");
        Files.writeString(file, "format: 1\nrepositories: []\ndependencies: []\n" + spec);
        return file;
    }

    @Test void passiveStartersApplyOnceAndCanBeInspected() throws Exception {
        var file = manifest("night:\n  starters: [core, cache, actuator]\n");
        try (NightExtension ext = new NightExtension()) {
            assertEquals(1, ext.invoke("night.autoconfig.apply", List.of(file.toString())));
            assertEquals(1, ext.invoke("night.autoconfig.enabled", List.of("cache")));
            assertEquals(0, ext.invoke("night.autoconfig.enabled", List.of("security")));
            assertEquals(List.of("core", "cache", "actuator"),
                    ext.invoke("night.autoconfig.starters", List.of()));
            assertEquals(1, ext.invoke("night.autoconfig.apply", List.of(file.toString())));
            Files.writeString(file, "format: 1\nnight:\n  starters: [core]\n");
            assertThrows(IllegalStateException.class,
                    () -> ext.invoke("night.autoconfig.apply", List.of(file.toString())));
        }
    }

    @Test void noImplicitAuthenticationAndManifestMustExist() throws Exception {
        try (NightExtension ext = new NightExtension()) {
            assertThrows(IllegalStateException.class,
                    () -> ext.invoke("night.autoconfig.enabled", List.of("security")));
            assertThrows(IllegalArgumentException.class,
                    () -> ext.invoke("night.autoconfig.apply", List.of(folder.resolve("missing/dependencies.yml").toString())));
        }
    }
}
