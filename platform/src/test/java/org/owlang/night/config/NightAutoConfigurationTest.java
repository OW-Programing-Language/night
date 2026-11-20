package org.owlang.night.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NightAutoConfigurationTest {
    @TempDir Path folder;

    private Path manifest(String night) throws Exception {
        Path file = folder.resolve("dependencies.yml");
        Files.writeString(file, "format: 1\nrepositories: []\ndependencies: []\n" + night);
        return file;
    }

    @Test void selectsAndOrdersStartersExplicitly() throws Exception {
        var plan = NightAutoConfiguration.read(manifest("""
                night:
                  starters: [security, cache, data, actuator]
                  migrations: "filesystem:db/migration"
                """));
        assertEquals(List.of("cache", "actuator", "data", "security"), plan.ordered());
        assertEquals("filesystem:db/migration", plan.migrations());
        assertTrue(plan.has("data"));
        assertFalse(plan.has("redis"));
    }

    @Test void detectsDependencyAliasesAndHonorsDisable() throws Exception {
        var file = manifest("""
                night:
                  disable: [redis]
                  starters: [validation]
                """);
        Files.writeString(file, Files.readString(file).replace("dependencies: []", """
                dependencies:
                  - name: night-security
                    repository: github
                    path: org/example
                    version: main
                  - name: night-redis
                    repository: github
                    path: org/example
                    version: main
                """));
        var plan = NightAutoConfiguration.read(file);
        assertTrue(plan.has("security"));
        assertTrue(plan.has("validation"));
        assertFalse(plan.has("redis"));
    }

    @Test void rejectsConflictingSettingsAndInvalidStarters() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                NightAutoConfiguration.read(manifest("""
                night:
                  starters: [redis]
                  disable: [redis]
                """)));
        assertThrows(IllegalArgumentException.class, () ->
                NightAutoConfiguration.read(manifest("night:\n  starters: [reids]\n")));
        assertThrows(IllegalArgumentException.class, () ->
                NightAutoConfiguration.read(manifest("night:\n  starters: [data, data]\n")));
    }

    @Test void rejectsMigrationsWithoutDataAndUnsafeLocation() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                NightAutoConfiguration.read(manifest("night:\n  migrations: 'filesystem:db/migration'\n")));
        assertThrows(IllegalArgumentException.class, () ->
                NightAutoConfiguration.read(manifest("""
                night:
                  starters: [data]
                  migrations: "filesystem:../secrets"
                """)));
    }

    @Test void rejectsDuplicateYamlKeysAndUnexpectedNightKeys() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                NightAutoConfiguration.read(manifest("night:\n  starters: [data]\n  starters: [redis]\n")));
        assertThrows(IllegalArgumentException.class, () ->
                NightAutoConfiguration.read(manifest("night:\n  secret: value\n")));
    }

    @Test void emptyManifestDoesNotEnableSensitiveStarters() throws Exception {
        var plan = NightAutoConfiguration.read(manifest(""));
        assertTrue(plan.starters().isEmpty());
        assertFalse(plan.has("security"));
        assertFalse(plan.has("data"));
    }
}
