package org.apache.paimon.agent.service;

import org.apache.paimon.agent.config.AgentConfiguration;
import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.config.SourceConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgentProcessLockTest {

    @TempDir Path tempDir;

    @Test
    void locksTheWriterIdentityInsteadOfTheDataDirectory() throws Exception {
        AgentConfiguration writer = configuration("writer-a", "sessions", "messages");
        Path firstConfig = tempDir.resolve("first/project.properties");
        Path copiedConfig = tempDir.resolve("second/project.properties");

        try (AgentProcessLock ignored =
                AgentProcessLock.acquire(tempDir.resolve("locks"), writer, firstConfig)) {
            assertThatThrownBy(
                            () ->
                                    AgentProcessLock.acquire(
                                            tempDir.resolve("locks"), writer, copiedConfig))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("table pair");

            assertThatThrownBy(
                            () ->
                                    AgentProcessLock.acquire(
                                            tempDir.resolve("locks"),
                                            configuration("writer-b", "sessions", "messages"),
                                            copiedConfig))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("table pair");

            assertThatCode(
                            () -> {
                                try (AgentProcessLock different =
                                        AgentProcessLock.acquire(
                                                tempDir.resolve("locks"),
                                                configuration(
                                                        "writer-b", "other-sessions", "other-messages"),
                                                copiedConfig)) {
                                    // A different writer/table identity must not be over-serialized.
                                }
                            })
                    .doesNotThrowAnyException();
        }
    }

    private AgentConfiguration configuration(
            String collectorId, String sessionsTable, String messagesTable) {
        Map<String, String> catalog = new LinkedHashMap<>();
        catalog.put("metastore", "rest");
        catalog.put("uri", "http://catalog.example.test");
        catalog.put("warehouse", "warehouse");
        ProjectConfig project =
                new ProjectConfig(
                        "database",
                        sessionsTable,
                        messagesTable,
                        Duration.ofMinutes(5),
                        Duration.ofMinutes(5),
                        false,
                        collectorId,
                        new SourceConfig(false, tempDir),
                        new SourceConfig(false, tempDir),
                        true,
                        false,
                        1024,
                        100,
                        100,
                        0,
                        Duration.ofSeconds(1));
        return new AgentConfiguration(catalog, project);
    }
}
