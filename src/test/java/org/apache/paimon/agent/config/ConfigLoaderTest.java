package org.apache.paimon.agent.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigLoaderTest {

    @TempDir Path tempDir;

    @Test
    void keepsCatalogAndProjectPropertiesSeparate() throws Exception {
        Path paimon = tempDir.resolve("paimon.properties");
        Path project = tempDir.resolve("project.properties");
        Files.writeString(
                paimon,
                "metastore=REST\nuri=http://localhost:8080\nwarehouse=test\ntoken=secret\n");
        Files.writeString(
                project,
                "database=memory\ncollector.scan.interval=30s\n"
                        + "collector.id=test-installation\n"
                        + "collector.commit.interval=2m\ncollector.codex.enabled=false\n"
                        + "collector.claude.enabled=true\ncollector.claude.path=~/claude-test\n");

        AgentConfiguration loaded = ConfigLoader.load(paimon, project);

        assertThat(loaded.catalogOptions())
                .containsEntry("metastore", "rest")
                .containsEntry("token", "secret")
                .doesNotContainKey("database");
        assertThat(loaded.project().database()).isEqualTo("memory");
        assertThat(loaded.project().scanInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(loaded.project().commitInterval()).isEqualTo(Duration.ofMinutes(2));
        assertThat(loaded.project().dashboard().enabled()).isTrue();
        assertThat(loaded.project().dashboard().host()).isEqualTo("127.0.0.1");
        assertThat(loaded.project().dashboard().port()).isEqualTo(8787);
        assertThat(loaded.project().dashboard().pageSize()).isEqualTo(25);
        assertThat(loaded.project().dashboard().maxScanRows()).isEqualTo(50_000);
        assertThat(loaded.project().codex().enabled()).isFalse();
        assertThat(loaded.project().claude().path())
                .isEqualTo(
                        Path.of(System.getProperty("user.home"), "claude-test")
                                .toAbsolutePath()
                                .normalize());
    }

    @Test
    void acceptsOnlyTheRestMetastoreProperty() throws Exception {
        Path project = tempDir.resolve("project.properties");
        Files.writeString(project, "database=memory\ncollector.id=test-installation\n");

        Path legacy = tempDir.resolve("legacy.properties");
        Files.writeString(legacy, "type=rest\nwarehouse=test\n");
        assertThatThrownBy(() -> ConfigLoader.load(legacy, project))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("use metastore=rest");

        Path unsupported = tempDir.resolve("unsupported.properties");
        Files.writeString(unsupported, "metastore=filesystem\nwarehouse=test\n");
        assertThatThrownBy(() -> ConfigLoader.load(unsupported, project))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("only metastore=rest is supported");
    }

    @Test
    void rejectsTheDistributionCollectorIdPlaceholder() throws Exception {
        Path paimon = tempDir.resolve("paimon.properties");
        Path project = tempDir.resolve("project.properties");
        Files.writeString(
                paimon,
                "metastore=rest\nuri=http://localhost:8080\nwarehouse=test\n");
        Files.writeString(
                project,
                "database=memory\ncollector.id=replace-with-a-unique-installation-id\n");

        assertThatThrownBy(() -> ConfigLoader.load(paimon, project))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("distribution placeholder");
    }

    @Test
    void rejectsRemoteOrUnboundedDashboardSettings() throws Exception {
        Path paimon = tempDir.resolve("paimon.properties");
        Path remote = tempDir.resolve("remote.properties");
        Files.writeString(
                paimon,
                "metastore=rest\nuri=http://localhost:8080\nwarehouse=test\n");
        Files.writeString(
                remote,
                "database=memory\ncollector.id=test-installation\n"
                        + "dashboard.host=0.0.0.0\n");

        assertThatThrownBy(() -> ConfigLoader.load(paimon, remote))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("loopback");

        Path invalidPage = tempDir.resolve("invalid-page.properties");
        Files.writeString(
                invalidPage,
                "database=memory\ncollector.id=test-installation\n"
                        + "dashboard.page-size=101\ndashboard.max-page-size=100\n");
        assertThatThrownBy(() -> ConfigLoader.load(paimon, invalidPage))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("max-page-size");
    }
}
