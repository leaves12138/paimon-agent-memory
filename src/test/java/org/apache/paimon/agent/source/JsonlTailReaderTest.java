package org.apache.paimon.agent.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonlTailReaderTest {

    @TempDir Path tempDir;

    @Test
    void tracksUtf8ByteOffsetsAndMarksPartialTail() throws Exception {
        Path file = tempDir.resolve("events.jsonl");
        String contents = "{\"text\":\"你好\"}\n{\"id\":2}\n{\"partial\":";
        Files.write(file, contents.getBytes(StandardCharsets.UTF_8));

        List<JsonlRecord> records = new JsonlTailReader().read(file, 0, 10);

        assertThat(records).hasSize(3);
        assertThat(records.get(0).endOffset())
                .isEqualTo("{\"text\":\"你好\"}\n".getBytes(StandardCharsets.UTF_8).length);
        assertThat(records.get(0).lineTerminated()).isTrue();
        assertThat(records.get(2).lineTerminated()).isFalse();
        assertThat(records.get(2).json()).isEqualTo("{\"partial\":");
    }

    @Test
    void neverReadsPastTheWakeUpBoundary() throws Exception {
        Path file = tempDir.resolve("growing.jsonl");
        Files.writeString(file, "{\"id\":1}\n{\"partial\":");
        long boundary = Files.size(file);
        Files.writeString(
                file,
                "true}\n{\"id\":2}\n",
                StandardOpenOption.APPEND);

        List<JsonlRecord> bounded =
                new JsonlTailReader().read(file, 0, 10, boundary);

        assertThat(bounded).hasSize(2);
        assertThat(bounded.get(0).lineTerminated()).isTrue();
        assertThat(bounded.get(1).json()).isEqualTo("{\"partial\":");
        assertThat(bounded.get(1).lineTerminated()).isFalse();
        assertThat(new JsonlTailReader().read(file, boundary, 10, boundary)).isEmpty();
    }

    @Test
    void boundedReadRejectsATruncationRaceInsteadOfRestartingAtZero() throws Exception {
        Path file = tempDir.resolve("truncated.jsonl");
        Files.writeString(file, "{\"id\":1}\n{\"id\":2}\n");
        long boundary = Files.size(file);
        Files.writeString(
                file,
                "{\"id\":1}\n",
                StandardOpenOption.TRUNCATE_EXISTING);

        assertThatThrownBy(() -> new JsonlTailReader().read(file, 10, 10, boundary))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("truncated");
    }
}
