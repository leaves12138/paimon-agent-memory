package org.apache.paimon.agent.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
}
