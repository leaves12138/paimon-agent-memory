package org.apache.paimon.agent.source.claude;

import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.source.AttachmentResolver;
import org.apache.paimon.agent.source.IncrementalFiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ClaudeConversationSourceTest {

    @TempDir Path tempDir;

    @Test
    void rotatesHotSessionsBetweenBoundedScans() throws Exception {
        Path project = tempDir.resolve("projects/-tmp-project");
        Files.createDirectories(project);
        Path hot = project.resolve("hot.jsonl");
        Path cool = project.resolve("cool.jsonl");
        Files.writeString(
                hot, claudeUser("hot", "hot-1") + "\n" + claudeUser("hot", "hot-2") + "\n");
        Files.writeString(cool, claudeUser("cool", "cool") + "\n");
        Files.setLastModifiedTime(hot, FileTime.fromMillis(2_000));
        Files.setLastModifiedTime(cool, FileTime.fromMillis(1_000));
        ObjectMapper mapper = new ObjectMapper();
        ClaudeConversationSource source =
                new ClaudeConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        List<SessionBatch> first = source.scan(Collections.emptyMap(), 1);
        List<SessionBatch> second =
                source.scan(
                        Collections.singletonMap(
                                first.get(0).session().key(), first.get(0).session()),
                        1);

        assertThat(first)
                .singleElement()
                .satisfies(
                        batch ->
                                assertThat(batch.session().key().sessionId()).isEqualTo("hot"));
        assertThat(second)
                .singleElement()
                .satisfies(
                        batch ->
                                assertThat(batch.session().key().sessionId()).isEqualTo("cool"));
    }

    @Test
    void readsMainTranscriptAndExtractsNestedBase64Image() throws Exception {
        Path project = tempDir.resolve("projects/-tmp-project");
        Files.createDirectories(project);
        String data = Base64.getEncoder().encodeToString("jpeg".getBytes(StandardCharsets.UTF_8));
        String user =
                "{\"type\":\"user\",\"sessionId\":\"session-2\",\"uuid\":\"u1\","
                        + "\"timestamp\":\"2026-01-01T00:00:01Z\",\"cwd\":\"/tmp/project\","
                        + "\"message\":{\"role\":\"user\",\"content\":["
                        + "{\"type\":\"text\",\"text\":\"look\"},"
                        + "{\"type\":\"image\",\"source\":{\"type\":\"base64\","
                        + "\"media_type\":\"image/jpeg\",\"data\":\""
                        + data
                        + "\"}}]}}";
        String assistant =
                "{\"type\":\"assistant\",\"sessionId\":\"session-2\",\"uuid\":\"a1\","
                        + "\"timestamp\":\"2026-01-01T00:00:02Z\","
                        + "\"message\":{\"role\":\"assistant\",\"content\":["
                        + "{\"type\":\"text\",\"text\":\"seen\"}]}}";
        Files.writeString(
                project.resolve("session-2.jsonl"), user + "\n" + assistant + "\n");
        Files.writeString(
                project.resolve("sessions-index.json"),
                "{\"originalPath\":\"/tmp/project\",\"entries\":[{\"sessionId\":\"session-2\","
                        + "\"summary\":\"Image chat\"}]}" );

        ObjectMapper mapper = new ObjectMapper();
        ClaudeConversationSource source =
                new ClaudeConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));
        List<SessionBatch> batches = source.scan(Collections.emptyMap(), 100);

        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).session().title()).isEqualTo("Image chat");
        assertThat(batches.get(0).messages()).hasSize(2);
        assertThat(batches.get(0).messages().get(0).attachments().get(0).bytes())
                .isEqualTo("jpeg".getBytes(StandardCharsets.UTF_8));
        assertThat(batches.get(0).messages().get(0).contentJson())
                .contains("paimon-blob:0")
                .doesNotContain(data);
        assertThat(
                        source.scan(
                                Collections.singletonMap(
                                        batches.get(0).session().key(), batches.get(0).session()),
                                100))
                .isEmpty();
    }

    @Test
    void resumesAfterAnInPlaceRewriteMovesTheCheckpointAnchor() throws Exception {
        Path project = tempDir.resolve("projects/-tmp-project");
        Files.createDirectories(project);
        Path transcript = project.resolve("session-rewrite.jsonl");
        String first = claudeUser("session-rewrite", "first");
        Files.writeString(transcript, first + '\n');
        String originalFileKey = IncrementalFiles.fileKey(transcript);

        ObjectMapper mapper = new ObjectMapper();
        ClaudeConversationSource source =
                new ClaudeConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));
        SessionBatch checkpoint = source.scan(Collections.emptyMap(), 1).get(0);

        Files.writeString(
                transcript,
                "{\"type\":\"summary\"}\n"
                        + first
                        + '\n'
                        + claudeUser("session-rewrite", "second")
                        + '\n',
                StandardOpenOption.TRUNCATE_EXISTING);

        assertThat(IncrementalFiles.fileKey(transcript)).isEqualTo(originalFileKey);
        List<SessionBatch> resumed =
                source.scan(
                        Collections.singletonMap(
                                checkpoint.session().key(), checkpoint.session()),
                        100);

        assertThat(resumed).hasSize(1);
        assertThat(resumed.get(0).messages()).hasSize(1);
        assertThat(resumed.get(0).messages().get(0).contentJson()).contains("second");
    }

    private static String claudeUser(String sessionId, String text) {
        return "{\"type\":\"user\",\"sessionId\":\""
                + sessionId
                + "\","
                + "\"timestamp\":\"2026-01-01T00:00:01Z\","
                + "\"message\":{\"role\":\"user\",\"content\":["
                + "{\"type\":\"text\",\"text\":\""
                + text
                + "\"}]}}";
    }
}
