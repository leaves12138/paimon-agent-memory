package org.apache.paimon.agent.source.claude;

import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.source.AttachmentResolver;
import org.apache.paimon.agent.source.ConversationSource;
import org.apache.paimon.agent.source.IncrementalFiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    void scanCycleStopsAtTheEofCapturedWhenTheWakeUpStarted() throws Exception {
        Path project = tempDir.resolve("projects/-tmp-project");
        Files.createDirectories(project);
        Path transcript = project.resolve("growing.jsonl");
        Files.writeString(transcript, claudeUser("growing", "before") + "\n");
        ObjectMapper mapper = new ObjectMapper();
        ClaudeConversationSource source =
                new ClaudeConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        SessionBatch first;
        try (ConversationSource.ScanCycle cycle = source.openScanCycle()) {
            Files.writeString(
                    transcript,
                    claudeUser("growing", "after") + "\n",
                    StandardOpenOption.APPEND);
            first = cycle.scan(Collections.emptyMap(), 1, Collections.emptySet()).get(0);

            assertThat(
                            cycle.scan(
                                    Collections.singletonMap(
                                            first.session().key(), first.session()),
                                    100,
                                    Collections.emptySet()))
                    .isEmpty();
        }

        List<SessionBatch> nextWake =
                source.scan(
                        Collections.singletonMap(first.session().key(), first.session()), 100);
        assertThat(nextWake).singleElement();
        assertThat(nextWake.get(0).messages()).singleElement();
        assertThat(nextWake.get(0).messages().get(0).contentJson()).contains("after");
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
        assertThat(batches.get(0).session().subagentSourceJson()).isNull();
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

    @Test
    void resumesAfterACloudRestoreBoundaryWithoutRecollectingRestoredMessages()
            throws Exception {
        Path project = tempDir.resolve("projects/-tmp-project");
        Files.createDirectories(project);
        Path transcript = project.resolve("session-restored.jsonl");
        String first = claudeUser("session-restored", "original");
        Files.writeString(transcript, first + '\n');
        ObjectMapper mapper = new ObjectMapper();
        ClaudeConversationSource source =
                new ClaudeConversationSource(
                        tempDir,
                        mapper,
                        new AttachmentResolver(mapper, true, false, 1024));
        SessionBatch checkpoint = source.scan(Collections.emptyMap(), 100).get(0);

        String restoredAssistant =
                withRestoreBoundary(
                        mapper,
                "{\"type\":\"assistant\",\"sessionId\":\"session-restored\","
                        + "\"uuid\":\"restored-assistant\","
                        + "\"message\":{\"role\":\"assistant\",\"content\":["
                                + "{\"type\":\"text\",\"text\":\"restored\"}]}}",
                        checkpoint.session());
        Files.writeString(
                transcript,
                "{\"type\":\"summary\"}\n"
                        + first
                        + '\n'
                        + restoredAssistant
                        + '\n'
                        + claudeUser("session-restored", "after restore")
                        + '\n',
                StandardOpenOption.TRUNCATE_EXISTING);

        List<SessionBatch> resumed =
                source.scan(
                        Collections.singletonMap(
                                checkpoint.session().key(), checkpoint.session()),
                        100);

        assertThat(resumed).hasSize(1);
        assertThat(resumed.get(0).messages()).hasSize(1);
        assertThat(resumed.get(0).messages().get(0).contentJson())
                .contains("after restore")
                .doesNotContain("\"text\":\"restored\"");

        Files.writeString(
                transcript,
                "{\"type\":\"a-longer-summary-after-the-checkpoint-advanced\"}\n"
                        + first
                        + '\n'
                        + restoredAssistant
                        + '\n'
                        + claudeUser("session-restored", "after restore")
                        + '\n',
                StandardOpenOption.TRUNCATE_EXISTING);
        List<SessionBatch> remapped =
                source.scan(
                        Collections.singletonMap(
                                resumed.get(0).session().key(), resumed.get(0).session()),
                        100);
        assertThat(remapped).singleElement().satisfies(batch -> assertThat(batch.messages()).isEmpty());
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

    private static String withRestoreBoundary(
            ObjectMapper mapper, String json, ChatSession checkpoint) throws Exception {
        ObjectNode event = (ObjectNode) mapper.readTree(json);
        event.set(
                IncrementalFiles.RESTORE_BOUNDARY_FIELD,
                IncrementalFiles.restoreBoundaryMarker(mapper, checkpoint));
        return mapper.writeValueAsString(event);
    }
}
