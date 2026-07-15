package org.apache.paimon.agent.source.codex;

import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.source.AttachmentResolver;
import org.apache.paimon.agent.source.ConversationSource;
import org.apache.paimon.agent.source.IncrementalFiles;
import org.apache.paimon.agent.source.SourceCursors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class CodexConversationSourceTest {

    @TempDir Path tempDir;

    @Test
    void assignsTrueAndFalseFromAuthoritativeProjectlessState() throws Exception {
        Path directory = tempDir.resolve("sessions/2026/01/01");
        Files.createDirectories(directory);
        Path projectless = directory.resolve("projectless.jsonl");
        Path project = directory.resolve("project.jsonl");
        Files.writeString(projectless, canonicalUser("without project") + "\n");
        Files.writeString(project, canonicalUser("with project") + "\n");
        createStateDatabase(
                new ThreadRow("projectless", projectless, "No project", 2),
                new ThreadRow("project", project, "Project", 1));
        writeGlobalState("{\"projectless-thread-ids\":[\"projectless\"]}");
        ObjectMapper mapper = new ObjectMapper();
        CodexConversationSource source =
                new CodexConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        List<SessionBatch> batches = source.scan(Collections.emptyMap(), 100);

        assertThat(batches)
                .filteredOn(batch -> batch.session().key().sessionId().equals("projectless"))
                .singleElement()
                .satisfies(batch -> assertThat(batch.session().projectless()).isTrue());
        assertThat(batches)
                .filteredOn(batch -> batch.session().key().sessionId().equals("project"))
                .singleElement()
                .satisfies(batch -> assertThat(batch.session().projectless()).isFalse());
    }

    @Test
    void unknownProjectlessStateInheritsCheckpointAndNeverBlocksCollection() throws Exception {
        Path directory = tempDir.resolve("sessions/2026/01/01");
        Files.createDirectories(directory);
        Path rollout = directory.resolve("unknown.jsonl");
        Files.writeString(rollout, canonicalUser("first") + "\n");
        createStateDatabase(new ThreadRow("unknown", rollout, "Unknown", 1));
        ObjectMapper mapper = new ObjectMapper();
        CodexConversationSource source =
                new CodexConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        SessionBatch missing = source.scan(Collections.emptyMap(), 100).get(0);
        assertThat(missing.session().projectless()).isNull();

        writeGlobalState("{\"projectless-thread-ids\":[\"unknown\"]}");
        SessionBatch known =
                source.scan(
                                Collections.singletonMap(
                                        missing.session().key(), missing.session()),
                                100)
                        .get(0);
        assertThat(known.messages()).isEmpty();
        assertThat(known.session().projectless()).isTrue();

        writeGlobalState("{\"projectless-thread-ids\":[42]}");
        Files.writeString(
                rollout,
                canonicalUser("invalid field") + "\n",
                StandardOpenOption.APPEND);
        SessionBatch invalid =
                source.scan(Collections.singletonMap(known.session().key(), known.session()), 100)
                        .get(0);
        assertThat(invalid.messages()).singleElement();
        assertThat(invalid.session().projectless()).isTrue();

        writeGlobalState("{not-json");
        Files.writeString(
                rollout,
                canonicalUser("corrupt file") + "\n",
                StandardOpenOption.APPEND);
        SessionBatch corrupt =
                source.scan(
                                Collections.singletonMap(
                                        invalid.session().key(), invalid.session()),
                                100)
                        .get(0);
        assertThat(corrupt.messages()).singleElement();
        assertThat(corrupt.session().projectless()).isTrue();

        Path globalState = tempDir.resolve(".codex-global-state.json");
        Files.delete(globalState);
        Path symlinkTarget = tempDir.resolve("symlink-target.json");
        Files.writeString(symlinkTarget, "{\"projectless-thread-ids\":[]}");
        Files.createSymbolicLink(globalState, symlinkTarget.getFileName());
        Files.writeString(rollout, canonicalUser("symlink") + "\n", StandardOpenOption.APPEND);
        SessionBatch symlink =
                source.scan(
                                Collections.singletonMap(
                                        corrupt.session().key(), corrupt.session()),
                                100)
                        .get(0);
        assertThat(symlink.messages()).singleElement();
        assertThat(symlink.session().projectless()).isTrue();
    }

    @Test
    void snapshotsProjectlessStateOnceAndPublishesMetadataOnlyChanges() throws Exception {
        Path directory = tempDir.resolve("sessions/2026/01/01");
        Files.createDirectories(directory);
        Path rollout = directory.resolve("snapshot.jsonl");
        Files.writeString(rollout, canonicalUser("first") + "\n");
        createStateDatabase(new ThreadRow("snapshot", rollout, "Snapshot", 1));
        writeGlobalState("{\"projectless-thread-ids\":[\"snapshot\"]}");
        ObjectMapper mapper = new ObjectMapper();
        CodexConversationSource source =
                new CodexConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        SessionBatch captured;
        try (ConversationSource.ScanCycle cycle = source.openScanCycle()) {
            writeGlobalState("{\"projectless-thread-ids\":[]}");
            captured = cycle.scan(Collections.emptyMap(), 100, Collections.emptySet()).get(0);
        }
        assertThat(captured.session().projectless()).isTrue();

        List<SessionBatch> changed =
                source.scan(
                        Collections.singletonMap(captured.session().key(), captured.session()),
                        100);

        assertThat(changed).singleElement();
        assertThat(changed.get(0).messages()).isEmpty();
        assertThat(changed.get(0).sourceRecordsRead()).isZero();
        assertThat(changed.get(0).session().projectless()).isFalse();
    }

    @Test
    void usesLastValidSidebarThreadNameAndPublishesRenameWithoutNewMessages() throws Exception {
        Path directory = tempDir.resolve("sessions/2026/01/01");
        Files.createDirectories(directory);
        Path rollout = directory.resolve("named.jsonl");
        Files.writeString(rollout, canonicalUser("First user question") + "\n");
        createStateDatabase(
                new ThreadRow("named", rollout, "First user question", 2));
        Path index = tempDir.resolve("session_index.jsonl");
        Files.writeString(
                index,
                String.join(
                                "\n",
                                "{\"id\":\"named\",\"thread_name\":\"Old name\","
                                        + "\"updated_at\":\"2026-01-01T00:00:00Z\"}",
                                "{not-json",
                                "{\"id\":\"named\",\"thread_name\":\"Tie loser\","
                                        + "\"updated_at\":\"2026-01-01T00:00:01Z\"}",
                                "{\"id\":\"named\",\"thread_name\":\"Official sidebar title\","
                                        + "\"updated_at\":\"2026-01-01T00:00:01Z\"}",
                                "{\"id\":\"named\",\"thread_name\":\"Missing timestamp\"}",
                                "{\"id\":\"named\",\"thread_name\":42,"
                                        + "\"updated_at\":\"2026-01-01T00:00:08Z\"}",
                                "{\"id\":\"named\",\"thread_name\":\"   \","
                                        + "\"updated_at\":\"2026-01-01T00:00:09Z\"}")
                        + "\n",
                StandardCharsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        CodexConversationSource source =
                new CodexConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        SessionBatch first = source.scan(Collections.emptyMap(), 100).get(0);

        assertThat(first.session().title()).isEqualTo("Official sidebar title");
        assertThat(first.session().updatedAt())
                .isEqualTo(Instant.parse("2026-01-01T00:00:01Z"));

        Files.writeString(
                index,
                "{\"id\":\"named\",\"thread_name\":\"Earlier-clock rename\","
                        + "\"updated_at\":\"2025-01-01T00:00:00Z\"}\n"
                        + "{\"id\":\"named\",\"thread_name\":\"  Renamed sidebar title  \","
                        + "\"updated_at\":\"unknown\"}\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);

        List<SessionBatch> renamed =
                source.scan(
                        Collections.singletonMap(first.session().key(), first.session()),
                        100);

        assertThat(renamed).singleElement();
        assertThat(renamed.get(0).messages()).isEmpty();
        assertThat(renamed.get(0).sourceRecordsRead()).isZero();
        assertThat(renamed.get(0).session().title()).isEqualTo("Renamed sidebar title");
        assertThat(renamed.get(0).session().updatedAt())
                .isEqualTo(Instant.parse("2026-01-01T00:00:01Z"));
    }

    @Test
    void appliesSidebarThreadNameToRolloutMissingFromSqlite() throws Exception {
        Path directory = tempDir.resolve("sessions/2026/01/01");
        Files.createDirectories(directory);
        Path rollout = directory.resolve("unindexed.jsonl");
        Files.writeString(
                rollout,
                "{\"timestamp\":\"2026-01-01T00:00:00Z\",\"type\":\"session_meta\","
                        + "\"payload\":{\"id\":\"unindexed\",\"cwd\":\"/tmp/project\"}}\n"
                        + canonicalUser("first prompt")
                        + "\n",
                StandardCharsets.UTF_8);
        Files.writeString(
                tempDir.resolve("session_index.jsonl"),
                "{\"id\":\"unindexed\",\"thread_name\":\"Discovered title\","
                        + "\"updated_at\":\"2026-01-01T00:00:02Z\"}\n",
                StandardCharsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        CodexConversationSource source =
                new CodexConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        SessionBatch batch = source.scan(Collections.emptyMap(), 100).get(0);

        assertThat(batch.session().key().sessionId()).isEqualTo("unindexed");
        assertThat(batch.session().title()).isEqualTo("Discovered title");
    }

    @Test
    void rotatesHotSessionsBetweenBoundedScans() throws Exception {
        Path directory = tempDir.resolve("sessions/2026/01/01");
        Files.createDirectories(directory);
        Path hot = directory.resolve("hot.jsonl");
        Path cool = directory.resolve("cool.jsonl");
        Files.writeString(
                hot,
                canonicalUser("hot-1") + "\n" + canonicalUser("hot-2") + "\n",
                StandardCharsets.UTF_8);
        Files.writeString(cool, canonicalUser("cool") + "\n", StandardCharsets.UTF_8);
        createStateDatabase(
                new ThreadRow("hot", hot, "Hot", 2),
                new ThreadRow("cool", cool, "Cool", 1));
        ObjectMapper mapper = new ObjectMapper();
        CodexConversationSource source =
                new CodexConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        List<SessionBatch> first = source.scan(Collections.emptyMap(), 1);
        List<SessionBatch> second =
                source.scan(
                        Collections.singletonMap(
                                first.get(0).session().key(), first.get(0).session()),
                        1);

        assertThat(first).singleElement().satisfies(batch -> assertThat(batch.session().key().sessionId()).isEqualTo("hot"));
        assertThat(second).singleElement().satisfies(batch -> assertThat(batch.session().key().sessionId()).isEqualTo("cool"));
    }

    @Test
    void scanCycleStopsAtTheEofCapturedWhenTheWakeUpStarted() throws Exception {
        Path directory = tempDir.resolve("sessions/2026/01/01");
        Files.createDirectories(directory);
        Path rollout = directory.resolve("growing.jsonl");
        Files.writeString(rollout, canonicalUser("before") + "\n");
        createStateDatabase(new ThreadRow("growing", rollout, "Growing", 1));
        ObjectMapper mapper = new ObjectMapper();
        CodexConversationSource source =
                new CodexConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        SessionBatch first;
        try (ConversationSource.ScanCycle cycle = source.openScanCycle()) {
            Files.writeString(
                    rollout,
                    canonicalUser("after") + "\n",
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
    void usesFirstSessionMetaForUnindexedSubagentAndBackfillsAtEof() throws Exception {
        Path directory = tempDir.resolve("sessions/2026/01/01");
        Files.createDirectories(directory);
        Path rollout = directory.resolve("subagent.jsonl");
        String childMeta =
                "{\"timestamp\":\"2026-01-01T00:00:00Z\",\"type\":\"session_meta\","
                        + "\"payload\":{\"id\":\"child\",\"cwd\":\"/tmp/project\","
                        + "\"source\":{\"subagent\":{\"thread_spawn\":{"
                        + "\"depth\":1,\"parent_thread_id\":\"root\","
                        + "\"agent_path\":\"/root/audit\"}}}}}";
        String copiedParentMeta =
                "{\"timestamp\":\"2026-01-01T00:00:00Z\",\"type\":\"session_meta\","
                        + "\"payload\":{\"id\":\"root\",\"cwd\":\"/tmp/project\","
                        + "\"source\":\"vscode\"}}";
        Files.writeString(
                rollout,
                childMeta + "\n" + copiedParentMeta + "\n" + canonicalUser("work") + "\n",
                StandardCharsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        CodexConversationSource source =
                new CodexConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        SessionBatch collected = source.scan(Collections.emptyMap(), 100).get(0);
        String expected =
                "{\"thread_spawn\":{\"depth\":1,\"parent_thread_id\":\"root\","
                        + "\"agent_path\":\"/root/audit\"}}";
        assertThat(collected.session().key().sessionId()).isEqualTo("child");
        assertThat(collected.session().subagentSourceJson()).isEqualTo(expected);

        ChatSession legacyCheckpoint =
                new ChatSession(
                        collected.session().key(),
                        collected.session().title(),
                        collected.session().cwd(),
                        collected.session().archived(),
                        collected.session().sourcePath(),
                        collected.session().sourceCursor(),
                        collected.session().lastCommitId(),
                        collected.session().createdAt(),
                        collected.session().updatedAt(),
                        collected.session().lastMessageAt(),
                        collected.session().ingestedAt());
        List<SessionBatch> backfill =
                source.scan(
                        Collections.singletonMap(
                                legacyCheckpoint.key(), legacyCheckpoint),
                        100);

        assertThat(backfill).singleElement();
        assertThat(backfill.get(0).sourceRecordsRead()).isZero();
        assertThat(backfill.get(0).messages()).isEmpty();
        assertThat(backfill.get(0).session().subagentSourceJson()).isEqualTo(expected);
    }

    @Test
    void retryableAttachmentFailureDoesNotBlockLaterSession() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/fail",
                exchange -> {
                    exchange.sendResponseHeaders(500, -1);
                    exchange.close();
                });
        server.start();
        try {
            Path directory = tempDir.resolve("sessions/2026/01/01");
            Files.createDirectories(directory);
            Path bad = directory.resolve("bad.jsonl");
            Path good = directory.resolve("good.jsonl");
            String remote =
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/fail?token=secret";
            Files.writeString(
                    bad,
                    "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"type\":\"response_item\","
                            + "\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":["
                            + "{\"type\":\"input_image\",\"image_url\":\""
                            + remote
                            + "\"}]}}\n",
                    StandardCharsets.UTF_8);
            Files.writeString(good, canonicalUser("good") + "\n", StandardCharsets.UTF_8);
            createStateDatabase(
                    new ThreadRow("bad", bad, "Bad", 2),
                    new ThreadRow("good", good, "Good", 1));
            ObjectMapper mapper = new ObjectMapper();
            CodexConversationSource source =
                    new CodexConversationSource(
                            tempDir, mapper, new AttachmentResolver(mapper, true, true, 1024));

            List<SessionBatch> batches = source.scan(Collections.emptyMap(), 100);

            assertThat(batches)
                    .extracting(batch -> batch.session().key().sessionId())
                    .containsExactly("good");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void readsCanonicalMessagesAndMovesInlineImageToArrayBlob() throws Exception {
        Path sessions = tempDir.resolve("sessions/2026/01/01");
        Files.createDirectories(sessions);
        Path rollout = sessions.resolve("rollout-session-1.jsonl");
        Path attachments = tempDir.resolve("attachments");
        Files.createDirectories(attachments);
        Path duplicateImage = attachments.resolve("same-image.png");
        Path notes = attachments.resolve("notes.txt");
        Files.writeString(duplicateImage, "png", StandardCharsets.UTF_8);
        Files.writeString(notes, "notes", StandardCharsets.UTF_8);
        String image = Base64.getEncoder().encodeToString("png".getBytes(StandardCharsets.UTF_8));
        String sessionMeta =
                "{\"timestamp\":\"2026-01-01T00:00:00Z\",\"type\":\"session_meta\","
                        + "\"payload\":{\"id\":\"session-1\",\"cwd\":\"/tmp/project\"}}";
        String user =
                "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"type\":\"response_item\","
                        + "\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":["
                        + "{\"type\":\"input_text\",\"text\":\"hello\\n"
                        + "## pasted image: "
                        + duplicateImage
                        + "\\n## 2026-07-02 13:46:33 notes.txt: "
                        + notes
                        + "\\n<image name=[Image #1] path=\\\""
                        + duplicateImage
                        + "\\\">\"},"
                        + "{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,"
                        + image
                        + "\"}]}}";
        String duplicate =
                "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"type\":\"event_msg\","
                        + "\"payload\":{\"type\":\"user_message\",\"message\":\"hello\"}}";
        String assistant =
                "{\"timestamp\":\"2026-01-01T00:00:02Z\",\"type\":\"response_item\","
                        + "\"payload\":{\"type\":\"message\",\"role\":\"assistant\","
                        + "\"content\":[{\"type\":\"output_text\",\"text\":\"hi\"}]}}";
        Files.writeString(
                rollout,
                String.join("\n", sessionMeta, user, duplicate, assistant) + "\n{\"partial\":",
                StandardCharsets.UTF_8);

        createStateDatabase(rollout);
        ObjectMapper mapper = new ObjectMapper();
        CodexConversationSource source =
                new CodexConversationSource(
                        tempDir, mapper, new AttachmentResolver(mapper, true, false, 1024));

        List<SessionBatch> batches = source.scan(Collections.emptyMap(), 100);

        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).session().key().sessionId()).isEqualTo("session-1");
        assertThat(batches.get(0).session().title()).isEqualTo("Test title");
        assertThat(batches.get(0).messages()).hasSize(2);
        assertThat(batches.get(0).messages().get(0).attachments()).hasSize(2);
        assertThat(batches.get(0).messages().get(0).attachments().get(0).bytes())
                .isEqualTo("png".getBytes(StandardCharsets.UTF_8));
        assertThat(batches.get(0).messages().get(0).attachments().get(1).bytes())
                .isEqualTo("notes".getBytes(StandardCharsets.UTF_8));
        assertThat(batches.get(0).messages().get(0).contentJson())
                .contains("paimon-blob:0")
                .doesNotContain(image);
        assertThat(
                        source.scan(
                                Collections.singletonMap(
                                        batches.get(0).session().key(), batches.get(0).session()),
                                100))
                .isEmpty();

        String fileKey = IncrementalFiles.fileKey(rollout);
        long startOffset = (sessionMeta + "\n").getBytes(StandardCharsets.UTF_8).length;
        long fixedEndOffset =
                startOffset + (user + "\n").getBytes(StandardCharsets.UTF_8).length;
        SessionKey key = new SessionKey("codex", "session-1");
        ChatSession unfinished =
                new ChatSession(
                        key,
                        "Test title",
                        "/tmp/project",
                        false,
                        rollout.toString(),
                        SourceCursors.file(
                                startOffset,
                                fileKey,
                                IncrementalFiles.lineAnchor(sessionMeta)),
                        -1L,
                        0L,
                        SourceCursors.file(
                                fixedEndOffset, fileKey, IncrementalFiles.lineAnchor(user)),
                        Instant.EPOCH,
                        Instant.EPOCH,
                        null,
                        Instant.EPOCH);

        List<SessionBatch> recovered =
                source.scan(Map.of(key, unfinished), 100, Set.of(key));

        assertThat(recovered).hasSize(1);
        assertThat(recovered.get(0).sourceRecordsRead()).isEqualTo(1);
        assertThat(recovered.get(0).messages()).hasSize(1);
        assertThat(recovered.get(0).messages().get(0).role()).isEqualTo("user");
        assertThat(
                        SourceCursors.sameLogicalBoundary(
                                recovered.get(0).session().sourceCursor(),
                                unfinished.pendingCursor()))
                .isTrue();
    }

    private void createStateDatabase(Path rollout) throws Exception {
        createStateDatabase(new ThreadRow("session-1", rollout, "Test title", 2));
    }

    private void writeGlobalState(String value) throws Exception {
        Files.writeString(
                tempDir.resolve(".codex-global-state.json"),
                value,
                StandardCharsets.UTF_8);
    }

    private void createStateDatabase(ThreadRow... rows) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection("jdbc:sqlite:" + tempDir.resolve("state_5.sqlite"));
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE threads (id TEXT, rollout_path TEXT, title TEXT, cwd TEXT, "
                            + "archived INTEGER, created_at INTEGER, updated_at INTEGER)");
            for (ThreadRow row : rows) {
                statement.execute(
                        "INSERT INTO threads VALUES ('"
                                + row.sessionId
                                + "', '"
                                + row.rollout.toString().replace("'", "''")
                                + "', '"
                                + row.title
                                + "', '/tmp/project', 0, 1, "
                                + row.updatedAt
                                + ")");
            }
        }
    }

    private static String canonicalUser(String value) {
        return "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"type\":\"response_item\","
                + "\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":["
                + "{\"type\":\"input_text\",\"text\":\""
                + value
                + "\"}]}}";
    }

    private static final class ThreadRow {
        private final String sessionId;
        private final Path rollout;
        private final String title;
        private final long updatedAt;

        private ThreadRow(String sessionId, Path rollout, String title, long updatedAt) {
            this.sessionId = sessionId;
            this.rollout = rollout;
            this.title = title;
            this.updatedAt = updatedAt;
        }
    }
}
