package org.apache.paimon.agent.restore;

import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.sink.ChatMessageConsumer;
import org.apache.paimon.agent.sink.ChatRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestoreServiceTest {

    @TempDir Path tempDir;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void restoresCodexRolloutSidebarAndDataUri() throws Exception {
        String sessionId = "019f5952-14cd-7d21-a7f7-87b31cb62da6";
        SessionKey key = new SessionKey("codex", sessionId);
        ChatSession session = session(key, "Restored title", tempDir.toString());
        String content =
                "{\"timestamp\":\"2026-01-01T00:00:00Z\","
                        + "\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                        + "\"role\":\"user\",\"content\":[{\"type\":\"input_text\","
                        + "\"text\":\"hello restored codex\"},{\"type\":\"input_image\","
                        + "\"image_url\":\"paimon-blob:0\"}]},"
                        + "\"_paimon_attachments\":[{\"index\":0,"
                        + "\"source_kind\":\"data_uri\",\"source_reference\":\"inline:image/png\","
                        + "\"mime_type\":\"image/png\"}]}";
        ChatMessage message =
                message(
                        key,
                        "m1",
                        9L,
                        "user",
                        "response_item",
                        content,
                        Collections.singletonList(AttachmentPayload.of(new byte[] {1, 2, 3})));
        ChatMessage context =
                message(
                        key,
                        "m0",
                        1L,
                        "user",
                        "response_item",
                        "{\"timestamp\":\"2026-01-01T00:00:00Z\","
                                + "\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                                + "\"role\":\"user\",\"content\":[{\"type\":\"input_text\","
                                + "\"text\":\"<environment_context>hidden</environment_context>\"}]}}",
                        Collections.emptyList());
        ChatMessage assistant =
                message(
                        key,
                        "m2",
                        10L,
                        "assistant",
                        "response_item",
                        "{\"timestamp\":\"2026-01-01T00:00:01Z\","
                                + "\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                                + "\"role\":\"assistant\",\"phase\":\"final_answer\","
                                + "\"content\":[{\"type\":\"output_text\",\"text\":\"restored answer\"}]}}",
                        Collections.emptyList());
        FakeRepository repository = new FakeRepository(session, assistant, message, context);
        Path codexHome = tempDir.resolve("codex-home");
        initializeCodexState(codexHome);
        Path data = tempDir.resolve("data");

        RestoreSummary first =
                new RestoreService(repository, mapper)
                        .restore(
                                new RestoreOptions(
                                        RestoreType.CODEX,
                                        codexHome,
                                        data,
                                        null,
                                        null,
                                        false));

        assertThat(first.restoredSessions()).isEqualTo(1);
        assertThat(first.restoredMessages()).isEqualTo(3);
        List<Path> rollouts;
        try (Stream<Path> files = Files.walk(codexHome.resolve("sessions"))) {
            rollouts =
                    files.filter(path -> path.toString().endsWith(".jsonl"))
                            .collect(Collectors.toList());
        }
        assertThat(rollouts).hasSize(1);
        assertThat(rollouts.get(0).getFileName().toString())
                .endsWith("-" + sessionId + ".jsonl");
        List<String> lines = Files.readAllLines(rollouts.get(0), StandardCharsets.UTF_8);
        assertThat(mapper.readTree(lines.get(0)).path("type").asText()).isEqualTo("session_meta");
        List<JsonNode> events =
                lines.stream()
                        .map(
                                line -> {
                                    try {
                                        return mapper.readTree(line);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(Collectors.toList());
        JsonNode event =
                events.stream()
                        .filter(node -> "response_item".equals(node.path("type").asText()))
                        .filter(node -> node.path("payload").path("content").size() == 2)
                        .findFirst()
                        .orElseThrow();
        assertThat(event.has("_paimon_attachments")).isFalse();
        assertThat(event.path("payload").path("content").get(1).path("image_url").asText())
                .isEqualTo("data:image/png;base64,AQID");
        JsonNode userEvent =
                events.stream()
                        .filter(node -> "event_msg".equals(node.path("type").asText()))
                        .filter(
                                node ->
                                        "user_message"
                                                .equals(
                                                        node.path("payload")
                                                                .path("type")
                                                                .asText()))
                        .findFirst()
                        .orElseThrow();
        assertThat(userEvent.path("payload").path("message").asText())
                .isEqualTo("hello restored codex");
        assertThat(userEvent.path("payload").path("images").get(0).asText())
                .isEqualTo("data:image/png;base64,AQID");
        JsonNode agentEvent =
                events.stream()
                        .filter(
                                node ->
                                        "agent_message"
                                                .equals(
                                                        node.path("payload")
                                                                .path("type")
                                                                .asText()))
                        .findFirst()
                        .orElseThrow();
        assertThat(agentEvent.path("payload").path("message").asText())
                .isEqualTo("restored answer");
        List<String> turnIds =
                events.stream()
                        .filter(
                                node ->
                                        "task_started".equals(
                                                        node.path("payload")
                                                                .path("type")
                                                                .asText())
                                                || "task_complete".equals(
                                                        node.path("payload")
                                                                .path("type")
                                                                .asText()))
                        .map(node -> node.path("payload").path("turn_id").asText())
                        .collect(Collectors.toList());
        assertThat(turnIds).hasSize(2);
        assertThat(turnIds.get(0)).isEqualTo(turnIds.get(1));

        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + codexHome.resolve("state_5.sqlite"));
                Statement statement = connection.createStatement();
                ResultSet row =
                        statement.executeQuery(
                                "SELECT title, first_user_message, preview, rollout_path "
                                        + "FROM threads WHERE id = '"
                                        + sessionId
                                        + "'")) {
            assertThat(row.next()).isTrue();
            assertThat(row.getString("title")).isEqualTo("Restored title");
            assertThat(row.getString("first_user_message"))
                    .isEqualTo("hello restored codex");
            assertThat(row.getString("preview")).isEqualTo("hello restored codex");
            assertThat(Path.of(row.getString("rollout_path")))
                    .isEqualTo(rollouts.get(0).toRealPath());
        }
        List<String> sessionIndex =
                Files.readAllLines(codexHome.resolve("session_index.jsonl"), StandardCharsets.UTF_8);
        assertThat(sessionIndex).hasSize(1);
        JsonNode sessionIndexEntry = mapper.readTree(sessionIndex.get(0));
        assertThat(sessionIndexEntry.path("id").asText()).isEqualTo(sessionId);
        assertThat(sessionIndexEntry.path("thread_name").asText()).isEqualTo("Restored title");
        assertThat(sessionIndexEntry.path("updated_at").asText())
                .isEqualTo("2026-01-01T00:01:00Z");
        assertThat(data.resolve("restore")).isEmptyDirectory();

        RestoreSummary second =
                new RestoreService(repository, mapper)
                        .restore(
                                new RestoreOptions(
                                        RestoreType.CODEX,
                                        codexHome,
                                        data,
                                        null,
                                        null,
                                        false));
        assertThat(second.restoredSessions()).isZero();
        assertThat(second.skippedSessions()).isEqualTo(1);
    }

    @Test
    void restoresCompleteCodexRootGraphWhenNestedSubagentIsRequested() throws Exception {
        String rootId = "019f5952-14cd-7d21-a7f7-87b31cb62da6";
        String childId = "019f608e-7cb4-76f0-b006-89a317303fdb";
        String grandchildId = "019f608f-1d79-77f0-9e61-4b25cae10e5c";
        SessionKey rootKey = new SessionKey("codex", rootId);
        SessionKey childKey = new SessionKey("codex", childId);
        SessionKey grandchildKey = new SessionKey("codex", grandchildId);
        ChatSession root = session(rootKey, "Visible root", tempDir.toString());
        String subagentSource =
                "{\"thread_spawn\":{\"parent_thread_id\":\""
                        + rootId
                        + "\",\"depth\":1,\"agent_path\":\"/root/reviewer\","
                        + "\"agent_nickname\":\"Dirac\",\"agent_role\":\"reviewer\"}}";
        ChatSession child =
                session(childKey, "Internal reviewer", tempDir.toString())
                        .withSubagentSourceJson(subagentSource);
        String nestedSubagentSource =
                "{\"thread_spawn\":{\"parent_thread_id\":\""
                        + childId
                        + "\",\"depth\":2,\"agent_path\":\"/root/reviewer/audit\","
                        + "\"agent_nickname\":\"Noether\",\"agent_role\":\"auditor\"}}";
        ChatSession grandchild =
                session(grandchildKey, "Nested audit", tempDir.toString())
                        .withSubagentSourceJson(nestedSubagentSource);
        ChatMessage rootPrompt =
                message(
                        rootKey,
                        "root-prompt",
                        1L,
                        "user",
                        "response_item",
                        codexPrompt("root prompt"),
                        Collections.emptyList());
        ChatMessage childPrompt =
                message(
                        childKey,
                        "child-prompt",
                        1L,
                        "user",
                        "response_item",
                        codexPrompt("child prompt"),
                        Collections.emptyList());
        ChatMessage grandchildPrompt =
                message(
                        grandchildKey,
                        "grandchild-prompt",
                        1L,
                        "user",
                        "response_item",
                        codexPrompt("grandchild prompt"),
                        Collections.emptyList());
        Path codexHome = tempDir.resolve("subagent-codex-home");
        initializeCodexState(codexHome);
        String unrelatedIndexLine =
                "{\"id\":\"unrelated\",\"thread_name\":\"Keep me\","
                        + "\"updated_at\":\"2025-12-31T00:00:00Z\"}";
        Files.writeString(
                codexHome.resolve("session_index.jsonl"),
                unrelatedIndexLine + "\n",
                StandardCharsets.UTF_8);

        RestoreSummary summary =
                new RestoreService(
                                new FakeRepository(
                                        java.util.Arrays.asList(root, child, grandchild),
                                        rootPrompt,
                                        childPrompt,
                                        grandchildPrompt),
                                mapper)
                        .restore(
                                new RestoreOptions(
                                        RestoreType.CODEX,
                                        codexHome,
                                        tempDir.resolve("subagent-data"),
                                        null,
                                        grandchildId,
                                        false));

        assertThat(summary.restoredSessions()).isEqualTo(3);
        assertThat(summary.restoredMessages()).isEqualTo(3);
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + codexHome.resolve("state_5.sqlite"));
                java.sql.PreparedStatement statement =
                        connection.prepareStatement(
                                "SELECT source, thread_source, agent_path, agent_nickname, "
                                        + "agent_role FROM threads WHERE id = ?")) {
            statement.setString(1, childId);
            try (ResultSet row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                assertThat(mapper.readTree(row.getString("source")).path("subagent"))
                        .isEqualTo(mapper.readTree(subagentSource));
                assertThat(row.getString("thread_source")).isEqualTo("subagent");
                assertThat(row.getString("agent_path")).isEqualTo("/root/reviewer");
                assertThat(row.getString("agent_nickname")).isEqualTo("Dirac");
                assertThat(row.getString("agent_role")).isEqualTo("reviewer");
            }
        }
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + codexHome.resolve("state_5.sqlite"));
                java.sql.PreparedStatement statement =
                        connection.prepareStatement(
                                "SELECT parent_thread_id, status FROM thread_spawn_edges "
                                        + "WHERE child_thread_id = ?")) {
            statement.setString(1, childId);
            try (ResultSet row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                assertThat(row.getString("parent_thread_id")).isEqualTo(rootId);
                assertThat(row.getString("status")).isEqualTo("open");
            }
        }

        Path childRollout;
        try (Stream<Path> files = Files.walk(codexHome.resolve("sessions"))) {
            childRollout =
                    files.filter(path -> path.getFileName().toString().endsWith(childId + ".jsonl"))
                            .findFirst()
                            .orElseThrow();
        }
        JsonNode childMeta =
                mapper.readTree(Files.readAllLines(childRollout, StandardCharsets.UTF_8).get(0));
        assertThat(childMeta.path("payload").path("source").path("subagent"))
                .isEqualTo(mapper.readTree(subagentSource));
        assertThat(childMeta.path("payload").path("thread_source").asText())
                .isEqualTo("subagent");
        assertThat(childMeta.path("payload").path("session_id").asText()).isEqualTo(rootId);
        assertThat(childMeta.path("payload").path("multi_agent_version").asText())
                .isEqualTo("v2");
        UUID childWindow =
                UUID.fromString(
                        childMeta
                                .path("payload")
                                .path("context_window")
                                .path("window_id")
                                .asText());
        assertThat(childWindow.version()).isEqualTo(7);
        assertThat(childWindow.variant()).isEqualTo(2);
        assertThat(childMeta.path("payload").path("parent_thread_id").asText())
                .isEqualTo(rootId);

        Path rootRollout;
        Path grandchildRollout;
        try (Stream<Path> files = Files.walk(codexHome.resolve("sessions"))) {
            List<Path> rollouts = files.filter(Files::isRegularFile).collect(Collectors.toList());
            rootRollout =
                    rollouts.stream()
                            .filter(path -> path.getFileName().toString().endsWith(rootId + ".jsonl"))
                            .findFirst()
                            .orElseThrow();
            grandchildRollout =
                    rollouts.stream()
                            .filter(
                                    path ->
                                            path.getFileName()
                                                    .toString()
                                                    .endsWith(grandchildId + ".jsonl"))
                            .findFirst()
                            .orElseThrow();
        }
        JsonNode rootMeta =
                mapper.readTree(Files.readAllLines(rootRollout, StandardCharsets.UTF_8).get(0));
        assertThat(rootMeta.path("payload").path("session_id").asText()).isEqualTo(rootId);
        assertThat(rootMeta.path("payload").path("multi_agent_version").asText())
                .isEqualTo("v2");
        assertThat(
                        UUID.fromString(
                                        rootMeta
                                                .path("payload")
                                                .path("context_window")
                                                .path("window_id")
                                                .asText())
                                .version())
                .isEqualTo(7);
        JsonNode grandchildMeta =
                mapper.readTree(
                        Files.readAllLines(grandchildRollout, StandardCharsets.UTF_8).get(0));
        assertThat(grandchildMeta.path("payload").path("session_id").asText())
                .isEqualTo(rootId);
        assertThat(grandchildMeta.path("payload").path("parent_thread_id").asText())
                .isEqualTo(childId);
        assertThat(grandchildMeta.path("payload").path("multi_agent_version").asText())
                .isEqualTo("v2");

        List<String> sessionIndex =
                Files.readAllLines(codexHome.resolve("session_index.jsonl"), StandardCharsets.UTF_8);
        assertThat(sessionIndex).hasSize(2);
        assertThat(sessionIndex.get(0)).isEqualTo(unrelatedIndexLine);
        List<JsonNode> indexEntries = new ArrayList<>();
        for (String line : sessionIndex) {
            indexEntries.add(mapper.readTree(line));
        }
        assertThat(indexEntries)
                .extracting(entry -> entry.path("id").asText())
                .containsExactly("unrelated", rootId)
                .doesNotContain(childId, grandchildId);
        assertThat(indexEntries.get(1).path("thread_name").asText()).isEqualTo("Visible root");
    }

    @Test
    void skipsPendingCodexBranchWithoutRestoringOrphanDescendants() throws Exception {
        String rootId = "019f6100-0000-7000-8000-000000000001";
        String childId = "019f6100-0000-7000-8000-000000000002";
        String grandchildId = "019f6100-0000-7000-8000-000000000003";
        SessionKey rootKey = new SessionKey("codex", rootId);
        ChatSession root = session(rootKey, "Stable root", tempDir.toString());
        ChatSession pendingChild =
                session(new SessionKey("codex", childId), "Pending child", tempDir.toString())
                        .withSubagentSourceJson(
                                "{\"thread_spawn\":{\"parent_thread_id\":\""
                                        + rootId
                                        + "\",\"depth\":1}}")
                        .withPendingBoundary(9L, "cursor-pending");
        ChatSession stableGrandchild =
                session(
                                new SessionKey("codex", grandchildId),
                                "Stable grandchild",
                                tempDir.toString())
                        .withSubagentSourceJson(
                                "{\"thread_spawn\":{\"parent_thread_id\":\""
                                        + childId
                                        + "\",\"depth\":2}}");
        ChatMessage rootPrompt =
                message(
                        rootKey,
                        "root-only",
                        1L,
                        "user",
                        "response_item",
                        codexPrompt("stable root"),
                        Collections.emptyList());
        Path codexHome = tempDir.resolve("pending-branch-codex-home");
        initializeCodexState(codexHome);

        RestoreSummary summary =
                new RestoreService(
                                new FakeRepository(
                                        java.util.Arrays.asList(
                                                root, pendingChild, stableGrandchild),
                                        rootPrompt),
                                mapper)
                        .restore(
                                new RestoreOptions(
                                        RestoreType.CODEX,
                                        codexHome,
                                        tempDir.resolve("pending-branch-data"),
                                        null,
                                        rootId,
                                        false));

        assertThat(summary.restoredSessions()).isEqualTo(1);
        assertThat(summary.restoredMessages()).isEqualTo(1);
        assertThat(summary.skippedSessions()).isEqualTo(2);
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + codexHome.resolve("state_5.sqlite"));
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT id FROM threads")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo(rootId);
            assertThat(rows.next()).isFalse();
        }
    }

    @Test
    void restoresClaudeProjectTranscriptAndRepairsBrokenParentChain() throws Exception {
        String sessionId = UUID.randomUUID().toString();
        String firstUuid = UUID.randomUUID().toString();
        String secondUuid = UUID.randomUUID().toString();
        SessionKey key = new SessionKey("claude", sessionId);
        ChatSession session = session(key, "Restored Claude title", "/remote/project");
        ChatMessage first =
                message(
                        key,
                        "m1",
                        10L,
                        "user",
                        "user",
                        "{\"type\":\"user\",\"uuid\":\""
                                + firstUuid
                                + "\",\"parentUuid\":null,\"sessionId\":\""
                                + sessionId
                                + "\",\"cwd\":\"/remote/project\","
                                + "\"message\":{\"role\":\"user\",\"content\":\"hello\"}}",
                        Collections.emptyList());
        ChatMessage second =
                message(
                        key,
                        "m2",
                        20L,
                        "assistant",
                        "assistant",
                        "{\"type\":\"assistant\",\"uuid\":\""
                                + secondUuid
                                + "\",\"parentUuid\":\"missing-event\",\"sessionId\":\""
                                + sessionId
                                + "\",\"cwd\":\"/remote/project\","
                                + "\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\","
                                + "\"text\":\"hi\"}]}}",
                        Collections.emptyList());
        FakeRepository repository = new FakeRepository(session, second, first);
        Path claudeHome = tempDir.resolve("claude-home");
        Path project = Files.createDirectories(tempDir.resolve("target-project"));

        RestoreSummary summary =
                new RestoreService(repository, mapper)
                        .restore(
                                new RestoreOptions(
                                        RestoreType.CLAUDE,
                                        claudeHome,
                                        tempDir.resolve("data"),
                                        project,
                                        null,
                                        false));

        assertThat(summary.restoredSessions()).isEqualTo(1);
        Path transcript =
                claudeHome
                        .resolve("projects")
                        .resolve(
                                ClaudeFormatRestorer.sanitizeProjectDirectory(
                                        project.toRealPath().toString()))
                        .resolve(sessionId + ".jsonl");
        assertThat(transcript).isRegularFile();
        List<String> lines = Files.readAllLines(transcript, StandardCharsets.UTF_8);
        assertThat(lines).hasSize(3);
        JsonNode restoredFirst = mapper.readTree(lines.get(0));
        JsonNode restoredSecond = mapper.readTree(lines.get(1));
        JsonNode title = mapper.readTree(lines.get(2));
        assertThat(restoredFirst.path("uuid").asText()).isEqualTo(firstUuid);
        assertThat(restoredFirst.path("cwd").asText()).isEqualTo(project.toRealPath().toString());
        assertThat(restoredSecond.path("parentUuid").asText()).isEqualTo(firstUuid);
        assertThat(restoredSecond.path("isSidechain").asBoolean()).isFalse();
        assertThat(title.path("type").asText()).isEqualTo("custom-title");
        assertThat(title.path("customTitle").asText()).isEqualTo("Restored Claude title");
    }

    @Test
    void keepsCancelledCodexUserInputAsItsOwnTurn() throws Exception {
        String sessionId = "cancelled-turn-session";
        SessionKey key = new SessionKey("codex", sessionId);
        ChatSession session = session(key, "Cancelled turn", tempDir.toString());
        ChatMessage cancelled =
                message(
                        key,
                        "cancelled",
                        1L,
                        "user",
                        "response_item",
                        "{\"timestamp\":\"2026-01-01T00:00:00Z\","
                                + "\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                                + "\"role\":\"user\",\"content\":[{\"type\":\"input_text\","
                                + "\"text\":\"first cancelled prompt\"}]}}",
                        Collections.emptyList());
        ChatMessage next =
                message(
                        key,
                        "next",
                        2L,
                        "user",
                        "response_item",
                        "{\"timestamp\":\"2026-01-01T00:01:00Z\","
                                + "\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                                + "\"role\":\"user\",\"content\":[{\"type\":\"input_text\","
                                + "\"text\":\"second prompt\"}]}}",
                        Collections.emptyList());
        ChatMessage answer =
                message(
                        key,
                        "answer",
                        3L,
                        "assistant",
                        "response_item",
                        "{\"timestamp\":\"2026-01-01T00:01:01Z\","
                                + "\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                                + "\"role\":\"assistant\",\"phase\":\"final_answer\","
                                + "\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}",
                        Collections.emptyList());
        Path codexHome = tempDir.resolve("cancelled-codex-home");
        initializeCodexState(codexHome);

        new RestoreService(new FakeRepository(session, answer, next, cancelled), mapper)
                .restore(
                        new RestoreOptions(
                                RestoreType.CODEX,
                                codexHome,
                                tempDir.resolve("cancelled-data"),
                                null,
                                null,
                                false));

        List<JsonNode> events = readOnlyCodexRollout(codexHome);
        List<String> visibleUsers =
                events.stream()
                        .filter(node -> "event_msg".equals(node.path("type").asText()))
                        .filter(
                                node ->
                                        "user_message"
                                                .equals(
                                                        node.path("payload")
                                                                .path("type")
                                                                .asText()))
                        .map(node -> node.path("payload").path("message").asText())
                        .collect(Collectors.toList());
        assertThat(visibleUsers)
                .containsExactly("first cancelled prompt", "second prompt");
        assertThat(
                        events.stream()
                                .filter(
                                        node ->
                                                "task_started"
                                                        .equals(
                                                                node.path("payload")
                                                                        .path("type")
                                                                        .asText()))
                                .count())
                .isEqualTo(2L);
        assertThat(
                        events.stream()
                                .filter(
                                        node ->
                                                "task_complete"
                                                        .equals(
                                                                node.path("payload")
                                                                        .path("type")
                                                                        .asText()))
                                .count())
                .isEqualTo(2L);
    }

    @Test
    void mapsCodexCwdToExplicitProjectAndUsesIsolatedFallback() throws Exception {
        Path explicitHome = tempDir.resolve("explicit-codex-home");
        initializeCodexState(explicitHome);
        Path project = Files.createDirectories(tempDir.resolve("explicit-project"));
        SessionKey explicitKey = new SessionKey("codex", "explicit-cwd-session");
        ChatSession explicitSession = session(explicitKey, "explicit", "/remote/missing");

        new RestoreService(new FakeRepository(explicitSession), mapper)
                .restore(
                        new RestoreOptions(
                                RestoreType.CODEX,
                                explicitHome,
                                tempDir.resolve("explicit-data"),
                                project,
                                null,
                                false));
        assertThat(threadCwd(explicitHome, explicitKey.sessionId()))
                .isEqualTo(project.toRealPath().toString());

        Path fallbackRoot = Files.createDirectories(tempDir.resolve("fallback-root"));
        Path fallbackHome = fallbackRoot.resolve("codex-home");
        initializeCodexState(fallbackHome);
        SessionKey fallbackKey = new SessionKey("codex", "fallback-cwd-session");
        ChatSession fallbackSession = session(fallbackKey, "fallback", "/remote/missing");
        new RestoreService(new FakeRepository(fallbackSession), mapper)
                .restore(
                        new RestoreOptions(
                                RestoreType.CODEX,
                                fallbackHome,
                                tempDir.resolve("fallback-data"),
                                null,
                                null,
                                false));
        assertThat(threadCwd(fallbackHome, fallbackKey.sessionId()))
                .isEqualTo(fallbackRoot.toRealPath().toString());
    }

    @Test
    void rejectsMissingExplicitRestoreProjects() throws Exception {
        Path codexHome = tempDir.resolve("missing-project-codex-home");
        initializeCodexState(codexHome);
        Path missing = tempDir.resolve("does-not-exist");

        assertThatThrownBy(() -> new CodexFormatRestorer(codexHome, missing, mapper))
                .hasMessageContaining("not an existing directory");
        Path claudeHome = tempDir.resolve("missing-project-claude-home");
        assertThatThrownBy(() -> new ClaudeFormatRestorer(claudeHome, missing, mapper))
                .hasMessageContaining("not an existing directory");
        assertThat(claudeHome).doesNotExist();
    }

    @Test
    void rejectsCodexTargetAndStateDatabaseSymbolicLinks() throws Exception {
        Path realHome = tempDir.resolve("real-codex-home");
        initializeCodexState(realHome);
        Path homeLink = tempDir.resolve("linked-codex-home");
        createSymbolicLinkOrSkip(homeLink, realHome);
        assertThatThrownBy(() -> new CodexFormatRestorer(homeLink, mapper))
                .hasMessageContaining("symbolic link");

        Path linkedStateHome = Files.createDirectories(tempDir.resolve("linked-state-home"));
        Files.createSymbolicLink(
                linkedStateHome.resolve("state_5.sqlite"), realHome.resolve("state_5.sqlite"));
        assertThatThrownBy(() -> new CodexFormatRestorer(linkedStateHome, mapper))
                .hasMessageContaining("symbolic link");

        Path linkedSidecarHome = tempDir.resolve("linked-sidecar-home");
        initializeCodexState(linkedSidecarHome);
        Path outsideWal = Files.write(tempDir.resolve("outside-state-wal"), new byte[0]);
        Files.createSymbolicLink(
                linkedSidecarHome.resolve("state_5.sqlite-wal"), outsideWal);
        assertThatThrownBy(() -> new CodexFormatRestorer(linkedSidecarHome, mapper))
                .hasMessageContaining("sidecar")
                .hasMessageContaining("symbolic link");
    }

    @Test
    void refusesCodexOutputDirectorySymlinkWithoutWritingOutsideTarget() throws Exception {
        Path codexHome = tempDir.resolve("output-link-codex-home");
        initializeCodexState(codexHome);
        Path outside = Files.createDirectories(tempDir.resolve("outside-sessions"));
        createSymbolicLinkOrSkip(codexHome.resolve("sessions"), outside);
        SessionKey key = new SessionKey("codex", "output-link-session");
        ChatSession session = session(key, "output link", tempDir.toString());
        ChatMessage prompt =
                message(
                        key,
                        "prompt",
                        1L,
                        "user",
                        "response_item",
                        "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                                + "\"role\":\"user\",\"content\":[{\"type\":\"input_text\","
                                + "\"text\":\"hello\"}]}}",
                        Collections.emptyList());

        assertThatThrownBy(
                        () ->
                                new RestoreService(new FakeRepository(session, prompt), mapper)
                                        .restore(
                                                new RestoreOptions(
                                                        RestoreType.CODEX,
                                                        codexHome,
                                                        tempDir.resolve("output-link-data"),
                                                        null,
                                                        null,
                                                        false)))
                .hasMessageContaining("symbolic link");
        assertThat(outside).isEmptyDirectory();
    }

    @Test
    void hashesSanitizedCodexSessionComponentsToAvoidCollisions() throws Exception {
        Path codexHome = tempDir.resolve("collision-codex-home");
        initializeCodexState(codexHome);
        CodexFormatRestorer restorer = new CodexFormatRestorer(codexHome, mapper);
        ChatSession slash = session(new SessionKey("codex", "same/a"), "slash", tempDir.toString());
        ChatSession underscore =
                session(new SessionKey("codex", "same_a"), "underscore", tempDir.toString());

        assertThat(restorer.attachmentDirectory(slash))
                .isNotEqualTo(restorer.attachmentDirectory(underscore));
    }

    @Test
    void rejectsConflictingRowsWithTheSameMessageIdentity() throws Exception {
        SessionKey key = new SessionKey("claude", "conflicting-message-session");
        ChatSession session = session(key, "conflict", tempDir.toString());
        ChatMessage first =
                message(
                        key,
                        "same-message-id",
                        1L,
                        "user",
                        "user",
                        "{\"type\":\"user\",\"message\":{\"content\":\"first\"}}",
                        Collections.emptyList());
        ChatMessage conflicting =
                message(
                        key,
                        "same-message-id",
                        1L,
                        "user",
                        "user",
                        "{\"type\":\"user\",\"message\":{\"content\":\"different\"}}",
                        Collections.emptyList());
        Path project = Files.createDirectories(tempDir.resolve("conflict-project"));

        assertThatThrownBy(
                        () ->
                                new RestoreService(
                                                new FakeRepository(
                                                        session, first, conflicting),
                                                mapper)
                                        .restore(
                                                new RestoreOptions(
                                                        RestoreType.CLAUDE,
                                                        tempDir.resolve("conflict-claude-home"),
                                                        tempDir.resolve("conflict-data"),
                                                        project,
                                                        null,
                                                        false)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Conflicting duplicate restored message");
    }

    @Test
    void refusesAConversationThatChangesWhileMessagesAreBeingRead() throws Exception {
        SessionKey key = new SessionKey("claude", "moving-session");
        ChatSession initial = session(key, "initial", tempDir.toString());
        ChatSession advanced =
                initial.withCheckpoint(
                        "new-cursor", initial.lastCommitId() + 1L, Instant.now());
        ChatMessage message =
                message(
                        key,
                        "moving-message",
                        1L,
                        "user",
                        "user",
                        "{\"type\":\"user\",\"message\":{\"content\":\"moving\"}}",
                        Collections.emptyList());
        FakeRepository repository =
                new FakeRepository(initial, message).withSessionAfterMessageRead(advanced);
        Path claudeHome = tempDir.resolve("moving-claude-home");
        Path project = Files.createDirectories(tempDir.resolve("moving-project"));

        assertThatThrownBy(
                        () ->
                                new RestoreService(repository, mapper)
                                        .restore(
                                                new RestoreOptions(
                                                        RestoreType.CLAUDE,
                                                        claudeHome,
                                                        tempDir.resolve("moving-data"),
                                                        project,
                                                        null,
                                                        false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("changed while restore was reading Paimon");
        try (Stream<Path> files = Files.walk(claudeHome)) {
            assertThat(files.filter(path -> path.toString().endsWith(".jsonl"))).isEmpty();
        }
    }

    @Test
    void skipsSessionWhileItsCrossTableCommitIsPending() throws Exception {
        SessionKey key = new SessionKey("claude", UUID.randomUUID().toString());
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        ChatSession pending =
                new ChatSession(
                        key,
                        "pending",
                        tempDir.toString(),
                        false,
                        "/source/pending.jsonl",
                        "old-cursor",
                        1L,
                        2L,
                        "new-cursor",
                        now,
                        now,
                        now,
                        now);
        FakeRepository repository = new FakeRepository(pending);
        Path claudeHome = tempDir.resolve("pending-claude-home");

        RestoreSummary summary =
                new RestoreService(repository, mapper)
                        .restore(
                                new RestoreOptions(
                                        RestoreType.CLAUDE,
                                        claudeHome,
                                        tempDir.resolve("pending-data"),
                                        tempDir,
                                        null,
                                        false));

        assertThat(summary.restoredSessions()).isZero();
        assertThat(summary.skippedSessions()).isEqualTo(1);
        assertThat(repository.messageReadCalls).isZero();
        assertThat(claudeHome).doesNotExist();
    }

    @Test
    void appendsOverwrittenCodexTitleAfterRemovingStaleIndexEntries() throws Exception {
        String sessionId = "019f5952-14cd-7d21-a7f7-87b31cb62da8";
        SessionKey key = new SessionKey("codex", sessionId);
        ChatSession session = session(key, "replacement", tempDir.toString());
        ChatMessage message =
                message(
                        key,
                        "replacement-index-message",
                        1L,
                        "user",
                        "response_item",
                        "{\"timestamp\":\"2026-01-01T00:00:00Z\","
                                + "\"type\":\"response_item\",\"payload\":{"
                                + "\"type\":\"message\",\"role\":\"user\",\"content\":[{"
                                + "\"type\":\"input_text\",\"text\":\"first prompt\"}]}}",
                        Collections.emptyList());
        Path codexHome = tempDir.resolve("overwrite-index-codex-home");
        initializeCodexState(codexHome);
        String otherSession = "another-session";
        String otherIndexLine =
                "{\"id\":\""
                        + otherSession
                        + "\",\"thread_name\":\"replacement\","
                        + "\"updated_at\":\"2026-01-01T00:00:01Z\"}";
        Files.writeString(
                codexHome.resolve("session_index.jsonl"),
                "{\"id\":\""
                        + sessionId
                        + "\",\"thread_name\":\"old\"}\n"
                        + otherIndexLine
                        + "\n{\"id\":\""
                        + sessionId
                        + "\",\"thread_name\":\"stale duplicate\"}\n",
                StandardCharsets.UTF_8);
        Path oldRollout =
                Files.createDirectories(codexHome.resolve("sessions/2026/01/01"))
                        .resolve("old-index-rollout.jsonl");
        Files.writeString(oldRollout, "old rollout\n", StandardCharsets.UTF_8);
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + codexHome.resolve("state_5.sqlite"));
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO threads (id, rollout_path, created_at, updated_at, source, "
                            + "model_provider, cwd, title, sandbox_policy, approval_mode) VALUES ('"
                            + sessionId
                            + "', '"
                            + oldRollout
                            + "', 1, 1, 'vscode', 'openai', '/tmp', 'old', '{}', 'on-request')");
        }

        RestoreSummary summary =
                new RestoreService(new FakeRepository(session, message), mapper)
                        .restore(
                                new RestoreOptions(
                                        RestoreType.CODEX,
                                        codexHome,
                                        tempDir.resolve("overwrite-index-data"),
                                        null,
                                        null,
                                        true));

        assertThat(summary.restoredSessions()).isEqualTo(1);
        List<String> index =
                Files.readAllLines(codexHome.resolve("session_index.jsonl"), StandardCharsets.UTF_8);
        assertThat(index).hasSize(2);
        assertThat(index.get(0)).isEqualTo(otherIndexLine);
        assertThat(mapper.readTree(index.get(1)).path("id").asText()).isEqualTo(sessionId);
        assertThat(mapper.readTree(index.get(1)).path("thread_name").asText())
                .isEqualTo("replacement");
    }

    @Test
    void removesNewCodexTitleIndexWhenSqliteInsertFails() throws Exception {
        String sessionId = "019f5952-14cd-7d21-a7f7-87b31cb62da9";
        SessionKey key = new SessionKey("codex", sessionId);
        ChatSession session = session(key, "new title", tempDir.toString());
        ChatMessage message =
                message(
                        key,
                        "failed-index-message",
                        1L,
                        "user",
                        "response_item",
                        "{\"timestamp\":\"2026-01-01T00:00:00Z\","
                                + "\"type\":\"response_item\",\"payload\":{"
                                + "\"type\":\"message\",\"role\":\"user\",\"content\":[{"
                                + "\"type\":\"input_text\",\"text\":\"first prompt\"}]}}",
                        Collections.emptyList());
        Path codexHome = tempDir.resolve("new-index-rollback-codex-home");
        initializeCodexState(codexHome);
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + codexHome.resolve("state_5.sqlite"));
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TRIGGER reject_new_restore BEFORE INSERT ON threads "
                            + "BEGIN SELECT RAISE(FAIL, 'blocked new insert'); END");
        }

        assertThatThrownBy(
                        () ->
                                new RestoreService(
                                                new FakeRepository(session, message), mapper)
                                        .restore(
                                                new RestoreOptions(
                                                        RestoreType.CODEX,
                                                        codexHome,
                                                        tempDir.resolve("new-index-rollback-data"),
                                                        null,
                                                        null,
                                                        false)))
                .hasMessageContaining("blocked new insert");

        assertThat(codexHome.resolve("session_index.jsonl")).doesNotExist();
        try (Stream<Path> files = Files.walk(codexHome)) {
            assertThat(files.filter(path -> path.toString().endsWith(".jsonl"))).isEmpty();
        }
    }

    @Test
    void rollsBackAnExistingCodexRolloutWhenSidebarUpdateFails() throws Exception {
        String sessionId = "019f5952-14cd-7d21-a7f7-87b31cb62da7";
        SessionKey key = new SessionKey("codex", sessionId);
        ChatSession session = session(key, "replacement", tempDir.toString());
        ChatMessage message =
                message(
                        key,
                        "replacement-message",
                        1L,
                        "user",
                        "response_item",
                        "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                                + "\"role\":\"user\",\"content\":[{\"type\":\"input_text\","
                                + "\"text\":\"/source/report.txt\"}]},"
                                + "\"_paimon_attachments\":[{\"index\":0,"
                                + "\"source_kind\":\"local_path\","
                                + "\"source_reference\":\"/source/report.txt\","
                                + "\"file_name\":\"report.txt\"}]}",
                        Collections.singletonList(
                                AttachmentPayload.of(
                                        "attachment".getBytes(StandardCharsets.UTF_8))));
        Path codexHome = tempDir.resolve("rollback-codex-home");
        initializeCodexState(codexHome);
        String originalSessionIndex =
                "{\"id\":\"unrelated\",\"thread_name\":\"Keep me\","
                        + "\"updated_at\":\"2025-12-31T00:00:00Z\"}\n"
                        + "{\"id\":\""
                        + sessionId
                        + "\",\"thread_name\":\"old\","
                        + "\"updated_at\":\"2026-01-01T00:00:00Z\"}\n";
        Files.writeString(
                codexHome.resolve("session_index.jsonl"),
                originalSessionIndex,
                StandardCharsets.UTF_8);
        Path oldRollout = Files.createDirectories(codexHome.resolve("sessions/2026/01/01"))
                .resolve("old-rollout.jsonl");
        Files.writeString(oldRollout, "old rollout\n", StandardCharsets.UTF_8);
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + codexHome.resolve("state_5.sqlite"));
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO threads (id, rollout_path, created_at, updated_at, source, "
                            + "model_provider, cwd, title, sandbox_policy, approval_mode) VALUES ('"
                            + sessionId
                            + "', '"
                            + oldRollout
                            + "', 1, 1, 'vscode', 'openai', '/tmp', 'old', '{}', 'on-request')");
            statement.execute(
                    "CREATE TRIGGER reject_restore BEFORE UPDATE ON threads "
                            + "BEGIN SELECT RAISE(FAIL, 'blocked by test'); END");
        }

        assertThatThrownBy(
                        () ->
                                new RestoreService(
                                                new FakeRepository(session, message), mapper)
                                        .restore(
                                                new RestoreOptions(
                                                        RestoreType.CODEX,
                                                        codexHome,
                                                        tempDir.resolve("rollback-data"),
                                                        null,
                                                        null,
                                                        true)))
                .hasMessageContaining("blocked by test");

        assertThat(Files.readString(oldRollout, StandardCharsets.UTF_8))
                .isEqualTo("old rollout\n");
        assertThat(
                        Files.readString(
                                codexHome.resolve("session_index.jsonl"),
                                StandardCharsets.UTF_8))
                .isEqualTo(originalSessionIndex);
        try (Stream<Path> attachments =
                Files.walk(codexHome.resolve("attachments/restored"))) {
            assertThat(attachments.filter(Files::isRegularFile)).isEmpty();
        }
    }

    private List<JsonNode> readOnlyCodexRollout(Path codexHome) throws Exception {
        List<Path> rollouts;
        try (Stream<Path> files = Files.walk(codexHome.resolve("sessions"))) {
            rollouts =
                    files.filter(path -> path.toString().endsWith(".jsonl"))
                            .collect(Collectors.toList());
        }
        assertThat(rollouts).hasSize(1);
        List<JsonNode> events = new ArrayList<>();
        for (String line : Files.readAllLines(rollouts.get(0), StandardCharsets.UTF_8)) {
            events.add(mapper.readTree(line));
        }
        return events;
    }

    private static String threadCwd(Path codexHome, String sessionId) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + codexHome.resolve("state_5.sqlite"));
                java.sql.PreparedStatement statement =
                        connection.prepareStatement("SELECT cwd FROM threads WHERE id = ?")) {
            statement.setString(1, sessionId);
            try (ResultSet rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getString(1);
            }
        }
    }

    private static void createSymbolicLinkOrSkip(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
        } catch (IOException | UnsupportedOperationException e) {
            Assumptions.assumeTrue(false, "Symbolic links are unavailable: " + e.getMessage());
        }
    }

    private static ChatSession session(SessionKey key, String title, String cwd) {
        Instant created = Instant.parse("2026-01-01T00:00:00Z");
        return new ChatSession(
                key,
                title,
                cwd,
                false,
                "/source/" + key.sessionId() + ".jsonl",
                "cursor",
                1L,
                created,
                created.plusSeconds(60),
                created.plusSeconds(60),
                created.plusSeconds(61));
    }

    private static String codexPrompt(String text) {
        return "{\"timestamp\":\"2026-01-01T00:00:00Z\","
                + "\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                + "\"role\":\"user\",\"content\":[{\"type\":\"input_text\","
                + "\"text\":\""
                + text
                + "\"}]}}";
    }

    private static void initializeCodexState(Path codexHome) throws Exception {
        Files.createDirectories(codexHome);
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:sqlite:" + codexHome.resolve("state_5.sqlite"));
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE _sqlx_migrations (version INTEGER PRIMARY KEY)");
            statement.execute(
                    "CREATE TABLE threads ("
                            + "id TEXT PRIMARY KEY, rollout_path TEXT NOT NULL, "
                            + "created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, "
                            + "source TEXT NOT NULL, model_provider TEXT NOT NULL, cwd TEXT NOT NULL, "
                            + "title TEXT NOT NULL, sandbox_policy TEXT NOT NULL, "
                            + "approval_mode TEXT NOT NULL, preview TEXT NOT NULL DEFAULT '', "
                            + "first_user_message TEXT NOT NULL DEFAULT '', "
                            + "thread_source TEXT, agent_path TEXT, agent_nickname TEXT, "
                            + "agent_role TEXT)");
            statement.execute(
                    "CREATE TABLE thread_spawn_edges ("
                            + "parent_thread_id TEXT NOT NULL, child_thread_id TEXT NOT NULL "
                            + "PRIMARY KEY, status TEXT NOT NULL)");
        }
    }

    private static ChatMessage message(
            SessionKey key,
            String id,
            long sequence,
            String role,
            String eventType,
            String content,
            List<AttachmentPayload> attachments) {
        return new ChatMessage(
                id,
                key,
                sequence,
                role,
                eventType,
                content,
                attachments,
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:01:00Z"));
    }

    private static final class FakeRepository implements ChatRepository {
        private final Map<SessionKey, ChatSession> sessions = new LinkedHashMap<>();
        private final List<ChatMessage> messages = new ArrayList<>();
        private int messageReadCalls;
        private ChatSession sessionAfterMessageRead;

        private FakeRepository(ChatSession session, ChatMessage... messages) {
            this.sessions.put(session.key(), session);
            Collections.addAll(this.messages, messages);
        }

        private FakeRepository(List<ChatSession> sessions, ChatMessage... messages) {
            for (ChatSession session : sessions) {
                this.sessions.put(session.key(), session);
            }
            Collections.addAll(this.messages, messages);
        }

        @Override
        public void initialize() {}

        @Override
        public Map<SessionKey, ChatSession> loadSessions() {
            if (messageReadCalls > 0 && sessionAfterMessageRead != null) {
                return Collections.singletonMap(
                        sessionAfterMessageRead.key(), sessionAfterMessageRead);
            }
            return sessions;
        }

        private FakeRepository withSessionAfterMessageRead(ChatSession value) {
            this.sessionAfterMessageRead = value;
            return this;
        }

        @Override
        public void forEachMessage(
                String sourceType, Set<String> sessionIds, ChatMessageConsumer consumer)
                throws Exception {
            messageReadCalls++;
            for (ChatMessage message : messages) {
                if (sourceType.equals(message.sessionKey().sourceType())
                        && sessionIds.contains(message.sessionKey().sessionId())) {
                    consumer.accept(message);
                }
            }
        }

        @Override
        public void commit(long commitIdentifier, List<SessionBatch> batches) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
