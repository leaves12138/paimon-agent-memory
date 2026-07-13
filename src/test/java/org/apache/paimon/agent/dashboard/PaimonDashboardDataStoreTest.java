package org.apache.paimon.agent.dashboard;

import org.apache.paimon.agent.config.AgentConfiguration;
import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.config.SourceConfig;
import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.sink.PaimonChatRepository;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonDashboardDataStoreTest {

    @TempDir Path tempDir;

    @Test
    void readsBoundedPagesDetailsAndAttachmentsWithoutBlobReadsInLists() throws Exception {
        AgentConfiguration configuration = configuration();
        Instant first = Instant.parse("2026-01-01T00:00:00.123Z");
        Instant second = Instant.parse("2026-01-02T00:00:00.456Z");
        SessionKey codexKey = new SessionKey("codex", "codex-session");
        SessionKey claudeKey = new SessionKey("claude", "claude-session");
        byte[] image = new byte[] {1, 2, 3, 4};
        String contentJson =
                "{\"payload\":{\"text\":\"hello full-search needle\"},"
                        + "\"_paimon_attachments\":[{\"index\":0,\"file_name\":\"tiny.png\","
                        + "\"mime_type\":\"image/png\",\"status\":\"stored\",\"size\":4,"
                        + "\"sha256\":\"digest\"}]}";
        ChatMessage imageMessage =
                new ChatMessage(
                        "message-image",
                        codexKey,
                        10L,
                        "user",
                        "message",
                        contentJson,
                        Collections.singletonList(AttachmentPayload.of(image)),
                        second,
                        second);
        ChatMessage assistantMessage =
                new ChatMessage(
                        "message-answer",
                        codexKey,
                        11L,
                        "assistant",
                        "message",
                        "{\"payload\":{\"text\":\"answer\"}}",
                        Collections.emptyList(),
                        second.plusSeconds(1),
                        second.plusSeconds(1));
        ChatSession codex = session(codexKey, "Customer demo", false, second);
        ChatSession claude = session(claudeKey, "Archived chat", true, first);

        try (PaimonChatRepository repository = new PaimonChatRepository(configuration)) {
            repository.initialize();
            repository.commit(
                    0L,
                    Arrays.asList(
                            new SessionBatch(
                                    codex, Arrays.asList(imageMessage, assistantMessage)),
                            new SessionBatch(claude, Collections.emptyList())));

            try (PaimonDashboardDataStore store =
                    new PaimonDashboardDataStore(repository, 100)) {
                DashboardOverview overview = store.overview();
                assertThat(overview.getSessionCount()).isEqualTo(2L);
                assertThat(overview.getMessageCount()).isEqualTo(2L);
                assertThat(overview.getActiveSessionCount()).isEqualTo(1L);
                assertThat(overview.getArchivedSessionCount()).isEqualTo(1L);
                assertThat(overview.getPendingSessionCount()).isZero();
                assertThat(overview.getSessionCountBySource())
                        .containsEntry("codex", 1L)
                        .containsEntry("claude", 1L);
                assertThat(overview.getMessageCountBySource()).containsOnlyKeys("codex");
                assertThat(overview.getLastIngestedAt()).isAfter(second.plusSeconds(1));

                DashboardPage<DashboardSession> sessionPage =
                        store.listSessions(new SessionQuery(null, null, null, 1, 1));
                assertThat(sessionPage.getTotal()).isEqualTo(2L);
                assertThat(sessionPage.getItems())
                        .extracting(DashboardSession::getSessionId)
                        .containsExactly("codex-session");
                assertThat(sessionPage.isHasMore()).isTrue();
                assertThat(
                                store.listSessions(
                                                new SessionQuery(
                                                        "claude", "ARCHIVED", true, 1, 10))
                                        .getItems())
                        .extracting(DashboardSession::getSessionId)
                        .containsExactly("claude-session");

                DashboardPage<DashboardMessage> messages =
                        store.listMessages(
                                new MessageQuery(
                                        "codex",
                                        "codex-session",
                                        "user",
                                        "message",
                                        "NEEDLE",
                                        1,
                                        10));
                assertThat(messages.getTotal()).isEqualTo(1L);
                assertThat(messages.getItems().get(0).getMessageId())
                        .isEqualTo("message-image");
                assertThat(messages.getItems().get(0).getContentPreview()).contains("needle");

                DashboardMessageDetail detail =
                        store.messageDetail("codex", "codex-session", "message-image", 10L)
                                .orElseThrow(AssertionError::new);
                assertThat(detail.getContentJson()).isEqualTo(contentJson);
                assertThat(detail.getAttachments()).hasSize(1);
                DashboardAttachment attachment = detail.getAttachments().get(0);
                assertThat(attachment.isPresent()).isTrue();
                assertThat(attachment.getSize()).isEqualTo(4L);
                assertThat(attachment.getMimeType()).isEqualTo("image/png");
                assertThat(attachment.getFileName()).isEqualTo("tiny.png");

                AttachmentData data =
                        store.attachment(
                                        "codex",
                                        "codex-session",
                                        "message-image",
                                        10L,
                                        0,
                                        4L)
                                .orElseThrow(AssertionError::new);
                assertThat(data.getBytes()).isEqualTo(image);
                assertThat(data.getMimeType()).isEqualTo("image/png");
                assertThat(data.getFileName()).isEqualTo("tiny.png");
                assertThat(
                                store.attachment(
                                        "codex",
                                        "codex-session",
                                        "message-image",
                                        10L,
                                        1,
                                        4L))
                        .isEmpty();
                assertThatThrownBy(
                                () ->
                                        store.attachment(
                                                "codex",
                                                "codex-session",
                                                "message-image",
                                                10L,
                                                0,
                                                3L))
                        .isInstanceOf(DashboardAttachmentTooLargeException.class)
                        .hasMessageContaining("4 bytes");

                List<Path> blobFiles;
                try (Stream<Path> paths = Files.walk(tempDir)) {
                    blobFiles =
                            paths.filter(Files::isRegularFile)
                                    .filter(path -> path.getFileName().toString().endsWith(".blob"))
                                    .collect(Collectors.toList());
                }
                assertThat(blobFiles).isNotEmpty();
                for (Path blobFile : blobFiles) {
                    Files.move(blobFile, blobFile.resolveSibling(blobFile.getFileName() + ".offline"));
                }

                // Overview and list projections omit ARRAY<BLOB>, so missing blob files do not
                // prevent browsing the two tables.
                assertThat(store.overview().getMessageCount()).isEqualTo(2L);
                assertThat(
                                store.listMessages(
                                                new MessageQuery(
                                                        null, null, null, null, null, 1, 10))
                                        .getTotal())
                        .isEqualTo(2L);
            }

            try (PaimonDashboardDataStore bounded =
                    new PaimonDashboardDataStore(repository, 1)) {
                DashboardOverview boundedOverview = bounded.overview();
                assertThat(boundedOverview.getSessionCount()).isEqualTo(1L);
                assertThat(boundedOverview.getMessageCount()).isEqualTo(1L);
                assertThat(boundedOverview.isTruncated()).isTrue();
                assertThat(boundedOverview.isSessionCountTruncated()).isTrue();
                assertThat(boundedOverview.isMessageCountTruncated()).isTrue();
                DashboardPage<DashboardMessage> boundedMessages =
                        bounded.listMessages(
                                new MessageQuery(
                                        null, null, null, null, null, 1, 1));
                assertThat(boundedMessages.getItems()).hasSize(1);
                assertThat(boundedMessages.isTruncated()).isTrue();
            }
        }
    }

    @Test
    void validatesPaginationAndClosedState() throws Exception {
        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
            PaimonDashboardDataStore store = new PaimonDashboardDataStore(repository, 10);
            assertThatThrownBy(
                            () ->
                                    store.listSessions(
                                            new SessionQuery(null, null, null, 0, 10)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("page");
            assertThatThrownBy(
                            () ->
                                    store.listMessages(
                                            new MessageQuery(
                                                    null, null, null, null, null, 1, 11)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("pageSize");
            store.close();
            assertThatThrownBy(store::overview)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
        }
    }

    private static ChatSession session(
            SessionKey key, String title, boolean archived, Instant time) {
        return new ChatSession(
                key,
                title,
                "/tmp/" + key.sessionId(),
                archived,
                "/tmp/" + key.sessionId() + ".jsonl",
                "byte:100",
                -1L,
                time,
                time,
                time,
                time);
    }

    private AgentConfiguration configuration() {
        Map<String, String> catalog = new LinkedHashMap<>();
        catalog.put("metastore", "filesystem");
        catalog.put("warehouse", tempDir.toString());
        ProjectConfig project =
                new ProjectConfig(
                        "ai_memory",
                        "ai_chat_sessions",
                        "ai_chat_messages",
                        Duration.ofMinutes(5),
                        Duration.ofMinutes(5),
                        false,
                        "dashboard-test",
                        new SourceConfig(false, tempDir),
                        new SourceConfig(false, tempDir),
                        true,
                        false,
                        1024 * 1024,
                        100,
                        100,
                        0,
                        Duration.ofSeconds(1));
        return new AgentConfiguration(catalog, project);
    }
}
