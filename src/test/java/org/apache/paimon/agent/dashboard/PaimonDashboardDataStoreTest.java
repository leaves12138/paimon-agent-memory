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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
                "{\"payload\":{\"text\":\"hello full-search needle "
                        + "x".repeat(500)
                        + "\"},"
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
        ChatSession codex =
                session(codexKey, "Customer demo", false, second).withProjectless(true);
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
                assertThat(sessionPage.getItems())
                        .extracting(DashboardSession::getProjectless)
                        .containsExactly(true);
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
                assertThat(messages.getItems().get(0).getContentPreview().length())
                        .isGreaterThan(240);

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
                DashboardPage<DashboardMessage> filteredMessages =
                        bounded.listMessages(
                                new MessageQuery(
                                        "codex",
                                        "codex-session",
                                        "user",
                                        "message",
                                        null,
                                        1,
                                        1));
                assertThat(filteredMessages.getItems())
                        .extracting(DashboardMessage::getMessageId)
                        .containsExactly("message-image");
                assertThat(filteredMessages.isTruncated()).isFalse();
            }
        }
    }

    @Test
    void filtersConversationalMessagesInExecutionAndKeepsMessageCachesIsolated()
            throws Exception {
        Instant time = Instant.parse("2026-01-04T00:00:00Z");
        SessionKey key = new SessionKey("claude", "conversation-only-session");
        String pureToolUse =
                "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\","
                        + "\"content\":[{\"type\":\"tool_use\",\"name\":\"Bash\","
                        + "\"input\":{\"command\":\"pwd\"}}]}}";
        String pureToolResult =
                "{\"type\":\"user\",\"message\":{\"role\":\"user\","
                        + "\"content\":[{\"type\":\"tool_result\","
                        + "\"content\":\"command finished\"}]}}";
        String mixed =
                "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\","
                        + "\"content\":[{\"type\":\"text\",\"text\":\"human answer\"},"
                        + "{\"type\":\"tool_use\",\"name\":\"Bash\","
                        + "\"input\":{\"command\":\"pwd\"}}]}}";
        List<ChatMessage> messages =
                Arrays.asList(
                        message("user-message", key, 1L, "user", "message", time),
                        message("assistant-message", key, 2L, "assistant", null, time),
                        message("assistant-event", key, 3L, "assistant", "assistant", time),
                        message("user-event", key, 4L, "user", "user", time),
                        message(
                                "assistant-tool-call",
                                key,
                                5L,
                                "assistant",
                                "custom_tool_call",
                                time),
                        message("tool-output", key, 6L, "tool", "message", time),
                        message("developer-message", key, 7L, "developer", "message", time),
                        message(
                                "user-non-message",
                                key,
                                8L,
                                "user",
                                "custom_tool_call",
                                time),
                        message(
                                "claude-tool-use",
                                key,
                                9L,
                                "assistant",
                                "assistant",
                                pureToolUse,
                                time),
                        message(
                                "claude-tool-result",
                                key,
                                10L,
                                "user",
                                "user",
                                pureToolResult,
                                time),
                        message(
                                "claude-mixed",
                                key,
                                11L,
                                "assistant",
                                "assistant",
                                mixed,
                                time));

        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
            repository.commit(
                    0L,
                    Collections.singletonList(
                            new SessionBatch(
                                    session(key, "Conversation filtering", false, time),
                                    messages)));

            try (PaimonDashboardDataStore store =
                    new PaimonDashboardDataStore(repository, 100)) {
                DashboardPage<DashboardMessage> all =
                        store.listMessages(
                                new MessageQuery(
                                        "claude",
                                        key.sessionId(),
                                        null,
                                        null,
                                        null,
                                        false,
                                        1,
                                        20));
                assertThat(all.getTotal()).isEqualTo(11L);

                DashboardPage<DashboardMessage> firstConversationalPage =
                        store.listMessages(
                                new MessageQuery(
                                        "claude",
                                        key.sessionId(),
                                        null,
                                        null,
                                        null,
                                        true,
                                        1,
                                        1));
                assertThat(firstConversationalPage.getTotal()).isEqualTo(5L);
                assertThat(firstConversationalPage.isHasMore()).isTrue();
                assertThat(firstConversationalPage.getItems())
                        .extracting(DashboardMessage::getMessageId)
                        .contains("claude-mixed")
                        .doesNotContain("claude-tool-use", "claude-tool-result");

                DashboardPage<DashboardMessage> conversational =
                        store.listMessages(
                                new MessageQuery(
                                        "claude",
                                        key.sessionId(),
                                        null,
                                        null,
                                        null,
                                        true,
                                        1,
                                        20));
                assertThat(conversational.getTotal()).isEqualTo(5L);
                assertThat(conversational.getItems())
                        .extracting(DashboardMessage::getMessageId)
                        .containsExactlyInAnyOrder(
                                "user-message",
                                "assistant-message",
                                "assistant-event",
                                "user-event",
                                "claude-mixed");
                assertThat(conversational.getItems())
                        .filteredOn(message -> "claude-mixed".equals(message.getMessageId()))
                        .extracting(DashboardMessage::getContentPreview)
                        .containsExactly("human answer");

                assertThat(
                                store.listMessages(
                                                new MessageQuery(
                                                        "claude",
                                                        key.sessionId(),
                                                        null,
                                                        null,
                                                        null,
                                                        false,
                                                        1,
                                                        20))
                                        .getTotal())
                        .isEqualTo(11L);
            }
        }
    }

    @Test
    void keepsGeneratedImagesAndNonToolAttachmentsInConversationPages() throws Exception {
        Instant time = Instant.parse("2026-01-05T00:00:00Z");
        SessionKey codexKey = new SessionKey("codex", "generated-image-session");
        SessionKey claudeKey = new SessionKey("claude", "attachment-session");
        ChatMessage generatedImage =
                message(
                        "generated-image",
                        codexKey,
                        1L,
                        "assistant",
                        "image_generation_end",
                        "{\"type\":\"event_msg\",\"payload\":{"
                                + "\"type\":\"image_generation_end\","
                                + "\"result\":\"paimon-blob:0\"}}",
                        time);
        ChatMessage attachmentRole =
                message(
                        "attachment-role",
                        claudeKey,
                        1L,
                        "attachment",
                        "attachment",
                        "{\"type\":\"attachment\"}",
                        time);
        ChatMessage userAttachment =
                message(
                        "user-attachment",
                        claudeKey,
                        2L,
                        "user",
                        "attachment",
                        "{\"type\":\"attachment\",\"message\":{\"content\":[{"
                                + "\"type\":\"file\",\"name\":\"notes.txt\"}]}}",
                        time);
        ChatMessage internalAttachment =
                message(
                        "system-attachment",
                        claudeKey,
                        3L,
                        "system",
                        "attachment",
                        "{\"type\":\"attachment\"}",
                        time);

        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
            repository.commit(
                    0L,
                    Arrays.asList(
                            new SessionBatch(
                                    session(
                                            codexKey,
                                            "Generated image",
                                            false,
                                            time),
                                    Collections.singletonList(generatedImage)),
                            new SessionBatch(
                                    session(claudeKey, "Attachments", false, time),
                                    Arrays.asList(
                                            attachmentRole,
                                            userAttachment,
                                            internalAttachment))));

            try (PaimonDashboardDataStore store =
                    new PaimonDashboardDataStore(repository, 100)) {
                DashboardPage<DashboardMessage> codex =
                        store.listMessages(
                                new MessageQuery(
                                        "codex",
                                        codexKey.sessionId(),
                                        null,
                                        null,
                                        null,
                                        true,
                                        1,
                                        10));
                assertThat(codex.getTotal()).isEqualTo(1L);
                assertThat(codex.getItems())
                        .extracting(
                                DashboardMessage::getMessageId,
                                DashboardMessage::getContentPreview)
                        .containsExactly(
                                org.assertj.core.groups.Tuple.tuple(
                                        "generated-image", "生成图片"));

                DashboardPage<DashboardMessage> claude =
                        store.listMessages(
                                new MessageQuery(
                                        "claude",
                                        claudeKey.sessionId(),
                                        null,
                                        null,
                                        null,
                                        true,
                                        1,
                                        10));
                assertThat(claude.getTotal()).isEqualTo(2L);
                assertThat(claude.getItems())
                        .extracting(DashboardMessage::getMessageId)
                        .containsExactlyInAnyOrder(
                                "attachment-role", "user-attachment")
                        .doesNotContain("system-attachment");
                assertThat(claude.getItems())
                        .extracting(DashboardMessage::getContentPreview)
                        .containsOnly("附件");
            }
        }
    }

    @Test
    void filtersSubagentsBeforeSessionSearchPaginationAndTotalsButKeepsOverviewAllRows()
            throws Exception {
        Instant time = Instant.parse("2026-01-03T00:00:00Z");
        SessionKey rootKey = new SessionKey("codex", "root-session");
        SessionKey childKey = new SessionKey("codex", "child-session");
        ChatSession root = session(rootKey, "Visible root", false, time);
        ChatSession child =
                session(
                        childKey,
                        "Hidden subagent needle",
                        false,
                        time.plusSeconds(1),
                        "{\"thread_spawn\":{\"parent_thread_id\":\"root-session\"}}");

        try (PaimonChatRepository repository =
                        new PaimonChatRepository(configuration())) {
            repository.initialize();
            repository.commit(
                    0L,
                    Arrays.asList(
                            new SessionBatch(root, Collections.emptyList()),
                            new SessionBatch(child, Collections.emptyList())));

            try (PaimonDashboardDataStore store =
                    new PaimonDashboardDataStore(repository, 100)) {
                DashboardOverview overview = store.overview();
                assertThat(overview.getSessionCount()).isEqualTo(2L);
                assertThat(overview.getActiveSessionCount()).isEqualTo(2L);
                assertThat(overview.getSessionCountBySource()).containsEntry("codex", 2L);

                DashboardPage<DashboardSession> visible =
                        store.listSessions(new SessionQuery(null, null, null, 1, 1));
                assertThat(visible.getTotal()).isEqualTo(1L);
                assertThat(visible.isHasMore()).isFalse();
                assertThat(visible.getItems())
                        .extracting(DashboardSession::getSessionId)
                        .containsExactly("root-session");

                DashboardPage<DashboardSession> hiddenSearch =
                        store.listSessions(
                                new SessionQuery(
                                        "codex", "subagent needle", false, 1, 10));
                assertThat(hiddenSearch.getTotal()).isZero();
                assertThat(hiddenSearch.getItems()).isEmpty();
            }
        }
    }

    @Test
    void keepsSourcesIsolatedWhenSessionAndMessageKeysCollide() throws Exception {
        AgentConfiguration configuration = configuration();
        Instant time = Instant.parse("2026-02-01T00:00:00Z");
        SessionKey codexKey = new SessionKey("codex", "shared-session");
        SessionKey claudeKey = new SessionKey("claude", "shared-session");
        String codexContent =
                "{\"payload\":{\"text\":\"codex-only\"},"
                        + "\"_paimon_attachments\":[{\"index\":0,\"file_name\":\"codex.bin\","
                        + "\"mime_type\":\"application/octet-stream\",\"status\":\"stored\","
                        + "\"size\":3}]}";
        String claudeContent =
                "{\"payload\":{\"text\":\"claude-only\"},"
                        + "\"_paimon_attachments\":[{\"index\":0,\"file_name\":\"claude.bin\","
                        + "\"mime_type\":\"application/octet-stream\",\"status\":\"stored\","
                        + "\"size\":2}]}";
        ChatMessage codexMessage =
                new ChatMessage(
                        "shared-message",
                        codexKey,
                        7L,
                        "user",
                        "message",
                        codexContent,
                        Collections.singletonList(
                                AttachmentPayload.of(new byte[] {1, 2, 3})),
                        time,
                        time);
        ChatMessage claudeMessage =
                new ChatMessage(
                        "shared-message",
                        claudeKey,
                        7L,
                        "user",
                        "message",
                        claudeContent,
                        Collections.singletonList(AttachmentPayload.of(new byte[] {8, 9})),
                        time,
                        time);

        try (PaimonChatRepository repository = new PaimonChatRepository(configuration)) {
            repository.initialize();
            repository.commit(
                    0L,
                    Arrays.asList(
                            new SessionBatch(
                                    session(codexKey, "Codex collision", false, time),
                                    Collections.singletonList(codexMessage)),
                            new SessionBatch(
                                    session(claudeKey, "Claude collision", false, time),
                                    Collections.singletonList(claudeMessage))));

            try (PaimonDashboardDataStore store =
                    new PaimonDashboardDataStore(repository, 100)) {
                DashboardPage<DashboardMessage> codex =
                        store.listMessages(
                                new MessageQuery(
                                        "codex",
                                        "shared-session",
                                        "user",
                                        "message",
                                        null,
                                        1,
                                        10));
                assertThat(codex.getTotal()).isEqualTo(1L);
                assertThat(codex.getItems().get(0).getSourceType()).isEqualTo("codex");
                assertThat(codex.getItems().get(0).getContentPreview())
                        .contains("codex-only")
                        .doesNotContain("claude-only");

                DashboardPage<DashboardMessage> claude =
                        store.listMessages(
                                new MessageQuery(
                                        "claude",
                                        "shared-session",
                                        "user",
                                        "message",
                                        null,
                                        1,
                                        10));
                assertThat(claude.getTotal()).isEqualTo(1L);
                assertThat(claude.getItems().get(0).getSourceType()).isEqualTo("claude");
                assertThat(claude.getItems().get(0).getContentPreview())
                        .contains("claude-only")
                        .doesNotContain("codex-only");

                DashboardMessageDetail detail =
                        store.messageDetail(
                                        "codex",
                                        "shared-session",
                                        "shared-message",
                                        7L)
                                .orElseThrow(AssertionError::new);
                assertThat(detail.getContentJson()).isEqualTo(codexContent);
                assertThat(detail.getAttachments()).hasSize(1);
                assertThat(detail.getAttachments().get(0).getFileName()).isEqualTo("codex.bin");

                AttachmentData attachment =
                        store.attachment(
                                        "codex",
                                        "shared-session",
                                        "shared-message",
                                        7L,
                                        0,
                                        10L)
                                .orElseThrow(AssertionError::new);
                assertThat(attachment.getBytes()).containsExactly(1, 2, 3);
                assertThat(attachment.getFileName()).isEqualTo("codex.bin");
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

    @Test
    void closeInterruptsReadersAndOnlyThenClosesOwnedRepository() throws Exception {
        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
            ExecutorService readers = Executors.newFixedThreadPool(2);
            CountDownLatch readerStarted = new CountDownLatch(1);
            CountDownLatch blockReader = new CountDownLatch(1);
            AtomicBoolean readerExited = new AtomicBoolean();
            AtomicBoolean readerInterrupted = new AtomicBoolean();
            AtomicBoolean repositoryClosed = new AtomicBoolean();
            AtomicBoolean repositoryClosedWhileReaderRunning = new AtomicBoolean();
            PaimonDashboardDataStore store =
                    new PaimonDashboardDataStore(
                            repository.sessionsTableForRead(),
                            repository.messagesTableForRead(),
                            10,
                            () -> {
                                repositoryClosedWhileReaderRunning.set(!readerExited.get());
                                repositoryClosed.set(true);
                            },
                            readers,
                            25L);
            readers.execute(
                    () -> {
                        readerStarted.countDown();
                        try {
                            blockReader.await();
                        } catch (InterruptedException expected) {
                            readerInterrupted.set(true);
                            Thread.currentThread().interrupt();
                        } finally {
                            readerExited.set(true);
                        }
                    });

            try {
                assertThat(readerStarted.await(5, TimeUnit.SECONDS)).isTrue();
                store.close();

                assertThat(readerInterrupted).isTrue();
                assertThat(readerExited).isTrue();
                assertThat(repositoryClosed).isTrue();
                assertThat(repositoryClosedWhileReaderRunning).isFalse();
            } finally {
                blockReader.countDown();
                readers.shutdownNow();
                readers.awaitTermination(5, TimeUnit.SECONDS);
                store.close();
            }
        }
    }

    @Test
    void closeLeavesOwnedRepositoryOpenUntilStubbornReaderActuallyExits() throws Exception {
        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
            ExecutorService readers = Executors.newFixedThreadPool(2);
            CountDownLatch readerStarted = new CountDownLatch(1);
            CountDownLatch releaseReader = new CountDownLatch(1);
            CountDownLatch readerExited = new CountDownLatch(1);
            AtomicBoolean repositoryClosed = new AtomicBoolean();
            PaimonDashboardDataStore store =
                    new PaimonDashboardDataStore(
                            repository.sessionsTableForRead(),
                            repository.messagesTableForRead(),
                            10,
                            () -> repositoryClosed.set(true),
                            readers,
                            20L);
            readers.execute(
                    () -> {
                        readerStarted.countDown();
                        try {
                            boolean released = false;
                            while (!released) {
                                try {
                                    releaseReader.await();
                                    released = true;
                                } catch (InterruptedException ignored) {
                                    // Simulates a reader blocked in code which does not honor the
                                    // cancellation interrupt promptly.
                                }
                            }
                        } finally {
                            readerExited.countDown();
                        }
                    });

            try {
                assertThat(readerStarted.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(store::close)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("remains open");
                assertThat(repositoryClosed).isFalse();
                assertThat(readerExited.getCount()).isEqualTo(1L);

                releaseReader.countDown();
                assertThat(readerExited.await(5, TimeUnit.SECONDS)).isTrue();
                store.close();
                assertThat(repositoryClosed).isTrue();
            } finally {
                releaseReader.countDown();
                readers.shutdownNow();
                readers.awaitTermination(5, TimeUnit.SECONDS);
                store.close();
            }
        }
    }

    @Test
    void parallelCancellationWaitsForActualRunnerExitInsteadOfFutureState() throws Exception {
        ExecutorService readers = Executors.newSingleThreadExecutor();
        CountDownLatch readerStarted = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
        CountDownLatch readerExited = new CountDownLatch(1);
        AtomicReference<Throwable> waitFailure = new AtomicReference<>();
        Future<?> future =
                readers.submit(
                        () -> {
                            readerStarted.countDown();
                            try {
                                boolean released = false;
                                while (!released) {
                                    try {
                                        releaseReader.await();
                                        released = true;
                                    } catch (InterruptedException ignored) {
                                        cancellationObserved.countDown();
                                    }
                                }
                            } finally {
                                readerExited.countDown();
                            }
                        });

        try {
            assertThat(readerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            Thread waiter =
                    new Thread(
                            () -> {
                                try {
                                    PaimonDashboardDataStore.cancelAndAwaitParallelTasks(
                                            Collections.singletonList(future), readerExited, 5_000L);
                                } catch (Throwable failure) {
                                    waitFailure.set(failure);
                                }
                            },
                            "parallel-cancellation-test");
            waiter.start();

            assertThat(cancellationObserved.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(future.isCancelled()).isTrue();
            assertThat(readerExited.getCount()).isEqualTo(1L);
            assertThat(waiter.isAlive()).isTrue();

            releaseReader.countDown();
            waiter.join(5_000L);
            assertThat(waiter.isAlive()).isFalse();
            assertThat(waitFailure.get()).isNull();
            assertThat(readerExited.getCount()).isZero();
        } finally {
            releaseReader.countDown();
            readers.shutdownNow();
            readers.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void parallelCancellationTimesOutWhenRunnerIgnoresInterrupts() throws Exception {
        ExecutorService readers = Executors.newSingleThreadExecutor();
        CountDownLatch readerStarted = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
        CountDownLatch readerExited = new CountDownLatch(1);
        Future<?> future =
                readers.submit(
                        () -> {
                            readerStarted.countDown();
                            try {
                                boolean released = false;
                                while (!released) {
                                    try {
                                        releaseReader.await();
                                        released = true;
                                    } catch (InterruptedException ignored) {
                                        // Keep running past Future.cancel(true).
                                    }
                                }
                            } finally {
                                readerExited.countDown();
                            }
                        });

        try {
            assertThat(readerStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(
                            () ->
                                    PaimonDashboardDataStore.cancelAndAwaitParallelTasks(
                                            Collections.singletonList(future), readerExited, 20L))
                    .isInstanceOf(java.util.concurrent.TimeoutException.class)
                    .hasMessageContaining("did not terminate");
            assertThat(readerExited.getCount()).isEqualTo(1L);
        } finally {
            releaseReader.countDown();
            assertThat(readerExited.await(5, TimeUnit.SECONDS)).isTrue();
            readers.shutdownNow();
            readers.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void splitsIndependentDataEvolutionRangesWithoutSeparatingOverlappingFiles() {
        List<DataFileMeta> files =
                Arrays.asList(
                        dataFile("data-0.parquet", 0L),
                        dataFile("blob-0.blob", 0L),
                        dataFile("data-1.parquet", 10L),
                        dataFile("blob-1.blob", 10L),
                        dataFile("data-2.parquet", 20L),
                        dataFile("blob-2.blob", 20L));
        DataSplit planned =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(files)
                        .rawConvertible(false)
                        .build();

        List<Split> independent =
                PaimonDashboardDataStore.independentMessageSplits(
                        Collections.singletonList(planned));

        assertThat(independent).hasSize(3);
        List<DataFileMeta> splitFiles =
                independent.stream()
                        .map(DataSplit.class::cast)
                        .flatMap(split -> split.dataFiles().stream())
                        .collect(Collectors.toList());
        assertThat(splitFiles).containsExactlyInAnyOrderElementsOf(files);
        assertThat(independent)
                .allSatisfy(
                        split -> {
                            List<DataFileMeta> grouped = ((DataSplit) split).dataFiles();
                            assertThat(grouped).hasSize(2);
                            assertThat(grouped)
                                    .extracting(DataFileMeta::firstRowId)
                                    .containsOnly(grouped.get(0).firstRowId());
                        });
    }

    private static DataFileMeta dataFile(String fileName, long firstRowId) {
        return DataFileMeta.forAppend(
                fileName,
                100L,
                10L,
                SimpleStats.EMPTY_STATS,
                firstRowId,
                firstRowId + 9L,
                0L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                firstRowId,
                null);
    }

    private static ChatSession session(
            SessionKey key, String title, boolean archived, Instant time) {
        return session(key, title, archived, time, null);
    }

    private static ChatSession session(
            SessionKey key,
            String title,
            boolean archived,
            Instant time,
            String subagentSourceJson) {
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
                time,
                subagentSourceJson);
    }

    private static ChatMessage message(
            String messageId,
            SessionKey key,
            long sequence,
            String role,
            String eventType,
            Instant time) {
        return message(
                messageId,
                key,
                sequence,
                role,
                eventType,
                "{\"text\":\"" + messageId + "\"}",
                time);
    }

    private static ChatMessage message(
            String messageId,
            SessionKey key,
            long sequence,
            String role,
            String eventType,
            String contentJson,
            Instant time) {
        return new ChatMessage(
                messageId,
                key,
                sequence,
                role,
                eventType,
                contentJson,
                Collections.emptyList(),
                time.plusSeconds(sequence),
                time.plusSeconds(sequence));
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
