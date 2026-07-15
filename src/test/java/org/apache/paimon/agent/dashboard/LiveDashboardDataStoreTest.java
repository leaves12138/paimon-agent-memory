package org.apache.paimon.agent.dashboard;

import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.service.PendingDataSnapshot;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class LiveDashboardDataStoreTest {

    private static final Instant NOW = Instant.parse("2026-07-13T08:00:00Z");

    @Test
    void mergesAndExpandsPendingSessionsMessagesAndAttachments() throws Exception {
        SessionKey existingKey = new SessionKey("codex", "session-existing");
        SessionKey newKey = new SessionKey("claude", "session-new");
        DashboardSession uploadedSession =
                dashboardSession(existingKey, "old uploaded title");
        DashboardMessage uploadedMessage =
                new DashboardMessage(
                        "uploaded-message",
                        "codex",
                        "session-existing",
                        1L,
                        "assistant",
                        "message",
                        "uploaded",
                        8L,
                        NOW.minusSeconds(10),
                        NOW.minusSeconds(10));
        FakeUploadedStore uploaded = new FakeUploadedStore(uploadedSession, uploadedMessage);

        byte[] image = new byte[] {1, 2, 3, 4};
        ChatMessage pendingMessage =
                new ChatMessage(
                        "pending-message",
                        newKey,
                        2L,
                        "user",
                        "message",
                        "{\"text\":\"pending needle "
                                + "x".repeat(500)
                                + "\",\"_paimon_attachments\":[{"
                                + "\"index\":0,\"file_name\":\"pending.png\","
                                + "\"mime_type\":\"image/png\",\"status\":\"stored\","
                                + "\"size\":4,\"sha256\":\"digest\"}]}",
                        Collections.singletonList(AttachmentPayload.of(image)),
                        NOW,
                        NOW);
        ChatMessage pendingToolMessage =
                new ChatMessage(
                        "pending-tool-message",
                        newKey,
                        3L,
                        "assistant",
                        "custom_tool_call",
                        "{\"name\":\"exec\"}",
                        Collections.emptyList(),
                        NOW.plusSeconds(1),
                        NOW.plusSeconds(1));
        ChatMessage pendingClaudeToolUse =
                new ChatMessage(
                        "pending-claude-tool-use",
                        newKey,
                        4L,
                        "assistant",
                        "assistant",
                        "{\"type\":\"assistant\",\"message\":{\"content\":[{"
                                + "\"type\":\"tool_use\",\"name\":\"Bash\","
                                + "\"input\":{\"command\":\"pwd\"}}]}}",
                        Collections.emptyList(),
                        NOW.plusSeconds(2),
                        NOW.plusSeconds(2));
        ChatMessage pendingClaudeToolResult =
                new ChatMessage(
                        "pending-claude-tool-result",
                        newKey,
                        5L,
                        "user",
                        "user",
                        "{\"type\":\"user\",\"message\":{\"content\":[{"
                                + "\"type\":\"tool_result\","
                                + "\"content\":\"finished\"}]}}",
                        Collections.emptyList(),
                        NOW.plusSeconds(3),
                        NOW.plusSeconds(3));
        ChatMessage pendingClaudeMixed =
                new ChatMessage(
                        "pending-claude-mixed",
                        newKey,
                        6L,
                        "assistant",
                        "assistant",
                        "{\"type\":\"assistant\",\"message\":{\"content\":[{"
                                + "\"type\":\"text\",\"text\":\"mixed answer\"},{"
                                + "\"type\":\"tool_use\",\"name\":\"Bash\","
                                + "\"input\":{\"command\":\"pwd\"}}]}}",
                        Collections.emptyList(),
                        NOW.plusSeconds(4),
                        NOW.plusSeconds(4));
        ChatSession updatedExisting = chatSession(existingKey, "new pending title");
        ChatSession newSession = chatSession(newKey, "new session");
        PendingDataSnapshot snapshot =
                new PendingDataSnapshot(
                        7L,
                        Arrays.asList(
                                new SessionBatch(updatedExisting, Collections.emptyList()),
                                new SessionBatch(
                                        newSession,
                                        Arrays.asList(
                                                pendingMessage,
                                                pendingToolMessage,
                                                pendingClaudeToolUse,
                                                pendingClaudeToolResult,
                                                pendingClaudeMixed))));

        LiveDashboardDataStore store =
                new LiveDashboardDataStore(uploaded, () -> snapshot, 20);
        DashboardPage<DashboardSession> sessions =
                store.listSessions(new SessionQuery(null, null, null, 1, 20));
        assertThat(sessions.getTotal()).isEqualTo(2L);
        assertThat(sessions.getItems())
                .extracting(DashboardSession::getTitle)
                .containsExactlyInAnyOrder("new pending title", "new session");
        assertThat(sessions.getItems())
                .allMatch(
                        session ->
                                session.getStorageStatus()
                                        == DashboardStorageStatus.PENDING);
        assertThat(sessions.getItems())
                .extracting(DashboardSession::getPendingCommitId)
                .containsOnly(7L);

        DashboardPage<DashboardMessage> messages =
                store.listMessages(
                        new MessageQuery(
                                null, null, null, null, null, true, 1, 20));
        assertThat(messages.getTotal()).isEqualTo(3L);
        assertThat(messages.getItems())
                .extracting(DashboardMessage::getMessageId)
                .containsExactlyInAnyOrder(
                        "uploaded-message", "pending-message", "pending-claude-mixed")
                .doesNotContain(
                        "pending-tool-message",
                        "pending-claude-tool-use",
                        "pending-claude-tool-result");
        assertThat(uploaded.lastMessageQuery.isConversationOnly()).isTrue();
        DashboardMessage pendingRow =
                messages.getItems().stream()
                        .filter(item -> item.getMessageId().equals("pending-message"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        assertThat(pendingRow.getStorageStatus()).isEqualTo(DashboardStorageStatus.PENDING);
        assertThat(pendingRow.getContentPreview()).contains("pending needle");
        assertThat(pendingRow.getContentPreview().length()).isGreaterThan(240);
        assertThat(messages.getItems())
                .filteredOn(
                        message ->
                                "pending-claude-mixed".equals(message.getMessageId()))
                .extracting(DashboardMessage::getContentPreview)
                .containsExactly("mixed answer");

        DashboardMessageDetail detail =
                store.messageDetail("claude", "session-new", "pending-message", 2L)
                        .orElseThrow(AssertionError::new);
        assertThat(detail.getStorageStatus()).isEqualTo(DashboardStorageStatus.PENDING);
        assertThat(detail.getAttachments()).hasSize(1);
        assertThat(detail.getAttachments().get(0).getFileName()).isEqualTo("pending.png");
        assertThat(
                        store.attachment(
                                        "claude",
                                        "session-new",
                                        "pending-message",
                                        2L,
                                        0,
                                        4L)
                                .orElseThrow(AssertionError::new)
                                .getBytes())
                .isEqualTo(image);

        store.close();
        assertThat(uploaded.closed).isTrue();
    }

    @Test
    void hidesPendingSubagentsAndRemovesTheirLegacyUploadedSessionByKey()
            throws Exception {
        SessionKey childKey = new SessionKey("codex", "session-child");
        SessionKey rootKey = new SessionKey("codex", "session-root");
        DashboardSession legacyChild = dashboardSession(childKey, "legacy child");
        DashboardMessage unusedMessage =
                new DashboardMessage(
                        "message-child",
                        "codex",
                        "session-child",
                        1L,
                        "assistant",
                        "message",
                        "child",
                        5L,
                        NOW,
                        NOW);
        FakeUploadedStore uploaded = new FakeUploadedStore(legacyChild, unusedMessage);
        ChatSession pendingChild =
                chatSession(childKey, "pending child", "{\"thread_spawn\":{}}");
        ChatSession pendingRoot = chatSession(rootKey, "visible root");
        PendingDataSnapshot pending =
                new PendingDataSnapshot(
                        12L,
                        Arrays.asList(
                                new SessionBatch(pendingChild, Collections.emptyList()),
                                new SessionBatch(pendingRoot, Collections.emptyList())));

        LiveDashboardDataStore store =
                new LiveDashboardDataStore(uploaded, () -> pending, 20);

        DashboardPage<DashboardSession> page =
                store.listSessions(new SessionQuery(null, null, null, 1, 20));
        assertThat(page.getTotal()).isEqualTo(1L);
        assertThat(page.getItems())
                .extracting(DashboardSession::getSessionId)
                .containsExactly("session-root");
        assertThat(
                        store.listSessions(
                                        new SessionQuery(
                                                "codex", "legacy child", null, 1, 20))
                                .getItems())
                .isEmpty();
    }

    @Test
    void invalidatesUploadedCacheBeforeRemovingCommittedPendingOverlay() throws Exception {
        SessionKey key = new SessionKey("codex", "session-handoff");
        ChatSession pendingSession = chatSession(key, "handoff session");
        ChatMessage pendingMessage =
                new ChatMessage(
                        "message-handoff",
                        key,
                        3L,
                        "assistant",
                        "message",
                        "{\"text\":\"handoff answer\"}",
                        Collections.emptyList(),
                        NOW,
                        NOW);
        AtomicReference<PendingDataSnapshot> pending =
                new AtomicReference<>(
                        new PendingDataSnapshot(
                                11L,
                                Collections.singletonList(
                                        new SessionBatch(
                                                pendingSession,
                                                Collections.singletonList(pendingMessage)))));
        CachingUploadedStore uploaded = new CachingUploadedStore();
        LiveDashboardDataStore store =
                new LiveDashboardDataStore(uploaded, pending::get, 20);

        assertThat(
                        store.listSessions(new SessionQuery(null, null, null, 1, 20))
                                .getItems())
                .extracting(DashboardSession::getSessionId)
                .containsExactly("session-handoff");
        assertThat(
                        store.listMessages(
                                        new MessageQuery(
                                                "codex",
                                                "session-handoff",
                                                null,
                                                null,
                                                null,
                                                1,
                                                20))
                                .getItems())
                .extracting(DashboardMessage::getMessageId)
                .containsExactly("message-handoff");

        uploaded.commit(
                dashboardSession(key, "handoff session"),
                new DashboardMessage(
                        "message-handoff",
                        "codex",
                        "session-handoff",
                        3L,
                        "assistant",
                        "message",
                        "handoff answer",
                        25L,
                        NOW,
                        NOW));
        pending.set(PendingDataSnapshot.empty());

        DashboardSession committedSession =
                store.listSessions(new SessionQuery(null, null, null, 1, 20))
                        .getItems()
                        .get(0);
        DashboardMessage committedMessage =
                store.listMessages(
                                new MessageQuery(
                                        "codex",
                                        "session-handoff",
                                        null,
                                        null,
                                        null,
                                        1,
                                        20))
                        .getItems()
                        .get(0);
        assertThat(uploaded.invalidations).isEqualTo(1);
        assertThat(committedSession.getStorageStatus())
                .isEqualTo(DashboardStorageStatus.UPLOADED);
        assertThat(committedMessage.getStorageStatus())
                .isEqualTo(DashboardStorageStatus.UPLOADED);

        store.invalidate();
        assertThat(uploaded.invalidations).isEqualTo(2);
    }

    @Test
    void invalidatesWhenPendingCommitIdentifierAdvancesWithoutAnObservedEmptyState()
            throws Exception {
        SessionKey committedKey = new SessionKey("codex", "session-commit-21");
        SessionKey nextKey = new SessionKey("codex", "session-commit-22");
        AtomicReference<PendingDataSnapshot> pending =
                new AtomicReference<>(
                        new PendingDataSnapshot(
                                21L,
                                Collections.singletonList(
                                        new SessionBatch(
                                                chatSession(committedKey, "commit 21"),
                                                Collections.emptyList()))));
        CachingUploadedStore uploaded = new CachingUploadedStore();
        LiveDashboardDataStore store =
                new LiveDashboardDataStore(uploaded, pending::get, 20);

        store.listSessions(new SessionQuery(null, null, null, 1, 20));
        uploaded.commit(
                dashboardSession(committedKey, "commit 21"),
                new DashboardMessage(
                        "message-commit-21",
                        "codex",
                        "session-commit-21",
                        1L,
                        "user",
                        "message",
                        "committed",
                        9L,
                        NOW,
                        NOW));
        pending.set(
                new PendingDataSnapshot(
                        22L,
                        Collections.singletonList(
                                new SessionBatch(
                                        chatSession(nextKey, "commit 22"),
                                        Collections.emptyList()))));

        assertThat(
                        store.listSessions(new SessionQuery(null, null, null, 1, 20))
                                .getItems())
                .extracting(DashboardSession::getSessionId)
                .containsExactlyInAnyOrder("session-commit-21", "session-commit-22");
        assertThat(uploaded.invalidations).isEqualTo(1);
    }

    @Test
    void invalidatesWhenCommitCompletesBetweenDashboardReads() throws Exception {
        SessionKey key = new SessionKey("codex", "session-generation");
        AtomicReference<PendingDataSnapshot> pending =
                new AtomicReference<>(PendingDataSnapshot.empty());
        AtomicLong commitGeneration = new AtomicLong();
        CachingUploadedStore uploaded = new CachingUploadedStore();
        DashboardMessage message =
                new DashboardMessage(
                        "message-generation",
                        "codex",
                        "session-generation",
                        1L,
                        "user",
                        "message",
                        "unchanged message",
                        17L,
                        NOW,
                        NOW);
        uploaded.commit(dashboardSession(key, "old title"), message);
        uploaded.invalidate();

        LiveDashboardDataStore store =
                new LiveDashboardDataStore(
                        uploaded, pending::get, commitGeneration::get, 20);
        assertThat(
                        store.listSessions(new SessionQuery(null, null, null, 1, 20))
                                .getItems())
                .extracting(DashboardSession::getTitle)
                .containsExactly("old title");

        // The collector commits between requests, so the Dashboard never observes a non-empty
        // pending overlay. The generation must still evict the uploaded cache.
        uploaded.commit(dashboardSession(key, "renamed title"), message);
        commitGeneration.incrementAndGet();

        assertThat(
                        store.listSessions(new SessionQuery(null, null, null, 1, 20))
                                .getItems())
                .extracting(DashboardSession::getTitle)
                .containsExactly("renamed title");
        assertThat(uploaded.invalidations).isEqualTo(2);
    }

    private static DashboardSession dashboardSession(SessionKey key, String title) {
        return new DashboardSession(
                key.sourceType(),
                key.sessionId(),
                title,
                "/tmp",
                false,
                "/tmp/session.jsonl",
                "byte:1",
                1L,
                null,
                null,
                NOW,
                NOW,
                NOW,
                NOW);
    }

    private static ChatSession chatSession(SessionKey key, String title) {
        return chatSession(key, title, null);
    }

    private static ChatSession chatSession(
            SessionKey key, String title, String subagentSourceJson) {
        return new ChatSession(
                key,
                title,
                "/tmp",
                false,
                "/tmp/session.jsonl",
                "byte:2",
                1L,
                NOW,
                NOW,
                NOW,
                NOW,
                subagentSourceJson);
    }

    private static final class FakeUploadedStore implements DashboardDataStore {
        private final DashboardSession session;
        private final DashboardMessage message;
        private MessageQuery lastMessageQuery;
        private boolean closed;

        private FakeUploadedStore(DashboardSession session, DashboardMessage message) {
            this.session = session;
            this.message = message;
        }

        @Override
        public DashboardOverview overview() {
            Map<String, Long> counts = new LinkedHashMap<>();
            counts.put("codex", 1L);
            return new DashboardOverview(
                    1L, 1L, 1L, 0L, 0L, counts, counts, NOW);
        }

        @Override
        public DashboardPage<DashboardSession> listSessions(SessionQuery query) {
            return new DashboardPage<>(
                    Collections.singletonList(session), 1, query.getPageSize(), 1L);
        }

        @Override
        public DashboardPage<DashboardMessage> listMessages(MessageQuery query) {
            lastMessageQuery = query;
            return new DashboardPage<>(
                    Collections.singletonList(message), 1, query.getPageSize(), 1L);
        }

        @Override
        public Optional<DashboardMessageDetail> messageDetail(
                String sourceType, String sessionId, String messageId, long sequenceNo) {
            return Optional.empty();
        }

        @Override
        public Optional<AttachmentData> attachment(
                String sourceType,
                String sessionId,
                String messageId,
                long sequenceNo,
                int index,
                long maxBytes) {
            return Optional.empty();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class CachingUploadedStore implements DashboardDataStore {
        private List<DashboardSession> durableSessions = Collections.emptyList();
        private List<DashboardMessage> durableMessages = Collections.emptyList();
        private List<DashboardSession> cachedSessions = Collections.emptyList();
        private List<DashboardMessage> cachedMessages = Collections.emptyList();
        private int invalidations;

        private void commit(DashboardSession session, DashboardMessage message) {
            durableSessions = Collections.singletonList(session);
            durableMessages = Collections.singletonList(message);
        }

        @Override
        public DashboardOverview overview() {
            return new DashboardOverview(
                    cachedSessions.size(),
                    cachedMessages.size(),
                    cachedSessions.size(),
                    0L,
                    0L,
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    NOW);
        }

        @Override
        public DashboardPage<DashboardSession> listSessions(SessionQuery query) {
            return new DashboardPage<>(
                    cachedSessions,
                    query.getPage(),
                    query.getPageSize(),
                    cachedSessions.size());
        }

        @Override
        public DashboardPage<DashboardMessage> listMessages(MessageQuery query) {
            return new DashboardPage<>(
                    cachedMessages,
                    query.getPage(),
                    query.getPageSize(),
                    cachedMessages.size());
        }

        @Override
        public Optional<DashboardMessageDetail> messageDetail(
                String sourceType, String sessionId, String messageId, long sequenceNo) {
            return Optional.empty();
        }

        @Override
        public Optional<AttachmentData> attachment(
                String sourceType,
                String sessionId,
                String messageId,
                long sequenceNo,
                int index,
                long maxBytes) {
            return Optional.empty();
        }

        @Override
        public void invalidate() {
            invalidations++;
            cachedSessions = durableSessions;
            cachedMessages = durableMessages;
        }
    }
}
