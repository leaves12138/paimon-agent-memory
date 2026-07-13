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
import java.util.Map;
import java.util.Optional;

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
                        "{\"text\":\"pending needle\",\"_paimon_attachments\":[{"
                                + "\"index\":0,\"file_name\":\"pending.png\","
                                + "\"mime_type\":\"image/png\",\"status\":\"stored\","
                                + "\"size\":4,\"sha256\":\"digest\"}]}",
                        Collections.singletonList(AttachmentPayload.of(image)),
                        NOW,
                        NOW);
        ChatSession updatedExisting = chatSession(existingKey, "new pending title");
        ChatSession newSession = chatSession(newKey, "new session");
        PendingDataSnapshot snapshot =
                new PendingDataSnapshot(
                        7L,
                        Arrays.asList(
                                new SessionBatch(updatedExisting, Collections.emptyList()),
                                new SessionBatch(
                                        newSession,
                                        Collections.singletonList(pendingMessage))));

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
                        new MessageQuery(null, null, null, null, null, 1, 20));
        assertThat(messages.getTotal()).isEqualTo(2L);
        DashboardMessage pendingRow =
                messages.getItems().stream()
                        .filter(item -> item.getMessageId().equals("pending-message"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        assertThat(pendingRow.getStorageStatus()).isEqualTo(DashboardStorageStatus.PENDING);
        assertThat(pendingRow.getContentPreview()).contains("pending needle");

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
                NOW);
    }

    private static final class FakeUploadedStore implements DashboardDataStore {
        private final DashboardSession session;
        private final DashboardMessage message;
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
}
