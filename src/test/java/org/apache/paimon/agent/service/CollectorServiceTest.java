package org.apache.paimon.agent.service;

import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.config.SourceConfig;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.sink.ChatRepository;
import org.apache.paimon.agent.source.ConversationSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CollectorServiceTest {

    @TempDir Path tempDir;

    @Test
    void reservesBufferCapacityForEveryRemainingSource() throws Exception {
        SessionKey firstKey = new SessionKey("codex", "first");
        SessionKey secondKey = new SessionKey("claude", "second");
        List<String> calls = new ArrayList<>();
        List<Integer> budgets = new ArrayList<>();
        ConversationSource first =
                sourceWithOneMessage("first", firstKey, calls, budgets);
        ConversationSource second =
                sourceWithOneMessage("second", secondKey, calls, budgets);
        FakeRepository repository = new FakeRepository();

        try (CollectorService service =
                new CollectorService(config(2), Arrays.asList(first, second), repository)) {
            service.runOnce();
        }

        assertThat(calls).containsExactly("first", "second");
        assertThat(budgets).containsExactly(1, 1);
        assertThat(repository.committedBatches)
                .extracting(batch -> batch.session().key())
                .containsExactly(firstKey, secondKey);
    }

    @Test
    void rotatesTheFirstSourceAcrossScans() throws Exception {
        List<String> calls = new ArrayList<>();
        ConversationSource first = emptyRecordingSource("first", calls);
        ConversationSource second = emptyRecordingSource("second", calls);

        try (CollectorService service =
                new CollectorService(
                        config(), Arrays.asList(first, second), new FakeRepository())) {
            service.runOnce();
            service.runOnce();
        }

        assertThat(calls).containsExactly("first", "second", "second", "first");
    }

    @Test
    void recoversOnlyTheDurablePendingBoundaryWithTheSameCommitId() throws Exception {
        SessionKey key = new SessionKey("codex", "s1");
        ChatSession durable =
                new ChatSession(
                        key,
                        "title",
                        "/tmp",
                        false,
                        "/tmp/s1.jsonl",
                        "byte:0",
                        6L,
                        7L,
                        "byte:10",
                        Instant.EPOCH,
                        Instant.EPOCH,
                        null,
                        Instant.EPOCH);
        FakeRepository repository = new FakeRepository(durable);
        ConversationSource source =
                new ConversationSource() {
                    @Override
                    public String sourceType() {
                        return "codex";
                    }

                    @Override
                    public List<SessionBatch> scan(
                            Map<SessionKey, ChatSession> checkpoints,
                            int maxRecords,
                            Set<SessionKey> onlySessions) {
                        assertThat(onlySessions).containsExactly(key);
                        assertThat(checkpoints.get(key).sourceCursor()).isEqualTo("byte:0");
                        assertThat(checkpoints.get(key).pendingCursor()).isEqualTo("byte:10");
                        ChatSession recovered =
                                new ChatSession(
                                        key,
                                        "title",
                                        "/tmp",
                                        false,
                                        "/tmp/s1.jsonl",
                                        "byte:10",
                                        6L,
                                        Instant.EPOCH,
                                        Instant.EPOCH,
                                        Instant.EPOCH,
                                        Instant.EPOCH);
                        ChatMessage message =
                                new ChatMessage(
                                        "m1",
                                        key,
                                        1L,
                                        "user",
                                        "message",
                                        "{}",
                                        Collections.emptyList(),
                                        Instant.EPOCH,
                                        Instant.EPOCH);
                        return Collections.singletonList(
                                new SessionBatch(
                                        recovered,
                                        Collections.singletonList(message),
                                        1,
                                        "byte:0",
                                        6L));
                    }
                };

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(source), repository)) {
            service.runOnce();
        }

        assertThat(repository.committedIdentifier).isEqualTo(7L);
        assertThat(repository.committedBatches).hasSize(1);
        assertThat(repository.committedBatches.get(0).session().sourceCursor())
                .isEqualTo("byte:10");
    }

    @Test
    void doesNotCommitAnIncompleteMultiSessionRecovery() throws Exception {
        SessionKey firstKey = new SessionKey("codex", "s1");
        SessionKey secondKey = new SessionKey("codex", "s2");
        FakeRepository repository =
                new FakeRepository(
                        pendingSession(firstKey, "byte:10"),
                        pendingSession(secondKey, "byte:20"));
        ConversationSource source =
                new ConversationSource() {
                    @Override
                    public String sourceType() {
                        return "codex";
                    }

                    @Override
                    public List<SessionBatch> scan(
                            Map<SessionKey, ChatSession> checkpoints,
                            int maxRecords,
                            Set<SessionKey> onlySessions) {
                        ChatSession recovered =
                                checkpoints
                                        .get(firstKey)
                                        .withCheckpoint("byte:10", 6L, Instant.EPOCH);
                        return Collections.singletonList(
                                new SessionBatch(
                                        recovered,
                                        Collections.singletonList(message(firstKey, "m1")),
                                        1,
                                        "byte:0",
                                        6L));
                    }
                };

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(source), repository)) {
            service.runOnce();
        }

        assertThat(repository.commitCalls).isZero();
    }

    @Test
    void freezesTheBatchAfterAnUncertainCommitFailure() throws Exception {
        SessionKey key = new SessionKey("codex", "s1");
        FakeRepository repository = new FakeRepository();
        repository.failNextCommit = true;
        int[] scans = {0};
        ConversationSource source =
                new ConversationSource() {
                    @Override
                    public String sourceType() {
                        return "codex";
                    }

                    @Override
                    public List<SessionBatch> scan(
                            Map<SessionKey, ChatSession> checkpoints,
                            int maxRecords,
                            Set<SessionKey> onlySessions) {
                        scans[0]++;
                        ChatSession session =
                                new ChatSession(
                                        key,
                                        "title",
                                        "/tmp",
                                        false,
                                        "/tmp/s1.jsonl",
                                        "byte:10",
                                        -1L,
                                        Instant.EPOCH,
                                        Instant.EPOCH,
                                        Instant.EPOCH,
                                        Instant.EPOCH);
                        return Collections.singletonList(
                                new SessionBatch(
                                        session,
                                        Collections.singletonList(message(key, "m1")),
                                        1,
                                        null,
                                        -1L));
                    }
                };

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(source), repository)) {
            assertThatThrownBy(service::runOnce).isInstanceOf(Exception.class);
            assertThat(service.pendingData().commitIdentifier()).isZero();
            assertThat(service.pendingData().batches()).hasSize(1);
            assertThat(service.pendingData().batches().get(0).messages())
                    .extracting(ChatMessage::messageId)
                    .containsExactly("m1");
            service.runOnce();
        }

        assertThat(scans[0]).isEqualTo(1);
        assertThat(repository.commitCalls).isEqualTo(2);
        assertThat(repository.committedBatches.get(0).session().sourceCursor())
                .isEqualTo("byte:10");
    }

    @Test
    void replaysTheLocalWalWithoutReadingADeletedSource() throws Exception {
        SessionKey key = new SessionKey("codex", "wal-session");
        ChatSession session =
                new ChatSession(
                        key,
                        "title",
                        "/tmp",
                        false,
                        "/deleted/source.jsonl",
                        "byte:10",
                        -1L,
                        Instant.EPOCH,
                        Instant.EPOCH,
                        Instant.EPOCH,
                        Instant.EPOCH);
        ChatMessage message =
                new ChatMessage(
                        "wal-message",
                        key,
                        1L,
                        "user",
                        "message",
                        "{\"text\":\"persisted\"}",
                        Collections.singletonList(
                                AttachmentPayload.of(new byte[] {1, 2, 3})),
                        Instant.EPOCH,
                        Instant.EPOCH);
        ConversationSource initialSource =
                new ConversationSource() {
                    @Override
                    public String sourceType() {
                        return "codex";
                    }

                    @Override
                    public List<SessionBatch> scan(
                            Map<SessionKey, ChatSession> checkpoints,
                            int maxRecords,
                            Set<SessionKey> onlySessions) {
                        return Collections.singletonList(
                                new SessionBatch(
                                        session,
                                        Collections.singletonList(message),
                                        1,
                                        null,
                                        -1L));
                    }
                };
        PendingBatchStore store =
                new PendingBatchStore(
                        tempDir.resolve("wal"), "collector-test", "table-pair-a");
        FakeRepository firstRepository = new FakeRepository();
        firstRepository.failNextCommit = true;
        CollectorService crashed =
                new CollectorService(
                        config(),
                        Collections.singletonList(initialSource),
                        firstRepository,
                        store);

        assertThatThrownBy(crashed::runOnce).isInstanceOf(Exception.class);
        assertThat(store.pendingFile()).isRegularFile();
        assertThatThrownBy(
                        () ->
                                new PendingBatchStore(
                                                tempDir.resolve("wal"),
                                                "collector-test",
                                                "table-pair-b")
                                        .load())
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("different Catalog table pair");

        PendingBatchStore corruptStore =
                new PendingBatchStore(
                        tempDir.resolve("corrupt-wal"),
                        "collector-test",
                        "table-pair-a");
        Files.createDirectories(corruptStore.pendingFile().getParent());
        Files.copy(store.pendingFile(), corruptStore.pendingFile());
        byte[] corrupted = Files.readAllBytes(corruptStore.pendingFile());
        corrupted[12] ^= 0x01;
        Files.write(corruptStore.pendingFile(), corrupted);
        assertThatThrownBy(corruptStore::load)
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("checksum mismatch");

        ConversationSource deletedSource =
                new ConversationSource() {
                    @Override
                    public String sourceType() {
                        return "codex";
                    }

                    @Override
                    public List<SessionBatch> scan(
                            Map<SessionKey, ChatSession> checkpoints,
                            int maxRecords,
                            Set<SessionKey> onlySessions) {
                        throw new AssertionError("the deleted source must not be scanned");
                    }
                };
        FakeRepository restartedRepository = new FakeRepository();
        try (CollectorService restarted =
                new CollectorService(
                        config(),
                        Collections.singletonList(deletedSource),
                        restartedRepository,
                        store)) {
            restarted.runOnce();
        }

        assertThat(restartedRepository.committedIdentifier).isZero();
        assertThat(restartedRepository.committedBatches).hasSize(1);
        assertThat(
                        restartedRepository
                                .committedBatches
                                .get(0)
                                .messages()
                                .get(0)
                                .attachments()
                                .get(0)
                                .bytes())
                .isEqualTo(new byte[] {1, 2, 3});
        assertThat(store.pendingFile()).doesNotExist();
    }

    private static ChatSession pendingSession(SessionKey key, String targetCursor) {
        return new ChatSession(
                key,
                "title",
                "/tmp",
                false,
                "/tmp/" + key.sessionId() + ".jsonl",
                "byte:0",
                6L,
                7L,
                targetCursor,
                Instant.EPOCH,
                Instant.EPOCH,
                null,
                Instant.EPOCH);
    }

    private static ChatMessage message(SessionKey key, String id) {
        return new ChatMessage(
                id,
                key,
                1L,
                "user",
                "message",
                "{}",
                Collections.emptyList(),
                Instant.EPOCH,
                Instant.EPOCH);
    }

    private static ConversationSource sourceWithOneMessage(
            String name, SessionKey key, List<String> calls, List<Integer> budgets) {
        return new ConversationSource() {
            @Override
            public String sourceType() {
                return key.sourceType();
            }

            @Override
            public List<SessionBatch> scan(
                    Map<SessionKey, ChatSession> checkpoints,
                    int maxRecords,
                    Set<SessionKey> onlySessions) {
                calls.add(name);
                budgets.add(maxRecords);
                ChatSession session =
                        new ChatSession(
                                key,
                                name,
                                "/tmp",
                                false,
                                "/tmp/" + key.sessionId() + ".jsonl",
                                "byte:1",
                                -1L,
                                Instant.EPOCH,
                                Instant.EPOCH,
                                Instant.EPOCH,
                                Instant.EPOCH);
                return Collections.singletonList(
                        new SessionBatch(
                                session,
                                Collections.singletonList(message(key, name)),
                                1,
                                null,
                                -1L));
            }
        };
    }

    private static ConversationSource emptyRecordingSource(String name, List<String> calls) {
        return new ConversationSource() {
            @Override
            public String sourceType() {
                return name;
            }

            @Override
            public List<SessionBatch> scan(
                    Map<SessionKey, ChatSession> checkpoints,
                    int maxRecords,
                    Set<SessionKey> onlySessions) {
                calls.add(name);
                return Collections.emptyList();
            }
        };
    }

    private ProjectConfig config() {
        return config(100);
    }

    private ProjectConfig config(int maxBufferRecords) {
        return new ProjectConfig(
                "db",
                "sessions",
                "messages",
                Duration.ofMinutes(5),
                Duration.ofMinutes(5),
                false,
                "collector-test",
                new SourceConfig(true, tempDir),
                new SourceConfig(false, tempDir),
                true,
                false,
                1024,
                100,
                maxBufferRecords,
                0,
                Duration.ofSeconds(1));
    }

    private static final class FakeRepository implements ChatRepository {
        private final Map<SessionKey, ChatSession> sessions = new HashMap<>();
        private long committedIdentifier = -1L;
        private List<SessionBatch> committedBatches = Collections.emptyList();

        private int commitCalls;
        private boolean failNextCommit;

        private FakeRepository(ChatSession... initialSessions) {
            for (ChatSession session : initialSessions) {
                sessions.put(session.key(), session);
            }
        }

        @Override
        public void initialize() {}

        @Override
        public Map<SessionKey, ChatSession> loadSessions() {
            return new HashMap<>(sessions);
        }

        @Override
        public void commit(long commitIdentifier, List<SessionBatch> batches) {
            commitCalls++;
            if (failNextCommit) {
                failNextCommit = false;
                throw new IllegalStateException("uncertain test failure");
            }
            this.committedIdentifier = commitIdentifier;
            this.committedBatches = batches;
        }

        @Override
        public void close() {}
    }
}
