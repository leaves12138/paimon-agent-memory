package org.apache.paimon.agent.service;

import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.config.SourceConfig;
import org.apache.paimon.agent.dashboard.AttachmentData;
import org.apache.paimon.agent.dashboard.DashboardDataStore;
import org.apache.paimon.agent.dashboard.DashboardMessage;
import org.apache.paimon.agent.dashboard.DashboardMessageDetail;
import org.apache.paimon.agent.dashboard.DashboardOverview;
import org.apache.paimon.agent.dashboard.DashboardPage;
import org.apache.paimon.agent.dashboard.DashboardSession;
import org.apache.paimon.agent.dashboard.DashboardStorageStatus;
import org.apache.paimon.agent.dashboard.LiveDashboardDataStore;
import org.apache.paimon.agent.dashboard.MessageQuery;
import org.apache.paimon.agent.dashboard.SessionQuery;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.sink.ChatRepository;
import org.apache.paimon.agent.source.ConversationSource;
import org.apache.paimon.agent.source.SourceCursors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CollectorServiceTest {

    @TempDir Path tempDir;

    @Test
    void exposesImmutableDashboardSnapshotWhileRepositoryCommitIsBlocked() throws Exception {
        SessionKey key = new SessionKey("codex", "blocked-commit");
        BlockingRepository repository = new BlockingRepository();
        ConversationSource source =
                sourceWithOneMessage(
                        "blocked commit", key, new ArrayList<>(), new ArrayList<>());
        CollectorService service =
                new CollectorService(
                        config(100, 1), Collections.singletonList(source), repository);
        ExecutorService collector = Executors.newSingleThreadExecutor();
        ExecutorService dashboardReaders = Executors.newFixedThreadPool(4);
        LiveDashboardDataStore dashboard =
                new LiveDashboardDataStore(
                        new EmptyDashboardDataStore(),
                        service::pendingData,
                        service::commitGeneration,
                        20);
        Future<?> collection = collector.submit(() -> {
            service.runOnce();
            return null;
        });

        try {
            assertThat(repository.commitStarted.await(5, TimeUnit.SECONDS)).isTrue();

            Future<PendingDataSnapshot> pendingRead =
                    dashboardReaders.submit(service::pendingData);
            Future<Long> generationRead =
                    dashboardReaders.submit(service::commitGeneration);
            Future<CollectorStatus> statusRead = dashboardReaders.submit(service::status);
            Future<DashboardPage<DashboardSession>> sessionRead =
                    dashboardReaders.submit(
                            () ->
                                    dashboard.listSessions(
                                            new SessionQuery(null, null, null, 1, 20)));

            PendingDataSnapshot snapshot = pendingRead.get(1, TimeUnit.SECONDS);
            assertThat(generationRead.get(1, TimeUnit.SECONDS)).isZero();
            CollectorStatus status = statusRead.get(1, TimeUnit.SECONDS);
            DashboardPage<DashboardSession> page = sessionRead.get(1, TimeUnit.SECONDS);

            assertThat(snapshot.commitIdentifier()).isZero();
            assertThat(snapshot.batches()).hasSize(1);
            assertThat(snapshot.batches().get(0).messages())
                    .extracting(ChatMessage::messageId)
                    .containsExactly("blocked commit");
            assertThat(status.pendingSessions()).isOne();
            assertThat(status.pendingMessages()).isOne();
            assertThatThrownBy(() -> snapshot.batches().clear())
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> snapshot.batches().get(0).messages().clear())
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThat(page.getItems())
                    .extracting(DashboardSession::getSessionId)
                    .containsExactly("blocked-commit");
            assertThat(page.getItems())
                    .extracting(DashboardSession::getStorageStatus)
                    .containsExactly(DashboardStorageStatus.PENDING);

            repository.releaseCommit.countDown();
            collection.get(5, TimeUnit.SECONDS);

            assertThat(service.pendingData().isEmpty()).isTrue();
            assertThat(service.commitGeneration()).isEqualTo(1L);
            assertThat(snapshot.batches())
                    .flatExtracting(SessionBatch::messages)
                    .extracting(ChatMessage::messageId)
                    .containsExactly("blocked commit");
        } finally {
            repository.releaseCommit.countDown();
            collection.cancel(true);
            dashboardReaders.shutdownNow();
            collector.shutdownNow();
            dashboard.close();
            service.close();
        }
    }

    @Test
    void exposesSnapshotsDuringCommitRetryBackoff() throws Exception {
        SessionKey key = new SessionKey("codex", "retry-backoff");
        FakeRepository repository = new FakeRepository();
        repository.failNextCommit = true;
        ConversationSource source =
                sourceWithOneMessage(
                        "retry backoff", key, new ArrayList<>(), new ArrayList<>());
        CollectorService service =
                new CollectorService(
                        configWithRetry(100, 1),
                        Collections.singletonList(source),
                        repository);
        ExecutorService collector = Executors.newSingleThreadExecutor();
        ExecutorService readers = Executors.newFixedThreadPool(3);
        Future<?> collection = collector.submit(() -> {
            service.runOnce();
            return null;
        });

        try {
            assertThat(repository.firstCommitAttempted.await(5, TimeUnit.SECONDS)).isTrue();

            Future<PendingDataSnapshot> pendingRead = readers.submit(service::pendingData);
            Future<Long> generationRead = readers.submit(service::commitGeneration);
            Future<CollectorStatus> statusRead = readers.submit(service::status);
            assertThat(pendingRead.get(500, TimeUnit.MILLISECONDS).batches()).hasSize(1);
            assertThat(generationRead.get(500, TimeUnit.MILLISECONDS)).isZero();
            assertThat(statusRead.get(500, TimeUnit.MILLISECONDS).pendingMessages()).isOne();

            collection.get(5, TimeUnit.SECONDS);
            assertThat(repository.commitCalls).isEqualTo(2);
            assertThat(service.pendingData().isEmpty()).isTrue();
            assertThat(service.commitGeneration()).isEqualTo(1L);
        } finally {
            collection.cancel(true);
            readers.shutdownNow();
            collector.shutdownNow();
            service.close();
        }
    }

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

        assertThat(calls).containsExactly("first", "second", "second", "first");
        assertThat(budgets).containsExactly(1, 1, 1, 2);
        assertThat(repository.committedBatches)
                .extracting(batch -> batch.session().key())
                .containsExactly(firstKey, secondKey);
    }

    @Test
    void advancesCommitGenerationOnlyWhenACommitCompletes() throws Exception {
        SessionKey key = new SessionKey("codex", "generation");
        FakeRepository repository = new FakeRepository();
        repository.failNextCommit = true;
        ConversationSource source =
                sourceWithOneMessage(
                        "generation", key, new ArrayList<>(), new ArrayList<>());

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(source), repository)) {
            assertThat(service.commitGeneration()).isZero();

            assertThatThrownBy(service::runOnce)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("uncertain test failure");
            assertThat(service.commitGeneration()).isZero();

            service.runOnce();
            assertThat(service.commitGeneration()).isEqualTo(1L);

            service.runOnce();
            assertThat(service.commitGeneration()).isEqualTo(1L);
        }
    }

    @Test
    void drainsAllChunksAndCommitsBoundedBatchesInOneWakeUp() throws Exception {
        SessionKey key = new SessionKey("codex", "large-session");
        List<Integer> budgets = new ArrayList<>();
        FakeRepository repository = new FakeRepository();
        ConversationSource source = cursorDrainingSource(key, 11, true, budgets);

        try (CollectorService service =
                new CollectorService(
                        config(3, 4), Collections.singletonList(source), repository)) {
            service.runOnce();
            assertThat(service.pendingData().batches()).isEmpty();
        }

        assertThat(budgets).hasSizeGreaterThan(4).allMatch(budget -> budget <= 3);
        assertThat(repository.committedIdentifiers).containsExactly(0L, 1L, 2L);
        assertThat(repository.commits)
                .allSatisfy(
                        commit ->
                                assertThat(
                                                commit.stream()
                                                        .mapToInt(batch -> batch.messages().size())
                                                        .sum())
                                        .isLessThanOrEqualTo(4));
        assertThat(repository.commits.stream()
                        .flatMap(List::stream)
                        .flatMap(batch -> batch.messages().stream())
                        .map(ChatMessage::messageId))
                .containsExactly(
                        "m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10");
        assertThat(repository.committedBatches.get(0).session().sourceCursor())
                .isEqualTo("byte:11");
    }

    @Test
    void commitsSubagentMetadataBackfillWithoutNewSourceRecords() throws Exception {
        SessionKey key = new SessionKey("codex", "subagent-session");
        ChatSession existing =
                new ChatSession(
                        key,
                        "child task",
                        "/tmp",
                        false,
                        "/tmp/subagent-session.jsonl",
                        "byte:42",
                        7L,
                        Instant.EPOCH,
                        Instant.EPOCH,
                        Instant.EPOCH,
                        Instant.EPOCH);
        String subagentSource =
                "{\"thread_spawn\":{\"parent_thread_id\":\"root-session\",\"depth\":1}}";
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
                        ChatSession checkpoint = checkpoints.get(key);
                        if (checkpoint.subagentSourceJson() != null) {
                            return Collections.emptyList();
                        }
                        return Collections.singletonList(
                                new SessionBatch(
                                        checkpoint.withSubagentSourceJson(subagentSource),
                                        Collections.emptyList(),
                                        0,
                                        checkpoint.sourceCursor(),
                                        checkpoint.lastCommitId()));
                    }
                };
        FakeRepository repository = new FakeRepository(existing);

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(source), repository)) {
            service.runOnce();
        }

        assertThat(repository.committedBatches).hasSize(1);
        assertThat(repository.committedBatches.get(0).session().subagentSourceJson())
                .isEqualTo(subagentSource);
        assertThat(repository.committedBatches.get(0).messages()).isEmpty();
    }

    @Test
    void timedOutCollectorWorkerLeavesResourcesOpenUntilASecondSafeClose()
            throws Exception {
        FakeRepository repository = new FakeRepository();
        CountDownLatch collectorStarted = new CountDownLatch(1);
        CountDownLatch collectorInterrupted = new CountDownLatch(1);
        CountDownLatch releaseCollector = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean blockCollector =
                new java.util.concurrent.atomic.AtomicBoolean();
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
                        if (!blockCollector.get()) {
                            return Collections.emptyList();
                        }
                        collectorStarted.countDown();
                        while (releaseCollector.getCount() > 0L) {
                            try {
                                releaseCollector.await();
                            } catch (InterruptedException ignored) {
                                collectorInterrupted.countDown();
                            }
                        }
                        return Collections.emptyList();
                    }
                };
        CollectorService service =
                new CollectorService(
                        config(),
                        Collections.singletonList(source),
                        repository,
                        null,
                        50L);
        service.runOnce();

        blockCollector.set(true);
        service.start();
        assertThat(collectorStarted.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            assertThatThrownBy(service::close)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("collector executor did not terminate");
            assertThat(collectorInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(repository.closed).isFalse();
        } finally {
            releaseCollector.countDown();
            service.close();
        }

        assertThat(repository.closed).isTrue();
    }

    @Test
    void safeScanDoesNotSwallowVirtualMachineErrors() throws Exception {
        FakeRepository repository = new FakeRepository();
        VirtualMachineError fatal = new TestVirtualMachineError();
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
                        throw fatal;
                    }
                };

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(source), repository)) {
            assertThatThrownBy(service::safeScan).isSameAs(fatal);
        }
    }

    @Test
    void safeCommitDoesNotSwallowThreadDeath() throws Exception {
        SessionKey key = new SessionKey("codex", "fatal-commit");
        FakeRepository repository = new FakeRepository();
        AtomicInteger scans = new AtomicInteger();
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
                        if (scans.getAndIncrement() > 0) {
                            return Collections.emptyList();
                        }
                        return Collections.singletonList(
                                new SessionBatch(
                                        new ChatSession(
                                                key,
                                                "title",
                                                "/tmp",
                                                false,
                                                "/tmp/fatal.jsonl",
                                                "byte:1",
                                                -1L,
                                                Instant.EPOCH,
                                                Instant.EPOCH,
                                                Instant.EPOCH,
                                                Instant.EPOCH),
                                        Collections.singletonList(message(key, "fatal-message")),
                                        1,
                                        null,
                                        -1L));
                    }
                };

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(source), repository)) {
            service.safeScan();
            ThreadDeath fatal = new ThreadDeath();
            repository.failNextFatalCommit = fatal;
            assertThatThrownBy(service::safeCommit).isSameAs(fatal);
        }
    }

    @Test
    void drainsIgnoredRawRecordsUntilTheCursorCatchesUp() throws Exception {
        SessionKey key = new SessionKey("codex", "ignored-records");
        List<Integer> budgets = new ArrayList<>();
        FakeRepository repository = new FakeRepository();
        ConversationSource source = cursorDrainingSource(key, 9, false, budgets);

        try (CollectorService service =
                new CollectorService(
                        config(3, 4), Collections.singletonList(source), repository)) {
            service.runOnce();
            assertThat(service.pendingData().batches()).isEmpty();
        }

        assertThat(budgets).containsExactly(3, 3, 3, 3);
        assertThat(repository.commitCalls).isOne();
        assertThat(repository.committedBatches).hasSize(1);
        assertThat(repository.committedBatches.get(0).messages()).isEmpty();
        assertThat(repository.committedBatches.get(0).sourceRecordsRead()).isEqualTo(9);
        assertThat(repository.committedBatches.get(0).session().sourceCursor())
                .isEqualTo("byte:9");
    }

    @Test
    void acceptsAPhysicalAdvanceAcrossAFileReplacementWithARepeatedAnchor()
            throws Exception {
        SessionKey key = new SessionKey("codex", "replaced-file");
        String repeatedAnchor = "same-line-digest";
        ChatSession checkpoint =
                new ChatSession(
                        key,
                        "title",
                        "/tmp",
                        false,
                        "/tmp/replaced-file.jsonl",
                        SourceCursors.file(100L, "old-file", repeatedAnchor),
                        5L,
                        Instant.EPOCH,
                        Instant.EPOCH,
                        Instant.EPOCH,
                        Instant.EPOCH);
        FakeRepository repository = new FakeRepository(checkpoint);
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
                        ChatSession previous = checkpoints.get(key);
                        if (SourceCursors.parseByteOffset(previous.sourceCursor()) >= 200L) {
                            return Collections.emptyList();
                        }
                        ChatSession advanced =
                                new ChatSession(
                                        key,
                                        previous.title(),
                                        previous.cwd(),
                                        previous.archived(),
                                        previous.sourcePath(),
                                        SourceCursors.file(
                                                200L, "new-file", repeatedAnchor),
                                        previous.lastCommitId(),
                                        previous.createdAt(),
                                        previous.updatedAt(),
                                        previous.lastMessageAt(),
                                        previous.ingestedAt());
                        return Collections.singletonList(
                                new SessionBatch(
                                        advanced,
                                        Collections.singletonList(message(key, "repeated-line")),
                                        1,
                                        previous.sourceCursor(),
                                        previous.lastCommitId()));
                    }
                };

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(source), repository)) {
            service.runOnce();
        }

        assertThat(repository.committedIdentifier).isEqualTo(6L);
        assertThat(repository.committedBatches).hasSize(1);
        assertThat(repository.committedBatches.get(0).session().sourceCursor())
                .isEqualTo(SourceCursors.file(200L, "new-file", repeatedAnchor));
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
                        if (onlySessions.isEmpty()) {
                            return Collections.emptyList();
                        }
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
    void completesRecoveryWhenThePendingAnchorMovesInAReplacementFile()
            throws Exception {
        SessionKey key = new SessionKey("codex", "remapped-recovery");
        String targetAnchor = "pending-boundary-anchor";
        ChatSession durable =
                new ChatSession(
                        key,
                        "title",
                        "/tmp",
                        false,
                        "/tmp/remapped-recovery.jsonl",
                        SourceCursors.file(100L, "old-file", "committed-anchor"),
                        6L,
                        7L,
                        SourceCursors.file(200L, "old-file", targetAnchor),
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
                        if (onlySessions.isEmpty()) {
                            return Collections.emptyList();
                        }
                        ChatSession checkpoint = checkpoints.get(key);
                        if ("new-file"
                                .equals(
                                        SourceCursors.parseFileCursor(
                                                        checkpoint.sourceCursor())
                                                .fileKey())) {
                            return Collections.emptyList();
                        }
                        ChatSession recovered =
                                new ChatSession(
                                        key,
                                        checkpoint.title(),
                                        checkpoint.cwd(),
                                        checkpoint.archived(),
                                        checkpoint.sourcePath(),
                                        SourceCursors.file(240L, "new-file", targetAnchor),
                                        checkpoint.lastCommitId(),
                                        checkpoint.createdAt(),
                                        checkpoint.updatedAt(),
                                        checkpoint.lastMessageAt(),
                                        checkpoint.ingestedAt());
                        return Collections.singletonList(
                                new SessionBatch(
                                        recovered,
                                        Collections.singletonList(
                                                message(key, "recovered-message")),
                                        1,
                                        checkpoint.sourceCursor(),
                                        checkpoint.lastCommitId()));
                    }
                };

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(source), repository)) {
            service.runOnce();
            assertThat(service.pendingData().batches()).isEmpty();
        }

        assertThat(repository.committedIdentifiers).containsExactly(7L);
        assertThat(repository.committedBatches).hasSize(1);
        assertThat(repository.committedBatches.get(0).session().sourceCursor())
                .isEqualTo(SourceCursors.file(240L, "new-file", targetAnchor));
    }

    @Test
    void doesNotCommitAnIncompleteMultiSessionRecovery() throws Exception {
        SessionKey firstKey = new SessionKey("codex", "s1");
        SessionKey secondKey = new SessionKey("codex", "s2");
        FakeRepository repository =
                new FakeRepository(
                        pendingSession(firstKey, "byte:10"),
                        pendingSession(secondKey, "byte:20"));
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
                        if (scans[0]++ > 0) {
                            return Collections.emptyList();
                        }
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
    void retriesAFrozenBatchAndDrainsTheRemainingTailInTheSameWakeUp() throws Exception {
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
                        ChatSession previous = checkpoints.get(key);
                        int start =
                                Math.toIntExact(
                                        SourceCursors.parseByteOffset(
                                                previous == null
                                                        ? null
                                                        : previous.sourceCursor()));
                        if (start >= 20) {
                            return Collections.emptyList();
                        }
                        scans[0]++;
                        int end = start + 10;
                        ChatSession session =
                                new ChatSession(
                                        key,
                                        "title",
                                        "/tmp",
                                        false,
                                        "/tmp/s1.jsonl",
                                        SourceCursors.byteOffset(end),
                                        previous == null ? -1L : previous.lastCommitId(),
                                        Instant.EPOCH,
                                        Instant.EPOCH,
                                        Instant.EPOCH,
                                        Instant.EPOCH);
                        return Collections.singletonList(
                                new SessionBatch(
                                        session,
                                        Collections.singletonList(
                                                message(key, start == 0 ? "m1" : "m2")),
                                        1,
                                        previous == null ? null : previous.sourceCursor(),
                                        previous == null ? -1L : previous.lastCommitId()));
                    }
                };

        try (CollectorService service =
                new CollectorService(
                        config(100, 1), Collections.singletonList(source), repository)) {
            assertThatThrownBy(service::runOnce).isInstanceOf(Exception.class);
            assertThat(service.pendingData().commitIdentifier()).isZero();
            assertThat(service.pendingData().batches()).hasSize(1);
            assertThat(service.pendingData().batches().get(0).messages())
                    .extracting(ChatMessage::messageId)
                    .containsExactly("m1");
            service.runOnce();
        }

        assertThat(scans[0]).isEqualTo(2);
        assertThat(repository.commitCalls).isEqualTo(3);
        assertThat(repository.committedIdentifiers).containsExactly(0L, 1L);
        assertThat(repository.commits)
                .flatExtracting(batch -> batch)
                .flatExtracting(SessionBatch::messages)
                .extracting(ChatMessage::messageId)
                .containsExactly("m1", "m2");
        assertThat(repository.committedBatches.get(0).session().sourceCursor())
                .isEqualTo("byte:20");
    }

    @Test
    void runOnceRetriesAFrozenWalBeforeOpeningAnUnavailableSource() throws Exception {
        SessionKey key = new SessionKey("codex", "wal-before-open");
        PendingBatchStore store =
                new PendingBatchStore(
                        tempDir.resolve("wal-before-open"),
                        "collector-test",
                        "table-pair-a");
        store.save(0L, Collections.singletonList(frozenBatch(key)));
        FakeRepository repository = new FakeRepository();
        AtomicInteger openCalls = new AtomicInteger();
        ConversationSource unavailable = unavailableSource("codex", openCalls, null);

        try (CollectorService service =
                new CollectorService(
                        config(), Collections.singletonList(unavailable), repository, store)) {
            assertThatThrownBy(service::runOnce)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("source unavailable");

            assertThat(repository.committedIdentifiers).containsExactly(0L);
            assertThat(store.pendingFile()).doesNotExist();
            assertThat(openCalls).hasValue(1);
        }
    }

    @Test
    void acceptsOneLegacyWalIdentityAndWritesThePhysicalIdentityAfterItClears()
            throws Exception {
        Path directory = tempDir.resolve("wal-identity-upgrade");
        List<SessionBatch> batches =
                Collections.singletonList(
                        frozenBatch(new SessionKey("codex", "wal-identity-upgrade")));
        PendingBatchStore legacy =
                new PendingBatchStore(directory, "collector-test", "legacy-rest-identity");
        legacy.save(4L, batches);

        PendingBatchStore upgraded =
                new PendingBatchStore(
                        directory,
                        "collector-test",
                        "physical-table-identity",
                        "legacy-rest-identity");
        assertThat(upgraded.load().identifier()).isEqualTo(4L);
        upgraded.delete(4L);
        upgraded.save(5L, batches);

        assertThat(
                        new PendingBatchStore(
                                        directory,
                                        "collector-test",
                                        "physical-table-identity")
                                .load()
                                .identifier())
                .isEqualTo(5L);
        assertThatThrownBy(legacy::load)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("different Catalog table pair");
    }

    @Test
    void scheduledCommitRetriesAFrozenBatchBeforeASecondSourceOpenFails()
            throws Exception {
        SessionKey key = new SessionKey("codex", "scheduled-before-open");
        FakeRepository repository = new FakeRepository();
        repository.failNextCommit = true;
        AtomicInteger openCalls = new AtomicInteger();
        CountDownLatch failedSecondOpen = new CountDownLatch(1);
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
                        throw new AssertionError("collector must use the opened scan cycle");
                    }

                    @Override
                    public ScanCycle openScanCycle() throws Exception {
                        if (openCalls.getAndIncrement() > 0) {
                            failedSecondOpen.countDown();
                            throw new IOException("source unavailable after commit failure");
                        }
                        return new ScanCycle() {
                            @Override
                            public List<SessionBatch> scan(
                                    Map<SessionKey, ChatSession> checkpoints,
                                    int maxRecords,
                                    Set<SessionKey> onlySessions) {
                                return Collections.singletonList(frozenBatch(key));
                            }
                        };
                    }
                };

        try (CollectorService service =
                new CollectorService(
                        config(100, 1), Collections.singletonList(source), repository)) {
            service.start();

            assertThat(repository.successfulCommit.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failedSecondOpen.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(repository.committedIdentifiers).containsExactly(0L);
            assertThat(openCalls.get()).isGreaterThanOrEqualTo(2);
        }
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
                        Instant.EPOCH,
                        "{\"thread_spawn\":{\"parent_thread_id\":\"root\",\"depth\":1}}")
                        .withProjectless(true);
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
                        if (checkpoints.containsKey(key)) {
                            return Collections.emptyList();
                        }
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

        FakeRepository restartedRepository = new FakeRepository();
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
                        assertThat(restartedRepository.committedIdentifier).isZero();
                        assertThat(checkpoints.get(key).sourceCursor()).isEqualTo("byte:10");
                        return Collections.emptyList();
                    }
                };
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
                                .session()
                                .subagentSourceJson())
                .isEqualTo(session.subagentSourceJson());
        assertThat(
                        restartedRepository
                                .committedBatches
                                .get(0)
                                .session()
                                .projectless())
                .isTrue();
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

    private static SessionBatch frozenBatch(SessionKey key) {
        ChatSession session =
                new ChatSession(
                        key,
                        "title",
                        "/tmp",
                        false,
                        "/tmp/" + key.sessionId() + ".jsonl",
                        "byte:10",
                        -1L,
                        Instant.EPOCH,
                        Instant.EPOCH,
                        Instant.EPOCH,
                        Instant.EPOCH);
        return new SessionBatch(
                session,
                Collections.singletonList(message(key, "frozen-message")),
                1,
                null,
                -1L);
    }

    private static ConversationSource unavailableSource(
            String sourceType, AtomicInteger openCalls, CountDownLatch attemptedOpen) {
        return new ConversationSource() {
            @Override
            public String sourceType() {
                return sourceType;
            }

            @Override
            public List<SessionBatch> scan(
                    Map<SessionKey, ChatSession> checkpoints,
                    int maxRecords,
                    Set<SessionKey> onlySessions) {
                throw new AssertionError("unavailable source must not be scanned");
            }

            @Override
            public ScanCycle openScanCycle() throws Exception {
                openCalls.incrementAndGet();
                if (attemptedOpen != null) {
                    attemptedOpen.countDown();
                }
                throw new IOException("source unavailable");
            }
        };
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
                if (checkpoints.containsKey(key)) {
                    return Collections.emptyList();
                }
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

    private static ConversationSource cursorDrainingSource(
            SessionKey key, int totalRecords, boolean emitMessages, List<Integer> budgets) {
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
                budgets.add(maxRecords);
                ChatSession previous = checkpoints.get(key);
                int start =
                        Math.toIntExact(
                                SourceCursors.parseByteOffset(
                                        previous == null ? null : previous.sourceCursor()));
                if (start >= totalRecords) {
                    return Collections.emptyList();
                }
                int end = Math.min(totalRecords, start + maxRecords);
                List<ChatMessage> messages = new ArrayList<>();
                if (emitMessages) {
                    for (int index = start; index < end; index++) {
                        messages.add(message(key, "m" + index));
                    }
                }
                ChatSession session =
                        new ChatSession(
                                key,
                                "title",
                                "/tmp",
                                false,
                                "/tmp/" + key.sessionId() + ".jsonl",
                                SourceCursors.byteOffset(end),
                                previous == null ? -1L : previous.lastCommitId(),
                                Instant.EPOCH,
                                Instant.EPOCH,
                                Instant.EPOCH,
                                Instant.EPOCH);
                return Collections.singletonList(
                        new SessionBatch(
                                session,
                                messages,
                                end - start,
                                previous == null ? null : previous.sourceCursor(),
                                previous == null ? -1L : previous.lastCommitId()));
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
        return config(100, maxBufferRecords);
    }

    private ProjectConfig config(int maxScanRecordsPerSource, int maxBufferRecords) {
        return config(maxScanRecordsPerSource, maxBufferRecords, 0, Duration.ofSeconds(1));
    }

    private ProjectConfig configWithRetry(
            int maxScanRecordsPerSource, int maxBufferRecords) {
        return config(maxScanRecordsPerSource, maxBufferRecords, 1, Duration.ofSeconds(1));
    }

    private ProjectConfig config(
            int maxScanRecordsPerSource,
            int maxBufferRecords,
            int maxRetryAttempts,
            Duration initialRetryDelay) {
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
                maxScanRecordsPerSource,
                maxBufferRecords,
                maxRetryAttempts,
                initialRetryDelay);
    }

    private static final class BlockingRepository implements ChatRepository {
        private final CountDownLatch commitStarted = new CountDownLatch(1);
        private final CountDownLatch releaseCommit = new CountDownLatch(1);

        @Override
        public void initialize() {}

        @Override
        public Map<SessionKey, ChatSession> loadSessions() {
            return Collections.emptyMap();
        }

        @Override
        public void commit(long commitIdentifier, List<SessionBatch> batches)
                throws Exception {
            commitStarted.countDown();
            if (!releaseCommit.await(10, TimeUnit.SECONDS)) {
                throw new IOException("timed out waiting to release blocked commit");
            }
        }

        @Override
        public void close() {}
    }

    private static final class EmptyDashboardDataStore implements DashboardDataStore {

        @Override
        public DashboardOverview overview() {
            return new DashboardOverview(
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    null);
        }

        @Override
        public DashboardPage<DashboardSession> listSessions(SessionQuery query) {
            return new DashboardPage<>(
                    Collections.emptyList(), query.getPage(), query.getPageSize(), 0L);
        }

        @Override
        public DashboardPage<DashboardMessage> listMessages(MessageQuery query) {
            return new DashboardPage<>(
                    Collections.emptyList(), query.getPage(), query.getPageSize(), 0L);
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
    }

    private static final class FakeRepository implements ChatRepository {
        private final Map<SessionKey, ChatSession> sessions = new HashMap<>();
        private long committedIdentifier = -1L;
        private List<SessionBatch> committedBatches = Collections.emptyList();

        private int commitCalls;
        private boolean failNextCommit;
        private Error failNextFatalCommit;
        private volatile boolean closed;
        private final List<Long> committedIdentifiers = new ArrayList<>();
        private final List<List<SessionBatch>> commits = new ArrayList<>();
        private final CountDownLatch firstCommitAttempted = new CountDownLatch(1);
        private final CountDownLatch successfulCommit = new CountDownLatch(1);

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
            firstCommitAttempted.countDown();
            if (failNextFatalCommit != null) {
                Error fatal = failNextFatalCommit;
                failNextFatalCommit = null;
                throw fatal;
            }
            if (failNextCommit) {
                failNextCommit = false;
                throw new IllegalStateException("uncertain test failure");
            }
            this.committedIdentifier = commitIdentifier;
            this.committedBatches = batches;
            this.committedIdentifiers.add(commitIdentifier);
            this.commits.add(new ArrayList<>(batches));
            this.successfulCommit.countDown();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class TestVirtualMachineError extends VirtualMachineError {
        private static final long serialVersionUID = 1L;
    }
}
