package org.apache.paimon.agent.service;

import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.sink.ChatRepository;
import org.apache.paimon.agent.source.ConversationSource;
import org.apache.paimon.agent.source.SourceCursors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Coordinates independent scan and commit intervals on one serialized event loop. */
public final class CollectorService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CollectorService.class);

    private final ProjectConfig config;
    private final List<ConversationSource> sources;
    private final ChatRepository repository;
    private final PendingBatchStore batchStore;
    private final ScheduledExecutorService executor;
    private final Map<SessionKey, ChatSession> committedSessions;
    private final Map<SessionKey, PendingBatch> pending;
    private final Instant startedAt;

    private long nextCommitIdentifier;
    private int nextSourceIndex;
    private boolean pendingFrozen;
    private boolean closed;
    private boolean running;
    private Instant lastScanAt;
    private Instant lastCommitAt;
    private Instant lastErrorAt;
    private String lastError;

    public CollectorService(
            ProjectConfig config,
            List<ConversationSource> sources,
            ChatRepository repository)
            throws Exception {
        this(config, sources, repository, null);
    }

    public CollectorService(
            ProjectConfig config,
            List<ConversationSource> sources,
            ChatRepository repository,
            PendingBatchStore batchStore)
            throws Exception {
        this.config = config;
        this.sources = new ArrayList<>(sources);
        this.repository = repository;
        this.batchStore = batchStore;
        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "paimon-agent-loop");
                            thread.setDaemon(false);
                            return thread;
                        });
        this.committedSessions = new HashMap<>(repository.loadSessions());
        this.pending = new LinkedHashMap<>();
        this.startedAt = Instant.now();
        Set<Long> pendingIdentifiers = new LinkedHashSet<>();
        for (ChatSession session : committedSessions.values()) {
            if (session.hasPendingCommit()) {
                pendingIdentifiers.add(session.pendingCommitId());
            }
        }
        if (pendingIdentifiers.size() > 1) {
            throw new IllegalStateException(
                    "Sessions contain multiple unfinished commit identifiers: "
                            + pendingIdentifiers);
        }
        this.nextCommitIdentifier =
                pendingIdentifiers.isEmpty()
                        ? committedSessions.values().stream()
                                        .mapToLong(ChatSession::lastCommitId)
                                        .max()
                                        .orElse(-1L)
                                + 1L
                        : pendingIdentifiers.iterator().next();
        restoreFrozenBatch(pendingIdentifiers);
    }

    private void restoreFrozenBatch(Set<Long> pendingIdentifiers) throws Exception {
        if (batchStore == null) {
            return;
        }
        PendingBatchStore.StoredCommit stored = batchStore.load();
        if (stored == null) {
            return;
        }
        if (!pendingIdentifiers.isEmpty()
                && !pendingIdentifiers.contains(stored.identifier())) {
            throw new IllegalStateException(
                    "Paimon pending commit "
                            + pendingIdentifiers
                            + " does not match local WAL commit "
                            + stored.identifier());
        }
        long latestCommitted =
                committedSessions.values().stream()
                        .mapToLong(ChatSession::lastCommitId)
                        .max()
                        .orElse(-1L);
        if (latestCommitted > stored.identifier()) {
            throw new IllegalStateException(
                    "Local WAL commit "
                            + stored.identifier()
                            + " is older than the durable Paimon checkpoint "
                            + latestCommitted);
        }
        nextCommitIdentifier = stored.identifier();
        for (SessionBatch batch : stored.batches()) {
            merge(batch);
        }
        if (!pendingIdentifiers.isEmpty()
                && !recoveryComplete(durableRecoverySessions())) {
            throw new IllegalStateException(
                    "Local WAL commit "
                            + stored.identifier()
                            + " does not contain every durable pending session");
        }
        pendingFrozen = true;
        LOG.info(
                "Loaded {} frozen sessions and {} messages for commit {} from the local WAL",
                stored.batches().size(),
                stored.batches().stream().mapToInt(batch -> batch.messages().size()).sum(),
                stored.identifier());
    }

    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("Collector service is already closed");
        }
        running = true;
        executor.scheduleWithFixedDelay(
                this::safeScan,
                0,
                config.scanInterval().toMillis(),
                TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(
                this::safeCommit,
                config.commitInterval().toMillis(),
                config.commitInterval().toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void runOnce() throws Exception {
        synchronized (this) {
            running = true;
        }
        try {
            Map<SessionKey, String> previousRecoveryProgress = incompleteRecoveryProgress();
            while (true) {
                scan();
                Map<SessionKey, String> currentRecoveryProgress = incompleteRecoveryProgress();
                if (currentRecoveryProgress == null) {
                    break;
                }
                if (currentRecoveryProgress.equals(previousRecoveryProgress)) {
                    LOG.warn(
                            "Pending recovery made no progress; leaving its durable boundary for a later run");
                    break;
                }
                previousRecoveryProgress = currentRecoveryProgress;
            }
            commit();
        } catch (Exception failure) {
            recordError(failure);
            throw failure;
        } finally {
            synchronized (this) {
                running = false;
            }
        }
    }

    private void safeScan() {
        try {
            scan();
        } catch (Throwable error) {
            recordError(error);
            LOG.error("Conversation scan failed; checkpoints were not advanced", error);
        }
    }

    private void safeCommit() {
        try {
            commit();
        } catch (Throwable error) {
            recordError(error);
            LOG.error(
                    "Paimon commit {} failed; the same commit identifier will be retried",
                    nextCommitIdentifier,
                    error);
        }
    }

    private synchronized void scan() throws Exception {
        if (closed) {
            return;
        }
        try {
            if (scanInternal()) {
                lastScanAt = Instant.now();
                lastError = null;
                lastErrorAt = null;
            }
        } catch (Exception failure) {
            recordError(failure);
            throw failure;
        }
    }

    private boolean scanInternal() throws Exception {
        if (pendingFrozen) {
            LOG.warn(
                    "Commit {} is awaiting retry; source scanning remains frozen at its fixed boundary",
                    nextCommitIdentifier);
            return false;
        }
        Set<SessionKey> recoverySessions = durableRecoverySessions();
        Map<SessionKey, ChatSession> effectiveCheckpoints = new HashMap<>(committedSessions);
        for (Map.Entry<SessionKey, PendingBatch> entry : pending.entrySet()) {
            if (recoverySessions.contains(entry.getKey())) {
                ChatSession durable = committedSessions.get(entry.getKey());
                effectiveCheckpoints.put(
                        entry.getKey(),
                        entry.getValue()
                                .session
                                .withPendingBoundary(
                                        durable.pendingCommitId(), durable.pendingCursor()));
            } else if (recoverySessions.isEmpty()) {
                effectiveCheckpoints.put(entry.getKey(), entry.getValue().session);
            }
        }

        int remainingBuffer = Math.max(1, config.maxBufferRecords() - pendingMessageCount());
        int sourceCount = sources.size();
        int sourceStart = sourceCount == 0 ? 0 : Math.floorMod(nextSourceIndex, sourceCount);
        if (sourceCount > 0) {
            nextSourceIndex = (sourceStart + 1) % sourceCount;
        }
        for (int sourceOffset = 0; sourceOffset < sourceCount; sourceOffset++) {
            if (remainingBuffer <= 0) {
                break;
            }
            ConversationSource source = sources.get((sourceStart + sourceOffset) % sourceCount);
            int remainingSources = sourceCount - sourceOffset;
            int fairShare = divideRoundingUp(remainingBuffer, remainingSources);
            List<SessionBatch> batches =
                    source.scan(
                            effectiveCheckpoints,
                            Math.min(config.maxScanRecordsPerSource(), fairShare),
                            recoverySessions);
            for (SessionBatch batch : batches) {
                merge(batch);
                effectiveCheckpoints.put(batch.session().key(), batch.session());
                remainingBuffer -= batch.messages().size();
            }
        }

        if (!recoverySessions.isEmpty()) {
            if (!recoveryComplete(recoverySessions)) {
                LOG.warn(
                        "Waiting to reconstruct all sessions for pending commit {} ({}/{})",
                        nextCommitIdentifier,
                        pending.size(),
                        recoverySessions.size());
                return true;
            }
            commit();
            return true;
        }

        if (pendingMessageCount() >= config.maxBufferRecords()) {
            commit();
        }
        return true;
    }

    private void merge(SessionBatch batch) {
        PendingBatch existing = pending.get(batch.session().key());
        if (existing == null) {
            pending.put(
                    batch.session().key(),
                    new PendingBatch(
                            batch.session(),
                            new ArrayList<>(batch.messages()),
                            batch.sourceRecordsRead(),
                            batch.startingCursor(),
                            batch.startingCommitId()));
            return;
        }
        existing.session = batch.session();
        existing.messages.addAll(batch.messages());
        existing.sourceRecordsRead += batch.sourceRecordsRead();
    }

    private synchronized void commit() throws Exception {
        if (closed) {
            return;
        }
        commitPending();
    }

    /** Commits while the caller holds this service's monitor, including final shutdown. */
    private void commitPending() throws Exception {
        if (pending.isEmpty()) {
            return;
        }
        Set<SessionKey> recoverySessions = durableRecoverySessions();
        if (!recoverySessions.isEmpty() && !recoveryComplete(recoverySessions)) {
            LOG.warn(
                    "Not committing partial recovery for commit {} ({}/{} sessions reconstructed)",
                    nextCommitIdentifier,
                    pending.size(),
                    recoverySessions.size());
            return;
        }

        List<SessionBatch> batches = new ArrayList<>();
        for (PendingBatch value : pending.values()) {
            batches.add(
                    new SessionBatch(
                            value.session,
                            value.messages,
                            value.sourceRecordsRead,
                            value.startingCursor,
                            value.startingCommitId));
        }

        long commitIdentifier = nextCommitIdentifier;
        if (batchStore != null) {
            // Persist before Paimon's first pending-session write. A restart can then replay the
            // exact messages and BLOBs even if the mutable local transcript has disappeared.
            batchStore.save(commitIdentifier, batches);
        }

        // Once an identifier has been attempted, its exact source boundaries and messages must
        // remain immutable. An uncertain Paimon outcome may already have committed the append
        // side, so adding newly scanned records to the retry would make filterAndCommit drop them.
        pendingFrozen = true;
        commitWithRetry(commitIdentifier, batches);
        if (batchStore != null) {
            batchStore.delete(commitIdentifier);
        }
        Instant committedAt = Instant.now();
        for (SessionBatch batch : batches) {
            committedSessions.put(
                    batch.session().key(),
                    batch.session()
                            .withCheckpoint(
                                    batch.session().sourceCursor(),
                                    commitIdentifier,
                                    committedAt));
        }
        pending.clear();
        pendingFrozen = false;
        nextCommitIdentifier++;
        lastCommitAt = Instant.now();
        lastError = null;
        lastErrorAt = null;
        LOG.info(
                "Committed {} sessions and {} messages with commit identifier {}",
                batches.size(),
                batches.stream().mapToInt(batch -> batch.messages().size()).sum(),
                commitIdentifier);
    }

    private void commitWithRetry(long commitIdentifier, List<SessionBatch> batches)
            throws Exception {
        long delayMillis = config.initialRetryDelay().toMillis();
        for (int attempt = 0; ; attempt++) {
            try {
                repository.commit(commitIdentifier, batches);
                return;
            } catch (Exception failure) {
                if (attempt >= config.maxRetryAttempts()) {
                    throw failure;
                }
                LOG.warn(
                        "Commit {} failed on attempt {}; retrying with the same identifier in {} ms",
                        commitIdentifier,
                        attempt + 1,
                        delayMillis,
                        failure);
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    failure.addSuppressed(interrupted);
                    throw failure;
                }
                delayMillis = Math.min(60_000L, Math.multiplyExact(delayMillis, 2L));
            }
        }
    }

    private int pendingMessageCount() {
        int count = 0;
        for (PendingBatch batch : pending.values()) {
            count += batch.messages.size();
        }
        return count;
    }

    public synchronized CollectorStatus status() {
        return new CollectorStatus(
                running && !closed,
                startedAt,
                lastScanAt,
                lastCommitAt,
                lastErrorAt,
                lastError,
                pending.size(),
                pendingMessageCount());
    }

    /** Returns a stable copy for the read-only dashboard without exposing mutable buffers. */
    public synchronized PendingDataSnapshot pendingData() {
        if (pending.isEmpty()) {
            return PendingDataSnapshot.empty();
        }
        List<SessionBatch> batches = new ArrayList<>(pending.size());
        for (PendingBatch value : pending.values()) {
            batches.add(
                    new SessionBatch(
                            value.session,
                            value.messages,
                            value.sourceRecordsRead,
                            value.startingCursor,
                            value.startingCommitId));
        }
        return new PendingDataSnapshot(nextCommitIdentifier, batches);
    }

    private synchronized void recordError(Throwable error) {
        lastErrorAt = Instant.now();
        String message = error.getMessage();
        String value =
                error.getClass().getSimpleName()
                        + (message == null || message.trim().isEmpty() ? "" : ": " + message);
        lastError = value.length() <= 1_000 ? value : value.substring(0, 1_000);
    }

    private static int divideRoundingUp(int value, int divisor) {
        return value / divisor + (value % divisor == 0 ? 0 : 1);
    }

    private boolean recoveryComplete(Set<SessionKey> recoverySessions) {
        for (SessionKey key : recoverySessions) {
            PendingBatch recovered = pending.get(key);
            ChatSession durable = committedSessions.get(key);
            if (recovered == null
                    || durable == null
                    || !SourceCursors.samePosition(
                            recovered.session.sourceCursor(), durable.pendingCursor())) {
                return false;
            }
        }
        return true;
    }

    private Set<SessionKey> durableRecoverySessions() {
        Set<SessionKey> recoverySessions = new LinkedHashSet<>();
        for (ChatSession session : committedSessions.values()) {
            if (session.hasPendingCommit()) {
                recoverySessions.add(session.key());
            }
        }
        return recoverySessions;
    }

    /** Returns null when no more recovery scanning is needed, otherwise its progress signature. */
    private synchronized Map<SessionKey, String> incompleteRecoveryProgress() {
        Set<SessionKey> recoverySessions = durableRecoverySessions();
        if (recoverySessions.isEmpty() || recoveryComplete(recoverySessions)) {
            return null;
        }
        Map<SessionKey, String> progress = new HashMap<>();
        for (SessionKey key : recoverySessions) {
            PendingBatch batch = pending.get(key);
            progress.put(key, batch == null ? null : batch.session.sourceCursor());
        }
        return progress;
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            running = false;
        }

        // Do not hold the service monitor while waiting: an already-dispatched scheduled task
        // may be waiting to enter scan() or commit(). Both methods now return immediately once
        // closed, while an operation that started earlier is allowed to finish safely.
        executor.shutdown();
        InterruptedException interrupted = null;
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            interrupted = e;
            executor.shutdownNow();
        }

        Exception failure = null;
        synchronized (this) {
            try {
                commitPending();
            } catch (Exception e) {
                failure = e;
            }
            try {
                repository.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (interrupted != null) {
            Thread.currentThread().interrupt();
            if (failure == null) {
                failure = interrupted;
            } else {
                failure.addSuppressed(interrupted);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static final class PendingBatch {
        private ChatSession session;
        private final List<org.apache.paimon.agent.model.ChatMessage> messages;
        private int sourceRecordsRead;
        private final String startingCursor;
        private final long startingCommitId;

        private PendingBatch(
                ChatSession session,
                List<org.apache.paimon.agent.model.ChatMessage> messages,
                int sourceRecordsRead,
                String startingCursor,
                long startingCommitId) {
            this.session = session;
            this.messages = messages;
            this.sourceRecordsRead = sourceRecordsRead;
            this.startingCursor = startingCursor;
            this.startingCommitId = startingCommitId;
        }
    }
}
