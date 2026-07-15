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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Coordinates independent scan and commit intervals on one serialized event loop. */
public final class CollectorService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CollectorService.class);
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 30_000L;

    private final ProjectConfig config;
    private final List<ConversationSource> sources;
    private final ChatRepository repository;
    private final PendingBatchStore batchStore;
    private final ScheduledExecutorService executor;
    private final long shutdownTimeoutMillis;
    private final Object closeLifecycleLock = new Object();
    private final Map<SessionKey, ChatSession> committedSessions;
    private final Map<SessionKey, PendingBatch> pending;
    private final Instant startedAt;

    private long nextCommitIdentifier;
    private long commitGeneration;
    private int nextSourceIndex;
    private boolean pendingFrozen;
    private volatile boolean closed;
    private boolean resourcesClosed;
    private volatile boolean running;
    private Instant lastScanAt;
    private Instant lastCommitAt;
    private Instant lastErrorAt;
    private String lastError;

    public CollectorService(
            ProjectConfig config,
            List<ConversationSource> sources,
            ChatRepository repository)
            throws Exception {
        this(config, sources, repository, null, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
    }

    public CollectorService(
            ProjectConfig config,
            List<ConversationSource> sources,
            ChatRepository repository,
            PendingBatchStore batchStore)
            throws Exception {
        this(config, sources, repository, batchStore, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
    }

    CollectorService(
            ProjectConfig config,
            List<ConversationSource> sources,
            ChatRepository repository,
            PendingBatchStore batchStore,
            long shutdownTimeoutMillis)
            throws Exception {
        this.config = config;
        this.sources = new ArrayList<>(sources);
        this.repository = repository;
        this.batchStore = batchStore;
        if (shutdownTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("shutdownTimeoutMillis must be positive");
        }
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
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
                // The single-thread executor runs the initial scan registered above first, then
                // immediately flushes its final partial chunk. Later flushes use the configured
                // cadence independently from source scans.
                0,
                config.commitInterval().toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void runOnce() throws Exception {
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("Collector service is already closed");
            }
            running = true;
        }
        try {
            drainSources();
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

    void safeScan() {
        try {
            drainSources();
        } catch (Exception error) {
            recordError(error);
            LOG.error("Conversation scan failed; checkpoints were not advanced", error);
        }
    }

    /** Repeats bounded source reads in the same wake-up until every source reports no progress. */
    private void drainSources() throws Exception {
        // A frozen batch may already have appended messages in Paimon. Retry that exact batch
        // before touching any local source: opening a missing or unreadable source must never
        // prevent an otherwise self-contained WAL retry from completing.
        retryFrozenCommit();

        List<ConversationSource.ScanCycle> cycles = new ArrayList<>(sources.size());
        try {
            for (ConversationSource source : sources) {
                cycles.add(
                        Objects.requireNonNull(
                                source.openScanCycle(),
                                "Conversation source returned a null scan cycle: "
                                        + source.sourceType()));
            }

            int progressedPasses = 0;
            while (scan(cycles)) {
                progressedPasses++;
                // Each pass remains bounded by the configured source and in-memory chunk sizes. A
                // full buffer is committed by scanInternal before the next pass continues.
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Conversation backlog drain was interrupted");
                }
            }
            if (progressedPasses > 1) {
                LOG.info(
                        "Caught up conversation sources in {} bounded scan passes during one "
                                + "wake-up",
                        progressedPasses);
            }
        } finally {
            for (int index = cycles.size() - 1; index >= 0; index--) {
                cycles.get(index).close();
            }
        }
    }

    void safeCommit() {
        try {
            // A commit wake-up that unfreezes an uncertain boundary also completes the backlog
            // captured for that same wake-up instead of waiting for the next scan interval.
            if (retryFrozenCommit()) {
                drainSources();
            }
            commit();
        } catch (Exception error) {
            recordError(error);
            LOG.error(
                    "Paimon commit or post-recovery catch-up {} failed; the same frozen commit "
                            + "identifier will be retried when applicable",
                    nextCommitIdentifier,
                    error);
        }
    }

    /** Returns true only when this call successfully completed a previously frozen commit. */
    private synchronized boolean retryFrozenCommit() throws Exception {
        if (closed || !pendingFrozen) {
            return false;
        }
        LOG.info(
                "Retrying frozen commit {} before opening conversation sources",
                nextCommitIdentifier);
        commitPending();
        if (pendingFrozen) {
            throw new IllegalStateException(
                    "Frozen commit " + nextCommitIdentifier + " could not be completed");
        }
        return true;
    }

    private synchronized boolean scan(List<ConversationSource.ScanCycle> cycles)
            throws Exception {
        if (closed) {
            return false;
        }
        try {
            ScanResult result = scanInternal(cycles);
            lastScanAt = Instant.now();
            lastError = null;
            lastErrorAt = null;
            return result == ScanResult.PROGRESSED;
        } catch (Exception failure) {
            recordError(failure);
            throw failure;
        }
    }

    private ScanResult scanInternal(List<ConversationSource.ScanCycle> cycles) throws Exception {
        if (pendingFrozen) {
            LOG.info(
                    "Retrying frozen commit {} before continuing this wake-up's source scan",
                    nextCommitIdentifier);
            // Failure preserves the exact frozen batch and aborts this scan. Success advances the
            // checkpoint before any later source record is read.
            commitPending();
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
        int sourceCount = cycles.size();
        int sourceStart = sourceCount == 0 ? 0 : Math.floorMod(nextSourceIndex, sourceCount);
        if (sourceCount > 0) {
            nextSourceIndex = (sourceStart + 1) % sourceCount;
        }
        boolean madeProgress = false;
        for (int sourceOffset = 0; sourceOffset < sourceCount; sourceOffset++) {
            if (remainingBuffer <= 0) {
                break;
            }
            ConversationSource.ScanCycle cycle =
                    cycles.get((sourceStart + sourceOffset) % sourceCount);
            int remainingSources = sourceCount - sourceOffset;
            int fairShare = divideRoundingUp(remainingBuffer, remainingSources);
            List<SessionBatch> batches =
                    cycle.scan(
                            effectiveCheckpoints,
                            Math.min(config.maxScanRecordsPerSource(), fairShare),
                            recoverySessions);
            for (SessionBatch batch : batches) {
                if (!batchMakesProgress(batch, effectiveCheckpoints, recoverySessions)) {
                    continue;
                }
                merge(batch);
                effectiveCheckpoints.put(batch.session().key(), batch.session());
                remainingBuffer -= batch.messages().size();
                madeProgress = true;
            }
        }

        if (!recoverySessions.isEmpty()) {
            if (!recoveryComplete(recoverySessions)) {
                LOG.warn(
                        "Waiting to reconstruct all sessions for pending commit {} ({}/{})",
                        nextCommitIdentifier,
                        pending.size(),
                        recoverySessions.size());
                return madeProgress ? ScanResult.PROGRESSED : ScanResult.IDLE;
            }
            commit();
            return ScanResult.PROGRESSED;
        }

        if (pendingMessageCount() >= config.maxBufferRecords()) {
            commit();
            return ScanResult.PROGRESSED;
        }
        return madeProgress ? ScanResult.PROGRESSED : ScanResult.IDLE;
    }

    private boolean batchMakesProgress(
            SessionBatch batch,
            Map<SessionKey, ChatSession> effectiveCheckpoints,
            Set<SessionKey> recoverySessions) {
        SessionKey key = batch.session().key();
        ChatSession previous = effectiveCheckpoints.get(key);
        boolean establishesRecovery = recoverySessions.contains(key) && !pending.containsKey(key);
        boolean advancesCursor =
                previous == null
                        || !SourceCursors.samePhysicalPosition(
                                previous.sourceCursor(), batch.session().sourceCursor());
        boolean updatesMetadata = previous == null || sessionStateChanged(previous, batch.session());
        if (establishesRecovery || advancesCursor || updatesMetadata) {
            return true;
        }
        if (batch.sourceRecordsRead() > 0 || !batch.messages().isEmpty()) {
            throw new IllegalStateException(
                    "Source returned records without advancing session cursor for " + key);
        }
        return false;
    }

    private static boolean sessionStateChanged(ChatSession previous, ChatSession current) {
        return !Objects.equals(previous.title(), current.title())
                || !Objects.equals(previous.cwd(), current.cwd())
                || previous.archived() != current.archived()
                || !Objects.equals(previous.sourcePath(), current.sourcePath())
                || !Objects.equals(previous.createdAt(), current.createdAt())
                || !Objects.equals(previous.updatedAt(), current.updatedAt())
                || !Objects.equals(previous.lastMessageAt(), current.lastMessageAt())
                || !Objects.equals(
                        previous.subagentSourceJson(), current.subagentSourceJson())
                || !Objects.equals(previous.projectless(), current.projectless());
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
        commitGeneration++;
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

    /** Monotonically advances after each commit has completed successfully. */
    public synchronized long commitGeneration() {
        return commitGeneration;
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
                    || !SourceCursors.sameLogicalBoundary(
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

    @Override
    public void close() throws Exception {
        synchronized (closeLifecycleLock) {
            if (resourcesClosed) {
                return;
            }
            // Do not acquire the service monitor to publish the stop signal. A source scan holds
            // that monitor while it updates pending state and can itself be the operation which
            // needs shutdownNow's interrupt in order to terminate.
            closed = true;
            running = false;

            executor.shutdown();

            // Never close a Catalog while a previously dispatched task can still use it. A timed
            // out close leaves resources open so a later close call can finish safely.
            ExecutorShutdown collectorShutdown =
                    shutdownAndAwait(executor, "collector executor");
            if (!collectorShutdown.terminated) {
                Exception shutdownFailure = executorShutdownFailure("collector executor");
                InterruptedException interrupted = collectorShutdown.interrupted;
                if (interrupted != null) {
                    Thread.currentThread().interrupt();
                    shutdownFailure = appendFailure(shutdownFailure, interrupted);
                }
                throw shutdownFailure;
            }

            Exception failure = null;
            synchronized (this) {
                try {
                    commitPending();
                } catch (Exception e) {
                    failure = e;
                }
            }
            try {
                repository.close();
            } catch (Exception e) {
                failure = appendFailure(failure, e);
            }
            resourcesClosed = true;

            InterruptedException interrupted = collectorShutdown.interrupted;
            if (interrupted != null) {
                Thread.currentThread().interrupt();
                failure = appendFailure(failure, interrupted);
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private ExecutorShutdown shutdownAndAwait(
            ExecutorService service, String description) {
        service.shutdown();
        InterruptedException interrupted = null;
        boolean terminated = false;
        try {
            terminated =
                    service.awaitTermination(shutdownTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            interrupted = e;
        }
        if (!terminated) {
            service.shutdownNow();
            try {
                terminated =
                        service.awaitTermination(
                                shutdownTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (interrupted == null) {
                    interrupted = e;
                } else {
                    interrupted.addSuppressed(e);
                }
            }
        }
        if (!terminated) {
            LOG.error("{} did not terminate after cancellation", description);
        }
        return new ExecutorShutdown(terminated, interrupted);
    }

    private static Exception executorShutdownFailure(String description) {
        return new IllegalStateException(
                description
                        + " did not terminate; repository resources remain open to avoid "
                        + "a concurrent close");
    }

    private static Exception appendFailure(Exception current, Exception added) {
        if (current == null) {
            return added;
        }
        current.addSuppressed(added);
        return current;
    }

    private static final class ExecutorShutdown {
        private final boolean terminated;
        private final InterruptedException interrupted;

        private ExecutorShutdown(boolean terminated, InterruptedException interrupted) {
            this.terminated = terminated;
            this.interrupted = interrupted;
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

    private enum ScanResult {
        IDLE,
        PROGRESSED
    }
}
