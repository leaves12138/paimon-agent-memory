package org.apache.paimon.agent.service;

import java.time.Instant;

/** Immutable runtime status exposed by the local dashboard. */
public final class CollectorStatus {

    private final boolean running;
    private final Instant startedAt;
    private final Instant lastScanAt;
    private final Instant lastCommitAt;
    private final Instant lastErrorAt;
    private final String lastError;
    private final int pendingSessions;
    private final int pendingMessages;

    public CollectorStatus(
            boolean running,
            Instant startedAt,
            Instant lastScanAt,
            Instant lastCommitAt,
            Instant lastErrorAt,
            String lastError,
            int pendingSessions,
            int pendingMessages) {
        this.running = running;
        this.startedAt = startedAt;
        this.lastScanAt = lastScanAt;
        this.lastCommitAt = lastCommitAt;
        this.lastErrorAt = lastErrorAt;
        this.lastError = lastError;
        this.pendingSessions = pendingSessions;
        this.pendingMessages = pendingMessages;
    }

    public static CollectorStatus offline() {
        return new CollectorStatus(false, null, null, null, null, null, 0, 0);
    }

    public boolean running() {
        return running;
    }

    public Instant startedAt() {
        return startedAt;
    }

    public Instant lastScanAt() {
        return lastScanAt;
    }

    public Instant lastCommitAt() {
        return lastCommitAt;
    }

    public Instant lastErrorAt() {
        return lastErrorAt;
    }

    public String lastError() {
        return lastError;
    }

    public int pendingSessions() {
        return pendingSessions;
    }

    public int pendingMessages() {
        return pendingMessages;
    }
}
