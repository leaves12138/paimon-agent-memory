package org.apache.paimon.agent.dashboard;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Aggregate values derived without reading message attachment bytes. */
public final class DashboardOverview {

    private final long sessionCount;
    private final long messageCount;
    private final long activeSessionCount;
    private final long archivedSessionCount;
    private final long pendingSessionCount;
    private final Map<String, Long> sessionCountBySource;
    private final Map<String, Long> messageCountBySource;
    private final Instant lastIngestedAt;
    private final boolean sessionCountTruncated;
    private final boolean messageCountTruncated;

    public DashboardOverview(
            long sessionCount,
            long messageCount,
            long activeSessionCount,
            long archivedSessionCount,
            long pendingSessionCount,
            Map<String, Long> sessionCountBySource,
            Map<String, Long> messageCountBySource,
            Instant lastIngestedAt) {
        this(
                sessionCount,
                messageCount,
                activeSessionCount,
                archivedSessionCount,
                pendingSessionCount,
                sessionCountBySource,
                messageCountBySource,
                lastIngestedAt,
                false);
    }

    public DashboardOverview(
            long sessionCount,
            long messageCount,
            long activeSessionCount,
            long archivedSessionCount,
            long pendingSessionCount,
            Map<String, Long> sessionCountBySource,
            Map<String, Long> messageCountBySource,
            Instant lastIngestedAt,
            boolean truncated) {
        this(
                sessionCount,
                messageCount,
                activeSessionCount,
                archivedSessionCount,
                pendingSessionCount,
                sessionCountBySource,
                messageCountBySource,
                lastIngestedAt,
                truncated,
                truncated);
    }

    public DashboardOverview(
            long sessionCount,
            long messageCount,
            long activeSessionCount,
            long archivedSessionCount,
            long pendingSessionCount,
            Map<String, Long> sessionCountBySource,
            Map<String, Long> messageCountBySource,
            Instant lastIngestedAt,
            boolean sessionCountTruncated,
            boolean messageCountTruncated) {
        this.sessionCount = sessionCount;
        this.messageCount = messageCount;
        this.activeSessionCount = activeSessionCount;
        this.archivedSessionCount = archivedSessionCount;
        this.pendingSessionCount = pendingSessionCount;
        this.sessionCountBySource =
                Collections.unmodifiableMap(new LinkedHashMap<>(sessionCountBySource));
        this.messageCountBySource =
                Collections.unmodifiableMap(new LinkedHashMap<>(messageCountBySource));
        this.lastIngestedAt = lastIngestedAt;
        this.sessionCountTruncated = sessionCountTruncated;
        this.messageCountTruncated = messageCountTruncated;
    }

    public long getSessionCount() {
        return sessionCount;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public long getActiveSessionCount() {
        return activeSessionCount;
    }

    public long getArchivedSessionCount() {
        return archivedSessionCount;
    }

    public long getPendingSessionCount() {
        return pendingSessionCount;
    }

    public Map<String, Long> getSessionCountBySource() {
        return sessionCountBySource;
    }

    public Map<String, Long> getMessageCountBySource() {
        return messageCountBySource;
    }

    public Instant getLastIngestedAt() {
        return lastIngestedAt;
    }

    public boolean isTruncated() {
        return sessionCountTruncated || messageCountTruncated;
    }

    public boolean isSessionCountTruncated() {
        return sessionCountTruncated;
    }

    public boolean isMessageCountTruncated() {
        return messageCountTruncated;
    }
}
