package org.apache.paimon.agent.dashboard;

import java.time.Instant;

/** Display model containing all columns from one session row. */
public final class DashboardSession {

    private final String sourceType;
    private final String sessionId;
    private final String title;
    private final String cwd;
    private final boolean archived;
    private final String sourcePath;
    private final String sourceCursor;
    private final long lastCommitId;
    private final Long pendingCommitId;
    private final String pendingCursor;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final Instant lastMessageAt;
    private final Instant ingestedAt;
    private final String subagentSourceJson;
    private final Boolean projectless;
    private final DashboardStorageStatus storageStatus;

    public DashboardSession(
            String sourceType,
            String sessionId,
            String title,
            String cwd,
            boolean archived,
            String sourcePath,
            String sourceCursor,
            long lastCommitId,
            Long pendingCommitId,
            String pendingCursor,
            Instant createdAt,
            Instant updatedAt,
            Instant lastMessageAt,
            Instant ingestedAt) {
        this(
                sourceType,
                sessionId,
                title,
                cwd,
                archived,
                sourcePath,
                sourceCursor,
                lastCommitId,
                pendingCommitId,
                pendingCursor,
                createdAt,
                updatedAt,
                lastMessageAt,
                ingestedAt,
                null,
                pendingCommitId == null
                        ? DashboardStorageStatus.UPLOADED
                        : DashboardStorageStatus.PENDING);
    }

    public DashboardSession(
            String sourceType,
            String sessionId,
            String title,
            String cwd,
            boolean archived,
            String sourcePath,
            String sourceCursor,
            long lastCommitId,
            Long pendingCommitId,
            String pendingCursor,
            Instant createdAt,
            Instant updatedAt,
            Instant lastMessageAt,
            Instant ingestedAt,
            DashboardStorageStatus storageStatus) {
        this(
                sourceType,
                sessionId,
                title,
                cwd,
                archived,
                sourcePath,
                sourceCursor,
                lastCommitId,
                pendingCommitId,
                pendingCursor,
                createdAt,
                updatedAt,
                lastMessageAt,
                ingestedAt,
                null,
                storageStatus);
    }

    public DashboardSession(
            String sourceType,
            String sessionId,
            String title,
            String cwd,
            boolean archived,
            String sourcePath,
            String sourceCursor,
            long lastCommitId,
            Long pendingCommitId,
            String pendingCursor,
            Instant createdAt,
            Instant updatedAt,
            Instant lastMessageAt,
            Instant ingestedAt,
            String subagentSourceJson,
            DashboardStorageStatus storageStatus) {
        this(
                sourceType,
                sessionId,
                title,
                cwd,
                archived,
                sourcePath,
                sourceCursor,
                lastCommitId,
                pendingCommitId,
                pendingCursor,
                createdAt,
                updatedAt,
                lastMessageAt,
                ingestedAt,
                subagentSourceJson,
                null,
                storageStatus);
    }

    public DashboardSession(
            String sourceType,
            String sessionId,
            String title,
            String cwd,
            boolean archived,
            String sourcePath,
            String sourceCursor,
            long lastCommitId,
            Long pendingCommitId,
            String pendingCursor,
            Instant createdAt,
            Instant updatedAt,
            Instant lastMessageAt,
            Instant ingestedAt,
            String subagentSourceJson,
            Boolean projectless,
            DashboardStorageStatus storageStatus) {
        this.sourceType = sourceType;
        this.sessionId = sessionId;
        this.title = title;
        this.cwd = cwd;
        this.archived = archived;
        this.sourcePath = sourcePath;
        this.sourceCursor = sourceCursor;
        this.lastCommitId = lastCommitId;
        this.pendingCommitId = pendingCommitId;
        this.pendingCursor = pendingCursor;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.lastMessageAt = lastMessageAt;
        this.ingestedAt = ingestedAt;
        this.subagentSourceJson = subagentSourceJson;
        this.projectless = projectless;
        this.storageStatus = storageStatus;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getTitle() {
        return title;
    }

    public String getCwd() {
        return cwd;
    }

    public boolean isArchived() {
        return archived;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getSourceCursor() {
        return sourceCursor;
    }

    public long getLastCommitId() {
        return lastCommitId;
    }

    public Long getPendingCommitId() {
        return pendingCommitId;
    }

    public String getPendingCursor() {
        return pendingCursor;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Instant getLastMessageAt() {
        return lastMessageAt;
    }

    public Instant getIngestedAt() {
        return ingestedAt;
    }

    public String getSubagentSourceJson() {
        return subagentSourceJson;
    }

    public Boolean getProjectless() {
        return projectless;
    }

    public DashboardStorageStatus getStorageStatus() {
        return storageStatus;
    }
}
