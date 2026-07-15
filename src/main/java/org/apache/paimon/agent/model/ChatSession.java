package org.apache.paimon.agent.model;

import java.time.Instant;
import java.util.Objects;

/** Current state written to the ai_chat_sessions primary-key table. */
public final class ChatSession {

    private final SessionKey key;
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

    public ChatSession(
            SessionKey key,
            String title,
            String cwd,
            boolean archived,
            String sourcePath,
            String sourceCursor,
            long lastCommitId,
            Instant createdAt,
            Instant updatedAt,
            Instant lastMessageAt,
            Instant ingestedAt) {
        this(
                key,
                title,
                cwd,
                archived,
                sourcePath,
                sourceCursor,
                lastCommitId,
                null,
                null,
                createdAt,
                updatedAt,
                lastMessageAt,
                ingestedAt,
                null);
    }

    public ChatSession(
            SessionKey key,
            String title,
            String cwd,
            boolean archived,
            String sourcePath,
            String sourceCursor,
            long lastCommitId,
            Instant createdAt,
            Instant updatedAt,
            Instant lastMessageAt,
            Instant ingestedAt,
            String subagentSourceJson) {
        this(
                key,
                title,
                cwd,
                archived,
                sourcePath,
                sourceCursor,
                lastCommitId,
                null,
                null,
                createdAt,
                updatedAt,
                lastMessageAt,
                ingestedAt,
                subagentSourceJson);
    }

    public ChatSession(
            SessionKey key,
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
                key,
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
                null);
    }

    public ChatSession(
            SessionKey key,
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
            String subagentSourceJson) {
        this(
                key,
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
                null);
    }

    public ChatSession(
            SessionKey key,
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
            Boolean projectless) {
        this.key = Objects.requireNonNull(key, "key");
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
    }

    public SessionKey key() {
        return key;
    }

    public String title() {
        return title;
    }

    public String cwd() {
        return cwd;
    }

    public boolean archived() {
        return archived;
    }

    public String sourcePath() {
        return sourcePath;
    }

    public String sourceCursor() {
        return sourceCursor;
    }

    public long lastCommitId() {
        return lastCommitId;
    }

    public Long pendingCommitId() {
        return pendingCommitId;
    }

    public String pendingCursor() {
        return pendingCursor;
    }

    public boolean hasPendingCommit() {
        return pendingCommitId != null;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    public Instant lastMessageAt() {
        return lastMessageAt;
    }

    public Instant ingestedAt() {
        return ingestedAt;
    }

    public String subagentSourceJson() {
        return subagentSourceJson;
    }

    public Boolean projectless() {
        return projectless;
    }

    public ChatSession withSubagentSourceJson(String value) {
        return new ChatSession(
                key,
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
                value,
                projectless);
    }

    public ChatSession withProjectless(Boolean value) {
        return new ChatSession(
                key,
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
                value);
    }

    public ChatSession withCheckpoint(String cursor, long commitId, Instant ingestionTime) {
        return new ChatSession(
                key,
                title,
                cwd,
                archived,
                sourcePath,
                cursor,
                commitId,
                null,
                null,
                createdAt,
                updatedAt,
                lastMessageAt,
                ingestionTime,
                subagentSourceJson,
                projectless);
    }

    public ChatSession withPendingCommit(
            String committedCursor,
            long committedId,
            long pendingId,
            String targetCursor,
            Instant ingestionTime) {
        return new ChatSession(
                key,
                title,
                cwd,
                archived,
                sourcePath,
                committedCursor,
                committedId,
                pendingId,
                targetCursor,
                createdAt,
                updatedAt,
                lastMessageAt,
                ingestionTime,
                subagentSourceJson,
                projectless);
    }

    public ChatSession withPendingBoundary(long pendingId, String targetCursor) {
        return new ChatSession(
                key,
                title,
                cwd,
                archived,
                sourcePath,
                sourceCursor,
                lastCommitId,
                pendingId,
                targetCursor,
                createdAt,
                updatedAt,
                lastMessageAt,
                ingestedAt,
                subagentSourceJson,
                projectless);
    }
}
