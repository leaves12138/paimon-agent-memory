package org.apache.paimon.agent.dashboard;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Full JSON and attachment descriptors for a single message. */
public final class DashboardMessageDetail {

    private final String messageId;
    private final String sourceType;
    private final String sessionId;
    private final long sequenceNo;
    private final String role;
    private final String eventType;
    private final String contentJson;
    private final List<DashboardAttachment> attachments;
    private final Instant createdAt;
    private final Instant ingestedAt;
    private final DashboardStorageStatus storageStatus;

    public DashboardMessageDetail(
            String messageId,
            String sourceType,
            String sessionId,
            long sequenceNo,
            String role,
            String eventType,
            String contentJson,
            List<DashboardAttachment> attachments,
            Instant createdAt,
            Instant ingestedAt) {
        this(
                messageId,
                sourceType,
                sessionId,
                sequenceNo,
                role,
                eventType,
                contentJson,
                attachments,
                createdAt,
                ingestedAt,
                DashboardStorageStatus.UPLOADED);
    }

    public DashboardMessageDetail(
            String messageId,
            String sourceType,
            String sessionId,
            long sequenceNo,
            String role,
            String eventType,
            String contentJson,
            List<DashboardAttachment> attachments,
            Instant createdAt,
            Instant ingestedAt,
            DashboardStorageStatus storageStatus) {
        this.messageId = messageId;
        this.sourceType = sourceType;
        this.sessionId = sessionId;
        this.sequenceNo = sequenceNo;
        this.role = role;
        this.eventType = eventType;
        this.contentJson = contentJson;
        this.attachments = Collections.unmodifiableList(new ArrayList<>(attachments));
        this.createdAt = createdAt;
        this.ingestedAt = ingestedAt;
        this.storageStatus = storageStatus;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getSessionId() {
        return sessionId;
    }

    public long getSequenceNo() {
        return sequenceNo;
    }

    public String getRole() {
        return role;
    }

    public String getEventType() {
        return eventType;
    }

    public String getContentJson() {
        return contentJson;
    }

    public List<DashboardAttachment> getAttachments() {
        return attachments;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getIngestedAt() {
        return ingestedAt;
    }

    public DashboardStorageStatus getStorageStatus() {
        return storageStatus;
    }
}
