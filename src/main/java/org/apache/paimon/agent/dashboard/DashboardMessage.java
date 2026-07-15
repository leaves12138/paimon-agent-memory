package org.apache.paimon.agent.dashboard;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Message list item. It contains attachment metadata, never attachment/BLOB payloads. */
public class DashboardMessage {

    private final String messageId;
    private final String sourceType;
    private final String sessionId;
    private final long sequenceNo;
    private final String role;
    private final String eventType;
    private final String contentPreview;
    private final long contentLength;
    private final int attachmentCount;
    private final List<DashboardAttachment> attachments;
    private final Instant createdAt;
    private final Instant ingestedAt;
    private final DashboardStorageStatus storageStatus;

    public DashboardMessage(
            String messageId,
            String sourceType,
            String sessionId,
            long sequenceNo,
            String role,
            String eventType,
            String contentPreview,
            long contentLength,
            Instant createdAt,
            Instant ingestedAt) {
        this(
                messageId,
                sourceType,
                sessionId,
                sequenceNo,
                role,
                eventType,
                contentPreview,
                contentLength,
                0,
                Collections.emptyList(),
                createdAt,
                ingestedAt,
                DashboardStorageStatus.UPLOADED);
    }

    public DashboardMessage(
            String messageId,
            String sourceType,
            String sessionId,
            long sequenceNo,
            String role,
            String eventType,
            String contentPreview,
            long contentLength,
            int attachmentCount,
            Instant createdAt,
            Instant ingestedAt) {
        this(
                messageId,
                sourceType,
                sessionId,
                sequenceNo,
                role,
                eventType,
                contentPreview,
                contentLength,
                attachmentCount,
                Collections.emptyList(),
                createdAt,
                ingestedAt,
                DashboardStorageStatus.UPLOADED);
    }

    public DashboardMessage(
            String messageId,
            String sourceType,
            String sessionId,
            long sequenceNo,
            String role,
            String eventType,
            String contentPreview,
            long contentLength,
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
                contentPreview,
                contentLength,
                attachmentCount(attachments),
                attachments,
                createdAt,
                ingestedAt,
                DashboardStorageStatus.UPLOADED);
    }

    public DashboardMessage(
            String messageId,
            String sourceType,
            String sessionId,
            long sequenceNo,
            String role,
            String eventType,
            String contentPreview,
            long contentLength,
            Instant createdAt,
            Instant ingestedAt,
            DashboardStorageStatus storageStatus) {
        this(
                messageId,
                sourceType,
                sessionId,
                sequenceNo,
                role,
                eventType,
                contentPreview,
                contentLength,
                0,
                Collections.emptyList(),
                createdAt,
                ingestedAt,
                storageStatus);
    }

    public DashboardMessage(
            String messageId,
            String sourceType,
            String sessionId,
            long sequenceNo,
            String role,
            String eventType,
            String contentPreview,
            long contentLength,
            int attachmentCount,
            Instant createdAt,
            Instant ingestedAt,
            DashboardStorageStatus storageStatus) {
        this(
                messageId,
                sourceType,
                sessionId,
                sequenceNo,
                role,
                eventType,
                contentPreview,
                contentLength,
                attachmentCount,
                Collections.emptyList(),
                createdAt,
                ingestedAt,
                storageStatus);
    }

    public DashboardMessage(
            String messageId,
            String sourceType,
            String sessionId,
            long sequenceNo,
            String role,
            String eventType,
            String contentPreview,
            long contentLength,
            List<DashboardAttachment> attachments,
            Instant createdAt,
            Instant ingestedAt,
            DashboardStorageStatus storageStatus) {
        this(
                messageId,
                sourceType,
                sessionId,
                sequenceNo,
                role,
                eventType,
                contentPreview,
                contentLength,
                attachmentCount(attachments),
                attachments,
                createdAt,
                ingestedAt,
                storageStatus);
    }

    private DashboardMessage(
            String messageId,
            String sourceType,
            String sessionId,
            long sequenceNo,
            String role,
            String eventType,
            String contentPreview,
            long contentLength,
            int attachmentCount,
            List<DashboardAttachment> attachments,
            Instant createdAt,
            Instant ingestedAt,
            DashboardStorageStatus storageStatus) {
        if (attachmentCount < 0) {
            throw new IllegalArgumentException("attachmentCount must not be negative");
        }
        this.messageId = messageId;
        this.sourceType = sourceType;
        this.sessionId = sessionId;
        this.sequenceNo = sequenceNo;
        this.role = role;
        this.eventType = eventType;
        this.contentPreview = contentPreview;
        this.contentLength = contentLength;
        this.attachmentCount = attachmentCount;
        this.attachments = List.copyOf(Objects.requireNonNull(attachments, "attachments"));
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

    public String getContentPreview() {
        return contentPreview;
    }

    public long getContentLength() {
        return contentLength;
    }

    public int getAttachmentCount() {
        return attachmentCount;
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

    private static int attachmentCount(List<DashboardAttachment> attachments) {
        return Objects.requireNonNull(attachments, "attachments").size();
    }
}
