package org.apache.paimon.agent.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One append-only conversation event and its ordered ARRAY&lt;BLOB&gt; attachments. */
public final class ChatMessage {

    private final String messageId;
    private final SessionKey sessionKey;
    private final long sequenceNumber;
    private final String role;
    private final String eventType;
    private final String contentJson;
    private final List<AttachmentPayload> attachments;
    private final Instant createdAt;
    private final Instant ingestedAt;

    public ChatMessage(
            String messageId,
            SessionKey sessionKey,
            long sequenceNumber,
            String role,
            String eventType,
            String contentJson,
            List<AttachmentPayload> attachments,
            Instant createdAt,
            Instant ingestedAt) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.sessionKey = Objects.requireNonNull(sessionKey, "sessionKey");
        this.sequenceNumber = sequenceNumber;
        this.role = role;
        this.eventType = eventType;
        this.contentJson = Objects.requireNonNull(contentJson, "contentJson");
        this.attachments =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(attachments, "attachments")));
        this.createdAt = createdAt;
        this.ingestedAt = Objects.requireNonNull(ingestedAt, "ingestedAt");
    }

    public String messageId() {
        return messageId;
    }

    public SessionKey sessionKey() {
        return sessionKey;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public String role() {
        return role;
    }

    public String eventType() {
        return eventType;
    }

    public String contentJson() {
        return contentJson;
    }

    public List<AttachmentPayload> attachments() {
        return attachments;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant ingestedAt() {
        return ingestedAt;
    }
}
