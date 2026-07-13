package org.apache.paimon.agent.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** A source batch that advances exactly one session cursor. */
public final class SessionBatch {

    private final ChatSession session;
    private final List<ChatMessage> messages;
    private final int sourceRecordsRead;
    private final String startingCursor;
    private final long startingCommitId;

    public SessionBatch(ChatSession session, List<ChatMessage> messages) {
        this(session, messages, messages.size(), null, session.lastCommitId());
    }

    public SessionBatch(ChatSession session, List<ChatMessage> messages, int sourceRecordsRead) {
        this(session, messages, sourceRecordsRead, null, session.lastCommitId());
    }

    public SessionBatch(
            ChatSession session,
            List<ChatMessage> messages,
            int sourceRecordsRead,
            String startingCursor,
            long startingCommitId) {
        this.session = Objects.requireNonNull(session, "session");
        this.messages =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(messages, "messages")));
        if (sourceRecordsRead < 0) {
            throw new IllegalArgumentException("sourceRecordsRead must not be negative");
        }
        this.sourceRecordsRead = sourceRecordsRead;
        this.startingCursor = startingCursor;
        this.startingCommitId = startingCommitId;
    }

    public ChatSession session() {
        return session;
    }

    public List<ChatMessage> messages() {
        return messages;
    }

    public int sourceRecordsRead() {
        return sourceRecordsRead;
    }

    public String startingCursor() {
        return startingCursor;
    }

    public long startingCommitId() {
        return startingCommitId;
    }
}
