package org.apache.paimon.agent.restore;

import org.apache.paimon.agent.model.ChatSession;

import java.nio.file.Path;
import java.util.List;

/** Client-specific output writer used after Paimon messages have been ordered on disk. */
interface ConversationFormatRestorer extends AutoCloseable {

    boolean exists(ChatSession session) throws Exception;

    Path attachmentDirectory(ChatSession session);

    /** Supplies the stable restore graph before any client files are installed. */
    default void prepare(List<ChatSession> sessions) throws Exception {}

    void restore(ChatSession session, List<Path> orderedMessages, boolean overwrite)
            throws Exception;

    @Override
    default void close() throws Exception {}
}
