package org.apache.paimon.agent.restore;

import org.apache.paimon.agent.model.ChatSession;

import java.nio.file.Path;
import java.util.List;

/** Client-specific output writer used after Paimon messages have been ordered on disk. */
interface ConversationFormatRestorer extends AutoCloseable {

    boolean exists(ChatSession session) throws Exception;

    /** Revalidates native state after the final client guard and before installation. */
    default boolean existsForInstall(ChatSession session) throws Exception {
        return exists(session);
    }

    Path attachmentDirectory(ChatSession session);

    /** Creates and revalidates the native client home after the final process guard. */
    default void prepareForInstall() throws Exception {}

    /** Creates and revalidates an attachment directory after the final process guard. */
    default Path prepareAttachmentDirectory(ChatSession session) throws Exception {
        return attachmentDirectory(session);
    }

    /** Supplies the stable restore graph before any client files are installed. */
    default void prepare(List<ChatSession> sessions) throws Exception {}

    void restore(ChatSession session, List<Path> orderedMessages, boolean overwrite)
            throws Exception;

    @Override
    default void close() throws Exception {}
}
