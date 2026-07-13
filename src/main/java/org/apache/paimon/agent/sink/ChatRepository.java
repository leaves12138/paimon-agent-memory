package org.apache.paimon.agent.sink;

import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Persistence boundary used by the collector service and its tests. */
public interface ChatRepository extends AutoCloseable {

    void initialize() throws Exception;

    Map<SessionKey, ChatSession> loadSessions() throws Exception;

    /** Streams a snapshot of messages for restore without retaining all BLOBs in memory. */
    default void forEachMessage(
            String sourceType, Set<String> sessionIds, ChatMessageConsumer consumer)
            throws Exception {
        throw new UnsupportedOperationException("Message reads are not implemented");
    }

    void commit(long commitIdentifier, List<SessionBatch> batches) throws Exception;

    @Override
    void close() throws Exception;
}
