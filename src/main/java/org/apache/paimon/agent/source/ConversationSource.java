package org.apache.paimon.agent.source;

import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Adapter for one local AI chat client. */
public interface ConversationSource {

    String sourceType();

    default List<SessionBatch> scan(
            Map<SessionKey, ChatSession> checkpoints, int maxRecords) throws Exception {
        return scan(checkpoints, maxRecords, Collections.emptySet());
    }

    List<SessionBatch> scan(
            Map<SessionKey, ChatSession> checkpoints,
            int maxRecords,
            Set<SessionKey> onlySessions)
            throws Exception;
}
