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

    /**
     * Opens one collector wake-up window.
     *
     * <p>File-backed sources should override this method and freeze their file set and EOF
     * boundaries here. The collector may call {@link ScanCycle#scan} repeatedly to drain a large
     * backlog in bounded chunks; records appended after this cycle opens belong to the next
     * wake-up.
     */
    default ScanCycle openScanCycle() throws Exception {
        return new ScanCycle() {
            @Override
            public List<SessionBatch> scan(
                    Map<SessionKey, ChatSession> checkpoints,
                    int maxRecords,
                    Set<SessionKey> onlySessions)
                    throws Exception {
                return ConversationSource.this.scan(checkpoints, maxRecords, onlySessions);
            }
        };
    }

    interface ScanCycle extends AutoCloseable {

        List<SessionBatch> scan(
                Map<SessionKey, ChatSession> checkpoints,
                int maxRecords,
                Set<SessionKey> onlySessions)
                throws Exception;

        @Override
        default void close() {}
    }
}
