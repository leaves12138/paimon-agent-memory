package org.apache.paimon.agent.dashboard;

import java.util.Optional;

/** Read-only view of the two Paimon chat tables used by the dashboard. */
public interface DashboardDataStore extends AutoCloseable {

    DashboardOverview overview() throws Exception;

    DashboardPage<DashboardSession> listSessions(SessionQuery query) throws Exception;

    DashboardPage<DashboardMessage> listMessages(MessageQuery query) throws Exception;

    Optional<DashboardMessageDetail> messageDetail(
            String sourceType, String sessionId, String messageId, long sequenceNo)
            throws Exception;

    Optional<AttachmentData> attachment(
            String sourceType,
            String sessionId,
            String messageId,
            long sequenceNo,
            int index,
            long maxBytes)
            throws Exception;

    /** Invalidates any read caches so the next query observes the latest committed snapshot. */
    default void invalidate() {}

    @Override
    default void close() throws Exception {}
}
