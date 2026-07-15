package org.apache.paimon.agent.dashboard;

import java.util.Locale;

/** Filters and pagination for the append-only messages table. */
public final class MessageQuery {

    private final String sourceType;
    private final String sessionId;
    private final String role;
    private final String eventType;
    private final String search;
    private final boolean conversationOnly;
    private final int page;
    private final int pageSize;

    public MessageQuery(
            String sourceType,
            String sessionId,
            String role,
            String eventType,
            String search,
            int page,
            int pageSize) {
        this(sourceType, sessionId, role, eventType, search, false, page, pageSize);
    }

    public MessageQuery(
            String sourceType,
            String sessionId,
            String role,
            String eventType,
            String search,
            boolean conversationOnly,
            int page,
            int pageSize) {
        this.sourceType = sourceType;
        this.sessionId = sessionId;
        this.role = role;
        this.eventType = eventType;
        this.search = search;
        this.conversationOnly = conversationOnly;
        this.page = page;
        this.pageSize = pageSize;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getRole() {
        return role;
    }

    public String getEventType() {
        return eventType;
    }

    public String getSearch() {
        return search;
    }

    public boolean isConversationOnly() {
        return conversationOnly;
    }

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }

    static boolean isConversationalMessage(String role, String eventType) {
        String normalizedRole = normalize(role);
        String normalizedEventType = normalize(eventType);
        if ("system".equals(normalizedRole)
                || "developer".equals(normalizedRole)
                || "tool".equals(normalizedRole)) {
            return false;
        }
        if ("attachment".equals(normalizedRole)) {
            return normalizedEventType.isEmpty()
                    || "attachment".equals(normalizedEventType);
        }
        if ("attachment".equals(normalizedEventType)) {
            return true;
        }
        if ("assistant".equals(normalizedRole)
                && "image_generation_end".equals(normalizedEventType)) {
            return true;
        }
        return ("user".equals(normalizedRole) || "assistant".equals(normalizedRole))
                && (normalizedEventType.isEmpty()
                        || "message".equals(normalizedEventType)
                        || "user".equals(normalizedEventType)
                        || "assistant".equals(normalizedEventType));
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }
}
