package org.apache.paimon.agent.dashboard;

/** Filters and pagination for the append-only messages table. */
public final class MessageQuery {

    private final String sourceType;
    private final String sessionId;
    private final String role;
    private final String eventType;
    private final String search;
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
        this.sourceType = sourceType;
        this.sessionId = sessionId;
        this.role = role;
        this.eventType = eventType;
        this.search = search;
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

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }
}
