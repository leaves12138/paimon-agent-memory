package org.apache.paimon.agent.dashboard;

/** Filters and pagination for the sessions table. */
public final class SessionQuery {

    private final String sourceType;
    private final String search;
    private final Boolean archived;
    private final int page;
    private final int pageSize;

    public SessionQuery(
            String sourceType, String search, Boolean archived, int page, int pageSize) {
        this.sourceType = sourceType;
        this.search = search;
        this.archived = archived;
        this.page = page;
        this.pageSize = pageSize;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getSearch() {
        return search;
    }

    public Boolean getArchived() {
        return archived;
    }

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }
}
