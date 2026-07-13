package org.apache.paimon.agent.dashboard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** One stable, one-based page of dashboard rows. */
public final class DashboardPage<T> {

    private final List<T> items;
    private final int page;
    private final int pageSize;
    private final long total;
    private final boolean truncated;

    public DashboardPage(List<T> items, int page, int pageSize, long total) {
        this(items, page, pageSize, total, false);
    }

    public DashboardPage(
            List<T> items, int page, int pageSize, long total, boolean truncated) {
        this.items = Collections.unmodifiableList(new ArrayList<>(items));
        this.page = page;
        this.pageSize = pageSize;
        this.total = total;
        this.truncated = truncated;
    }

    public List<T> getItems() {
        return items;
    }

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public long getTotal() {
        return total;
    }

    public boolean isHasMore() {
        return (long) page * pageSize < total;
    }

    public boolean isTruncated() {
        return truncated;
    }
}
