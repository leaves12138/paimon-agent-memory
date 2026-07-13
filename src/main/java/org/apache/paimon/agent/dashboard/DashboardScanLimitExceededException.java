package org.apache.paimon.agent.dashboard;

/** Raised instead of silently returning a partial result after a bounded dashboard scan. */
public final class DashboardScanLimitExceededException extends IllegalStateException {

    public DashboardScanLimitExceededException(String tableName, int maxScanRows) {
        super(
                "Dashboard query exceeded dashboard.max-scan-rows="
                        + maxScanRows
                        + " while reading "
                        + tableName
                        + "; narrow the filters or raise the configured limit");
    }
}
