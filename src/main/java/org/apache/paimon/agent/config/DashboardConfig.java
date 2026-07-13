package org.apache.paimon.agent.config;

/** Local, read-only dashboard settings loaded from project.properties. */
public final class DashboardConfig {

    private final boolean enabled;
    private final String host;
    private final int port;
    private final int pageSize;
    private final int maxPageSize;
    private final int maxScanRows;
    private final long maxAttachmentPreviewBytes;

    public DashboardConfig(
            boolean enabled,
            String host,
            int port,
            int pageSize,
            int maxPageSize,
            int maxScanRows,
            long maxAttachmentPreviewBytes) {
        this.enabled = enabled;
        this.host = requireLoopback(host);
        if (port <= 0 || port > 65_535) {
            throw new ConfigurationException("dashboard.port must be between 1 and 65535");
        }
        this.port = port;
        if (pageSize <= 0) {
            throw new ConfigurationException("dashboard.page-size must be greater than zero");
        }
        if (maxPageSize < pageSize) {
            throw new ConfigurationException(
                    "dashboard.max-page-size must be greater than or equal to dashboard.page-size");
        }
        if (maxScanRows < maxPageSize) {
            throw new ConfigurationException(
                    "dashboard.max-scan-rows must be greater than or equal to dashboard.max-page-size");
        }
        if (maxAttachmentPreviewBytes <= 0) {
            throw new ConfigurationException(
                    "dashboard.max-attachment-preview-size must be greater than zero");
        }
        this.pageSize = pageSize;
        this.maxPageSize = maxPageSize;
        this.maxScanRows = maxScanRows;
        this.maxAttachmentPreviewBytes = maxAttachmentPreviewBytes;
    }

    private static String requireLoopback(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new ConfigurationException("dashboard.host must not be empty");
        }
        String host = value.trim();
        if (!"127.0.0.1".equals(host) && !"::1".equals(host)) {
            throw new ConfigurationException(
                    "dashboard.host must be the loopback literal 127.0.0.1 or ::1; "
                            + "use an SSH tunnel for remote access");
        }
        return host;
    }

    public boolean enabled() {
        return enabled;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public int pageSize() {
        return pageSize;
    }

    public int maxPageSize() {
        return maxPageSize;
    }

    public int maxScanRows() {
        return maxScanRows;
    }

    public long maxAttachmentPreviewBytes() {
        return maxAttachmentPreviewBytes;
    }
}
