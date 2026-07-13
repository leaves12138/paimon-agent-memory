package org.apache.paimon.agent.config;

import java.time.Duration;
import java.util.Objects;

/** Strongly typed settings loaded exclusively from project.properties. */
public final class ProjectConfig {

    private final String database;
    private final String sessionsTable;
    private final String messagesTable;
    private final Duration scanInterval;
    private final Duration commitInterval;
    private final boolean runOnce;
    private final String collectorId;
    private final SourceConfig codex;
    private final SourceConfig claude;
    private final boolean attachmentsEnabled;
    private final boolean downloadRemoteAttachments;
    private final long maxAttachmentBytes;
    private final int maxScanRecordsPerSource;
    private final int maxBufferRecords;
    private final int maxRetryAttempts;
    private final Duration initialRetryDelay;
    private final DashboardConfig dashboard;

    public ProjectConfig(
            String database,
            String sessionsTable,
            String messagesTable,
            Duration scanInterval,
            Duration commitInterval,
            boolean runOnce,
            String collectorId,
            SourceConfig codex,
            SourceConfig claude,
            boolean attachmentsEnabled,
            boolean downloadRemoteAttachments,
            long maxAttachmentBytes,
            int maxScanRecordsPerSource,
            int maxBufferRecords,
            int maxRetryAttempts,
            Duration initialRetryDelay) {
        this(
                database,
                sessionsTable,
                messagesTable,
                scanInterval,
                commitInterval,
                runOnce,
                collectorId,
                codex,
                claude,
                attachmentsEnabled,
                downloadRemoteAttachments,
                maxAttachmentBytes,
                maxScanRecordsPerSource,
                maxBufferRecords,
                maxRetryAttempts,
                initialRetryDelay,
                new DashboardConfig(true, "127.0.0.1", 8787, 25, 100, 50_000, 10L * 1024 * 1024));
    }

    public ProjectConfig(
            String database,
            String sessionsTable,
            String messagesTable,
            Duration scanInterval,
            Duration commitInterval,
            boolean runOnce,
            String collectorId,
            SourceConfig codex,
            SourceConfig claude,
            boolean attachmentsEnabled,
            boolean downloadRemoteAttachments,
            long maxAttachmentBytes,
            int maxScanRecordsPerSource,
            int maxBufferRecords,
            int maxRetryAttempts,
            Duration initialRetryDelay,
            DashboardConfig dashboard) {
        this.database = requireText(database, "database");
        this.sessionsTable = requireText(sessionsTable, "sessions.table");
        this.messagesTable = requireText(messagesTable, "messages.table");
        this.scanInterval = requirePositive(scanInterval, "collector.scan.interval");
        this.commitInterval = requirePositive(commitInterval, "collector.commit.interval");
        this.runOnce = runOnce;
        this.collectorId = requireCollectorId(collectorId);
        this.codex = Objects.requireNonNull(codex, "codex");
        this.claude = Objects.requireNonNull(claude, "claude");
        this.attachmentsEnabled = attachmentsEnabled;
        this.downloadRemoteAttachments = downloadRemoteAttachments;
        if (maxAttachmentBytes <= 0) {
            throw new ConfigurationException("attachments.max-size must be greater than zero");
        }
        this.maxAttachmentBytes = maxAttachmentBytes;
        if (maxScanRecordsPerSource <= 0) {
            throw new ConfigurationException(
                    "collector.scan.max-records-per-source must be greater than zero");
        }
        this.maxScanRecordsPerSource = maxScanRecordsPerSource;
        if (maxBufferRecords <= 0) {
            throw new ConfigurationException("collector.buffer.max-records must be greater than zero");
        }
        this.maxBufferRecords = maxBufferRecords;
        if (maxRetryAttempts < 0) {
            throw new ConfigurationException("collector.retry.max-attempts must not be negative");
        }
        this.maxRetryAttempts = maxRetryAttempts;
        this.initialRetryDelay = requirePositive(initialRetryDelay, "collector.retry.initial-delay");
        this.dashboard = Objects.requireNonNull(dashboard, "dashboard");
    }

    private static String requireText(String value, String key) {
        if (value == null || value.trim().isEmpty()) {
            throw new ConfigurationException(key + " must not be empty");
        }
        return value.trim();
    }

    private static Duration requirePositive(Duration value, String key) {
        Objects.requireNonNull(value, key);
        if (value.isZero() || value.isNegative()) {
            throw new ConfigurationException(key + " must be greater than zero");
        }
        return value;
    }

    private static String requireCollectorId(String value) {
        String collectorId = requireText(value, "collector.id");
        if ("replace-with-a-unique-installation-id".equals(collectorId)) {
            throw new ConfigurationException(
                    "collector.id still contains the distribution placeholder; "
                            + "set a stable unique identifier before starting the collector");
        }
        return collectorId;
    }

    public String database() {
        return database;
    }

    public String sessionsTable() {
        return sessionsTable;
    }

    public String messagesTable() {
        return messagesTable;
    }

    public Duration scanInterval() {
        return scanInterval;
    }

    public Duration commitInterval() {
        return commitInterval;
    }

    public boolean runOnce() {
        return runOnce;
    }

    public String collectorId() {
        return collectorId;
    }

    public String commitUser() {
        return "paimon-agent-" + collectorId;
    }

    public SourceConfig codex() {
        return codex;
    }

    public SourceConfig claude() {
        return claude;
    }

    public boolean attachmentsEnabled() {
        return attachmentsEnabled;
    }

    public boolean downloadRemoteAttachments() {
        return downloadRemoteAttachments;
    }

    public long maxAttachmentBytes() {
        return maxAttachmentBytes;
    }

    public int maxBufferRecords() {
        return maxBufferRecords;
    }

    public int maxScanRecordsPerSource() {
        return maxScanRecordsPerSource;
    }

    public int maxRetryAttempts() {
        return maxRetryAttempts;
    }

    public Duration initialRetryDelay() {
        return initialRetryDelay;
    }

    public DashboardConfig dashboard() {
        return dashboard;
    }
}
