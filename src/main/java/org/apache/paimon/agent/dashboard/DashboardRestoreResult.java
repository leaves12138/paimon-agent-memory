package org.apache.paimon.agent.dashboard;

import org.apache.paimon.agent.restore.RestoreSummary;

import java.nio.file.Path;
import java.util.Objects;

/** Result of restoring one Dashboard session to a concrete local client home. */
public final class DashboardRestoreResult {

    private final Path target;
    private final RestoreSummary summary;

    public DashboardRestoreResult(Path target, RestoreSummary summary) {
        this.target = Objects.requireNonNull(target, "target").toAbsolutePath().normalize();
        this.summary = Objects.requireNonNull(summary, "summary");
    }

    public Path target() {
        return target;
    }

    public RestoreSummary summary() {
        return summary;
    }
}
