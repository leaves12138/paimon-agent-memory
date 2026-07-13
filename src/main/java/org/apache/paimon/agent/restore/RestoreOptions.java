package org.apache.paimon.agent.restore;

import java.nio.file.Path;
import java.util.Objects;

/** Immutable command options for restoring cloud history into a local client home. */
public final class RestoreOptions {

    private final RestoreType type;
    private final Path target;
    private final Path dataDirectory;
    private final Path targetProject;
    private final String sessionId;
    private final boolean overwrite;

    public RestoreOptions(
            RestoreType type,
            Path target,
            Path dataDirectory,
            Path targetProject,
            String sessionId,
            boolean overwrite) {
        this.type = Objects.requireNonNull(type, "type");
        this.target =
                (target == null ? type.defaultTarget() : target)
                        .toAbsolutePath()
                        .normalize();
        this.dataDirectory =
                Objects.requireNonNull(dataDirectory, "dataDirectory")
                        .toAbsolutePath()
                        .normalize();
        this.targetProject =
                targetProject == null ? null : targetProject.toAbsolutePath().normalize();
        this.sessionId =
                sessionId == null || sessionId.trim().isEmpty() ? null : sessionId.trim();
        this.overwrite = overwrite;
    }

    public RestoreType type() {
        return type;
    }

    public Path target() {
        return target;
    }

    public Path dataDirectory() {
        return dataDirectory;
    }

    public String sessionId() {
        return sessionId;
    }

    public Path targetProject() {
        return targetProject;
    }

    public boolean overwrite() {
        return overwrite;
    }
}
