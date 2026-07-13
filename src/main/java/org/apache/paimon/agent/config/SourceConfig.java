package org.apache.paimon.agent.config;

import java.nio.file.Path;
import java.util.Objects;

/** Configuration for a local conversation source. */
public final class SourceConfig {

    private final boolean enabled;
    private final Path path;

    public SourceConfig(boolean enabled, Path path) {
        this.enabled = enabled;
        this.path = Objects.requireNonNull(path, "path");
    }

    public boolean enabled() {
        return enabled;
    }

    public Path path() {
        return path;
    }
}
