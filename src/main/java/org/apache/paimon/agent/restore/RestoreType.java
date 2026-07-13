package org.apache.paimon.agent.restore;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/** Local client format produced by a restore operation. */
public enum RestoreType {
    CODEX("codex", ".codex"),
    CLAUDE("claude", ".claude");

    private final String sourceType;
    private final String defaultHome;

    RestoreType(String sourceType, String defaultHome) {
        this.sourceType = sourceType;
        this.defaultHome = defaultHome;
    }

    public String sourceType() {
        return sourceType;
    }

    public Path defaultTarget() {
        if (this == CLAUDE) {
            String configured = System.getenv("CLAUDE_CONFIG_DIR");
            if (configured != null && !configured.trim().isEmpty()) {
                return Paths.get(configured).toAbsolutePath().normalize();
            }
        }
        return Paths.get(System.getProperty("user.home"), defaultHome)
                .toAbsolutePath()
                .normalize();
    }

    public static RestoreType parse(String value) {
        if (value == null) {
            throw new IllegalArgumentException("restore requires --type codex|claude");
        }
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "codex":
                return CODEX;
            case "claude":
                return CLAUDE;
            default:
                throw new IllegalArgumentException(
                        "Unsupported restore type " + value + "; expected codex or claude");
        }
    }
}
