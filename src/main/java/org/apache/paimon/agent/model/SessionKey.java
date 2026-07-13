package org.apache.paimon.agent.model;

import java.util.Objects;

/** Globally unique session identity across all supported clients. */
public final class SessionKey {

    private final String sourceType;
    private final String sessionId;

    public SessionKey(String sourceType, String sessionId) {
        this.sourceType = requireText(sourceType, "sourceType");
        this.sessionId = requireText(sessionId, "sessionId");
    }

    private static String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
        return value.trim();
    }

    public String sourceType() {
        return sourceType;
    }

    public String sessionId() {
        return sessionId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SessionKey)) {
            return false;
        }
        SessionKey that = (SessionKey) other;
        return sourceType.equals(that.sourceType) && sessionId.equals(that.sessionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceType, sessionId);
    }

    @Override
    public String toString() {
        return sourceType + ":" + sessionId;
    }
}
