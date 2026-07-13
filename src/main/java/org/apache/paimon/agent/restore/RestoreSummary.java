package org.apache.paimon.agent.restore;

/** User-facing restore counters. */
public final class RestoreSummary {

    private final int restoredSessions;
    private final int restoredMessages;
    private final int skippedSessions;

    public RestoreSummary(int restoredSessions, int restoredMessages, int skippedSessions) {
        this.restoredSessions = restoredSessions;
        this.restoredMessages = restoredMessages;
        this.skippedSessions = skippedSessions;
    }

    public int restoredSessions() {
        return restoredSessions;
    }

    public int restoredMessages() {
        return restoredMessages;
    }

    public int skippedSessions() {
        return skippedSessions;
    }
}
