package org.apache.paimon.agent.restore;

/** Another paimon-agent process is currently restoring the same local client home. */
public final class RestoreTargetBusyException extends IllegalStateException {

    public RestoreTargetBusyException(String message, Throwable cause) {
        super(message, cause);
    }
}
