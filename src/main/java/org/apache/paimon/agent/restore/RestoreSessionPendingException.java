package org.apache.paimon.agent.restore;

/** Selected session graph has local or durable changes that are not safe to restore yet. */
public final class RestoreSessionPendingException extends IllegalStateException {

    public RestoreSessionPendingException(String message) {
        super(message);
    }
}
