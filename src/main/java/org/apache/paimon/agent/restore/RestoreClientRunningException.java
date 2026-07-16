package org.apache.paimon.agent.restore;

/** Destination client appears to be running and may race with native-history updates. */
public final class RestoreClientRunningException extends IllegalStateException {

    public RestoreClientRunningException(String message) {
        super(message);
    }
}
