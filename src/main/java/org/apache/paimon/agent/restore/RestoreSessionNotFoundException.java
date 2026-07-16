package org.apache.paimon.agent.restore;

/** Selected source session disappeared or does not belong to the requested client type. */
public final class RestoreSessionNotFoundException extends IllegalArgumentException {

    public RestoreSessionNotFoundException(String message) {
        super(message);
    }
}
