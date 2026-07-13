package org.apache.paimon.agent.config;

/** Thrown when one of the agent configuration files is invalid. */
public class ConfigurationException extends RuntimeException {

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
