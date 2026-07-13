package org.apache.paimon.agent.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Deterministic identifiers used as a second line of defense against source replay. */
public final class MessageIds {

    private MessageIds() {}

    public static String fromSourcePosition(
            SessionKey key, String sourceIdentity, long sourcePosition, String eventType) {
        String input =
                key.sourceType()
                        + '\n'
                        + key.sessionId()
                        + '\n'
                        + sourceIdentity
                        + '\n'
                        + sourcePosition
                        + '\n'
                        + (eventType == null ? "" : eventType);
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(String.format("%02x", value & 0xff));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
