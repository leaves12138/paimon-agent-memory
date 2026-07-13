package org.apache.paimon.agent.model;

import java.util.Arrays;

/** Attachment bytes at one stable position in a message attachment array. */
public final class AttachmentPayload {

    private final byte[] bytes;

    private AttachmentPayload(byte[] bytes) {
        this.bytes = bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }

    public static AttachmentPayload of(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes must not be null; use missing() instead");
        }
        return new AttachmentPayload(bytes);
    }

    public static AttachmentPayload missing() {
        return new AttachmentPayload(null);
    }

    public boolean isMissing() {
        return bytes == null;
    }

    public long size() {
        return bytes == null ? -1L : bytes.length;
    }

    public byte[] bytes() {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }
}
