package org.apache.paimon.agent.dashboard;

import java.util.Arrays;

/** Bounded attachment payload returned by a single-message lookup. */
public final class AttachmentData {

    private final byte[] bytes;
    private final String mimeType;
    private final String fileName;

    public AttachmentData(byte[] bytes, String mimeType, String fileName) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.mimeType = mimeType;
        this.fileName = fileName;
    }

    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    public long getSize() {
        return bytes.length;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getFileName() {
        return fileName;
    }
}
