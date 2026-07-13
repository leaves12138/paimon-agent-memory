package org.apache.paimon.agent.dashboard;

/** Raised before an attachment larger than the configured response limit is materialized. */
public final class DashboardAttachmentTooLargeException extends IllegalArgumentException {

    private final long attachmentSize;
    private final long maxBytes;

    public DashboardAttachmentTooLargeException(long attachmentSize, long maxBytes) {
        super(
                "Attachment is "
                        + attachmentSize
                        + " bytes, exceeding the dashboard response limit of "
                        + maxBytes
                        + " bytes");
        this.attachmentSize = attachmentSize;
        this.maxBytes = maxBytes;
    }

    public long getAttachmentSize() {
        return attachmentSize;
    }

    public long getMaxBytes() {
        return maxBytes;
    }
}
