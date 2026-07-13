package org.apache.paimon.agent.dashboard;

/** Metadata for an attachment; attachment bytes are loaded only through attachment(). */
public final class DashboardAttachment {

    private final int index;
    private final boolean present;
    private final long size;
    private final String mimeType;
    private final String fileName;
    private final String status;
    private final String sha256;

    public DashboardAttachment(
            int index,
            boolean present,
            long size,
            String mimeType,
            String fileName,
            String status,
            String sha256) {
        this.index = index;
        this.present = present;
        this.size = size;
        this.mimeType = mimeType;
        this.fileName = fileName;
        this.status = status;
        this.sha256 = sha256;
    }

    public int getIndex() {
        return index;
    }

    public boolean isPresent() {
        return present;
    }

    public long getSize() {
        return size;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getFileName() {
        return fileName;
    }

    public String getStatus() {
        return status;
    }

    public String getSha256() {
        return sha256;
    }
}
