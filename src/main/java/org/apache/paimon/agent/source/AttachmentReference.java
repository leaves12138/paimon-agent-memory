package org.apache.paimon.agent.source;

import java.nio.file.Path;
import java.util.Objects;

/** A source-level reference to one attachment, before its bytes are materialized. */
public final class AttachmentReference {

    public enum Kind {
        LOCAL_PATH,
        DATA_URI,
        BASE64,
        REMOTE_URL
    }

    private final Kind kind;
    private final String value;
    private final String fileName;
    private final String mimeType;
    private final Path baseDirectory;

    public AttachmentReference(
            Kind kind, String value, String fileName, String mimeType, Path baseDirectory) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.value = Objects.requireNonNull(value, "value");
        this.fileName = fileName;
        this.mimeType = mimeType;
        this.baseDirectory = baseDirectory;
    }

    public Kind kind() {
        return kind;
    }

    public String value() {
        return value;
    }

    public String fileName() {
        return fileName;
    }

    public String mimeType() {
        return mimeType;
    }

    public Path baseDirectory() {
        return baseDirectory;
    }
}
