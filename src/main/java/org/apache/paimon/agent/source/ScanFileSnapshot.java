package org.apache.paimon.agent.source;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Objects;

/** Immutable identity, size, and timestamps for a transcript at the start of one scan cycle. */
public final class ScanFileSnapshot {

    private final long size;
    private final String fileKey;
    private final Instant creationTime;
    private final Instant lastModifiedTime;

    private ScanFileSnapshot(
            long size, String fileKey, Instant creationTime, Instant lastModifiedTime) {
        this.size = size;
        this.fileKey = fileKey;
        this.creationTime = creationTime;
        this.lastModifiedTime = lastModifiedTime;
    }

    public static ScanFileSnapshot capture(Path path) throws IOException {
        BasicFileAttributes attributes =
                Files.readAttributes(path, BasicFileAttributes.class);
        if (!attributes.isRegularFile()) {
            return null;
        }
        Object key = attributes.fileKey();
        return new ScanFileSnapshot(
                attributes.size(),
                key == null ? null : key.toString(),
                attributes.creationTime().toInstant(),
                attributes.lastModifiedTime().toInstant());
    }

    public long size() {
        return size;
    }

    public String fileKey() {
        return fileKey;
    }

    public Instant creationTime() {
        return creationTime;
    }

    public Instant lastModifiedTime() {
        return lastModifiedTime;
    }

    /** Returns false if the path was replaced or truncated after this snapshot was captured. */
    public boolean canRead(Path path) throws IOException {
        BasicFileAttributes current = Files.readAttributes(path, BasicFileAttributes.class);
        if (!current.isRegularFile() || current.size() < size) {
            return false;
        }
        Object currentKey = current.fileKey();
        return fileKey == null
                || currentKey == null
                || Objects.equals(fileKey, currentKey.toString());
    }
}
