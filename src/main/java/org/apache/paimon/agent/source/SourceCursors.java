package org.apache.paimon.agent.source;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/** Stable text encoding for source-specific cursors stored in ai_chat_sessions. */
public final class SourceCursors {

    private static final String BYTE_PREFIX = "byte:";
    private static final String FILE_PREFIX = "file:v1:";

    private SourceCursors() {}

    public static String byteOffset(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("byte offset must not be negative");
        }
        return BYTE_PREFIX + value;
    }

    public static long parseByteOffset(String cursor) {
        return parseFileCursor(cursor).offset();
    }

    public static String file(long offset, String fileKey, String anchor) {
        if (offset < 0) {
            throw new IllegalArgumentException("byte offset must not be negative");
        }
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        return FILE_PREFIX
                + offset
                + ':'
                + encoder.encodeToString(safe(fileKey).getBytes(StandardCharsets.UTF_8))
                + ':'
                + encoder.encodeToString(safe(anchor).getBytes(StandardCharsets.UTF_8));
    }

    public static FileCursor parseFileCursor(String cursor) {
        if (cursor == null || cursor.trim().isEmpty()) {
            return new FileCursor(0L, null, null);
        }
        if (cursor.startsWith(BYTE_PREFIX)) {
            try {
                return new FileCursor(
                        Long.parseLong(cursor.substring(BYTE_PREFIX.length())), null, null);
            } catch (NumberFormatException e) {
                return new FileCursor(0L, null, null);
            }
        }
        if (!cursor.startsWith(FILE_PREFIX)) {
            return new FileCursor(0L, null, null);
        }
        try {
            String[] parts = cursor.substring(FILE_PREFIX.length()).split(":", -1);
            Base64.Decoder decoder = Base64.getUrlDecoder();
            return new FileCursor(
                    Long.parseLong(parts[0]),
                    emptyToNull(new String(decoder.decode(parts[1]), StandardCharsets.UTF_8)),
                    emptyToNull(new String(decoder.decode(parts[2]), StandardCharsets.UTF_8)));
        } catch (RuntimeException e) {
            return new FileCursor(0L, null, null);
        }
    }

    public static boolean samePosition(String first, String second) {
        FileCursor left = parseFileCursor(first);
        FileCursor right = parseFileCursor(second);

        // An anchor identifies line content, not a unique occurrence. Repeated JSON events and
        // blank lines are valid, so positions in the same file must match by byte offset. Anchors
        // are only a cross-file identity after a source has explicitly remapped a replaced file.
        if (Objects.equals(left.fileKey(), right.fileKey())
                || left.fileKey() == null
                || right.fileKey() == null) {
            return left.offset() == right.offset();
        }
        if (left.anchor() != null && right.anchor() != null) {
            return left.anchor().equals(right.anchor());
        }
        return false;
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }

    private static String emptyToNull(String value) {
        return value.isEmpty() ? null : value;
    }

    public static final class FileCursor {
        private final long offset;
        private final String fileKey;
        private final String anchor;

        private FileCursor(long offset, String fileKey, String anchor) {
            this.offset = offset;
            this.fileKey = fileKey;
            this.anchor = anchor;
        }

        public long offset() {
            return offset;
        }

        public String fileKey() {
            return fileKey;
        }

        public String anchor() {
            return anchor;
        }
    }
}
