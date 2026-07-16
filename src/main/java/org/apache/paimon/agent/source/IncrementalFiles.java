package org.apache.paimon.agent.source;

import org.apache.paimon.agent.model.ChatSession;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/** Helpers for validating and recovering append-file cursors after file replacement or rewind. */
public final class IncrementalFiles {

    public static final String RESTORE_BOUNDARY_FIELD = "_paimon_agent_restore_boundary";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private IncrementalFiles() {}

    public static String fileKey(Path path) throws IOException {
        Object key = Files.readAttributes(path, BasicFileAttributes.class).fileKey();
        return key == null ? null : key.toString();
    }

    public static String lineAnchor(String jsonLine) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(jsonLine.getBytes(StandardCharsets.UTF_8));
            StringBuilder value = new StringBuilder(digest.length * 2);
            for (byte item : digest) {
                value.append(String.format("%02x", item & 0xff));
            }
            return value.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /** Verifies that a cursor still points immediately after the line represented by its anchor. */
    public static boolean anchorMatchesAtOffset(
            Path path, SourceCursors.FileCursor cursor) throws IOException {
        if (cursor.anchor() == null) {
            return true;
        }
        long offset = cursor.offset();
        long size = Files.size(path);
        if (offset <= 0 || offset > size) {
            return false;
        }

        try (SeekableByteChannel channel =
                Files.newByteChannel(path, StandardOpenOption.READ)) {
            if (readByte(channel, offset - 1) != '\n') {
                return false;
            }
            long lineEnd = offset - 1;
            if (lineEnd > 0 && readByte(channel, lineEnd - 1) == '\r') {
                lineEnd--;
            }
            long lineStart = findLineStart(channel, lineEnd);
            MessageDigest digest;
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 is unavailable", e);
            }
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            long position = lineStart;
            while (position < lineEnd) {
                int requested = (int) Math.min(buffer.capacity(), lineEnd - position);
                buffer.clear();
                buffer.limit(requested);
                channel.position(position);
                int read = readFully(channel, buffer);
                if (read != requested) {
                    return false;
                }
                digest.update(buffer.array(), 0, read);
                position += read;
            }
            return cursor.anchor().equals(hex(digest.digest()));
        }
    }

    private static long findLineStart(SeekableByteChannel channel, long lineEnd)
            throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        long searchEnd = lineEnd;
        while (searchEnd > 0) {
            long chunkStart = Math.max(0L, searchEnd - buffer.capacity());
            int length = (int) (searchEnd - chunkStart);
            buffer.clear();
            buffer.limit(length);
            channel.position(chunkStart);
            if (readFully(channel, buffer) != length) {
                return 0L;
            }
            byte[] values = buffer.array();
            for (int index = length - 1; index >= 0; index--) {
                if (values[index] == '\n') {
                    return chunkStart + index + 1L;
                }
            }
            searchEnd = chunkStart;
        }
        return 0L;
    }

    private static byte readByte(SeekableByteChannel channel, long position) throws IOException {
        ByteBuffer value = ByteBuffer.allocate(1);
        channel.position(position);
        return channel.read(value) == 1 ? value.array()[0] : 0;
    }

    private static int readFully(SeekableByteChannel channel, ByteBuffer buffer)
            throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                break;
            }
            if (read == 0) {
                continue;
            }
            total += read;
        }
        return total;
    }

    private static String hex(byte[] digest) {
        StringBuilder value = new StringBuilder(digest.length * 2);
        for (byte item : digest) {
            value.append(String.format("%02x", item & 0xff));
        }
        return value.toString();
    }

    /** Returns the byte immediately after a prior complete line, or -1 if its anchor is gone. */
    public static long findOffsetAfterAnchor(
            JsonlTailReader reader, Path path, String anchor, long preferredOffset)
            throws IOException {
        return findOffsetAfterAnchor(reader, path, anchor, preferredOffset, Long.MAX_VALUE);
    }

    /** Searches only through the EOF captured for the current collector wake-up. */
    public static long findOffsetAfterAnchor(
            JsonlTailReader reader,
            Path path,
            String anchor,
            long preferredOffset,
            long endOffset)
            throws IOException {
        if (anchor == null) {
            return -1L;
        }
        boolean bounded = endOffset != Long.MAX_VALUE;
        long searchEnd = bounded ? endOffset : Files.size(path);
        if (bounded && Files.size(path) < searchEnd) {
            throw new IOException(
                    "File was truncated below the captured anchor search boundary: " + path);
        }
        long offset = 0L;
        long bestOffset = -1L;
        long bestDistance = Long.MAX_VALUE;
        while (offset < searchEnd) {
            List<JsonlRecord> records =
                    bounded
                            ? reader.read(path, offset, 1_000, searchEnd)
                            : reader.read(path, offset, 1_000);
            if (records.isEmpty()) {
                return -1L;
            }
            long next = offset;
            for (JsonlRecord record : records) {
                if (!record.lineTerminated()) {
                    return bestOffset;
                }
                next = record.endOffset();
                if (anchor.equals(lineAnchor(record.json()))) {
                    long distance = Math.abs(next - preferredOffset);
                    if (distance < bestDistance) {
                        bestOffset = next;
                        bestDistance = distance;
                    }
                }
            }
            if (next <= offset) {
                return -1L;
            }
            offset = next;
        }
        return bestOffset;
    }

    /** Finds the last paimon-agent restore boundary in a frozen JSONL file. */
    public static RestoreBoundary findLastRestoreBoundary(
            JsonlTailReader reader,
            Path path,
            long endOffset,
            ChatSession previous)
            throws IOException {
        if (previous == null) {
            return null;
        }
        long offset = 0L;
        RestoreBoundary result = null;
        while (offset < endOffset) {
            List<JsonlRecord> records = reader.read(path, offset, 1_000, endOffset);
            if (records.isEmpty()) {
                break;
            }
            long next = offset;
            for (JsonlRecord record : records) {
                if (!record.lineTerminated()) {
                    return result;
                }
                next = record.endOffset();
                if (matchesRestoreBoundary(record.json(), previous)) {
                    result =
                            new RestoreBoundary(
                                    record.endOffset(), lineAnchor(record.json()));
                }
            }
            if (next <= offset) {
                break;
            }
            offset = next;
        }
        return result;
    }

    /** Metadata embedded into the last JSONL record emitted by a restore. */
    public static ObjectNode restoreBoundaryMarker(
            ObjectMapper objectMapper, ChatSession session) {
        ObjectNode marker = objectMapper.createObjectNode();
        marker.put("source_type", session.key().sourceType());
        marker.put("session_id", session.key().sessionId());
        marker.put("last_commit_id", session.lastCommitId());
        marker.put("source_cursor_sha256", checkpointFingerprint(session.sourceCursor()));
        return marker;
    }

    private static boolean matchesRestoreBoundary(String json, ChatSession previous) {
        if (!json.contains('"' + RESTORE_BOUNDARY_FIELD + '"')) {
            return false;
        }
        try {
            JsonNode root = OBJECT_MAPPER.readTree(json);
            if (root == null || !root.isObject()) {
                return false;
            }
            JsonNode marker = root.get(RESTORE_BOUNDARY_FIELD);
            return marker != null
                    && marker.isObject()
                    && previous.key().sourceType().equals(marker.path("source_type").asText())
                    && previous.key().sessionId().equals(marker.path("session_id").asText())
                    && marker.path("last_commit_id").isIntegralNumber()
                    && previous.lastCommitId() == marker.path("last_commit_id").asLong()
                    && checkpointFingerprint(previous.sourceCursor())
                            .equals(marker.path("source_cursor_sha256").asText());
        } catch (IOException ignored) {
            return false;
        }
    }

    private static String checkpointFingerprint(String cursor) {
        return lineAnchor(cursor == null ? "" : cursor);
    }

    /** Byte boundary and line anchor written by a completed local restore. */
    public static final class RestoreBoundary {
        private final long offset;
        private final String anchor;

        private RestoreBoundary(long offset, String anchor) {
            this.offset = offset;
            this.anchor = anchor;
        }

        public long offset() {
            return offset;
        }

        public String anchor() {
            return anchor;
        }
    }
}
