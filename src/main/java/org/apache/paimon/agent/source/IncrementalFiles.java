package org.apache.paimon.agent.source;

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
}
