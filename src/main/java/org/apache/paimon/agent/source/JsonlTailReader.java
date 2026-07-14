package org.apache.paimon.agent.source;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Reads complete UTF-8 JSONL records without converting byte cursors to character offsets. */
public final class JsonlTailReader {

    private static final int BUFFER_SIZE = 64 * 1024;

    public List<JsonlRecord> read(Path path, long requestedOffset, int maxRecords)
            throws IOException {
        return read(path, requestedOffset, maxRecords, Long.MAX_VALUE);
    }

    /** Reads at most through the byte boundary captured when a collector wake-up began. */
    public List<JsonlRecord> read(
            Path path, long requestedOffset, int maxRecords, long endOffset) throws IOException {
        if (endOffset < 0) {
            throw new IllegalArgumentException("end offset must not be negative");
        }
        long actualFileSize = Files.size(path);
        boolean bounded = endOffset != Long.MAX_VALUE;
        if (bounded && actualFileSize < endOffset) {
            throw new IOException(
                    "JSONL file was truncated below the captured scan boundary: " + path);
        }
        long fileSize = Math.min(actualFileSize, endOffset);
        if (bounded && requestedOffset > actualFileSize) {
            throw new IOException("JSONL cursor is beyond the current file size: " + path);
        }
        long offset = requestedOffset >= 0 && requestedOffset <= actualFileSize ? requestedOffset : 0L;
        List<JsonlRecord> records = new ArrayList<>();

        if (offset >= fileSize || maxRecords <= 0) {
            return records;
        }

        try (SeekableByteChannel channel =
                Files.newByteChannel(path, StandardOpenOption.READ)) {
            channel.position(offset);
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            ByteArrayOutputStream line = new ByteArrayOutputStream();
            long lineStart = offset;
            long absolutePosition = offset;

            while (records.size() < maxRecords && absolutePosition < fileSize) {
                buffer.clear();
                buffer.limit((int) Math.min(buffer.capacity(), fileSize - absolutePosition));
                if (channel.read(buffer) < 0) {
                    break;
                }
                buffer.flip();
                while (buffer.hasRemaining() && records.size() < maxRecords) {
                    byte value = buffer.get();
                    absolutePosition++;
                    if (value == '\n') {
                        records.add(
                                new JsonlRecord(
                                        lineStart,
                                        absolutePosition,
                                        decodeLine(line.toByteArray()),
                                        true));
                        line.reset();
                        lineStart = absolutePosition;
                    } else {
                        line.write(value);
                    }
                }
            }

            if (records.size() < maxRecords && line.size() > 0) {
                records.add(
                        new JsonlRecord(
                                lineStart,
                                absolutePosition,
                                decodeLine(line.toByteArray()),
                                false));
            }
        }
        return records;
    }

    private static String decodeLine(byte[] bytes) {
        int length = bytes.length;
        if (length > 0 && bytes[length - 1] == '\r') {
            length--;
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }
}
