package org.apache.paimon.agent.source;

import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionKey;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class IncrementalFilesTest {

    @TempDir Path tempDir;

    @Test
    void findsTheLastRestoreBoundaryWithoutMatchingEscapedMessageText() throws Exception {
        Path file = tempDir.resolve("restored.jsonl");
        ObjectMapper mapper = new ObjectMapper();
        ChatSession previous = checkpoint();
        String escaped =
                "{\"text\":\"\\\"_paimon_agent_restore_boundary\\\":true\"}";
        String first = boundary(mapper, previous, 1);
        String second = boundary(mapper, previous, 2);
        Files.writeString(file, escaped + '\n' + first + '\n' + second + '\n');

        IncrementalFiles.RestoreBoundary boundary =
                IncrementalFiles.findLastRestoreBoundary(
                        new JsonlTailReader(), file, Files.size(file), previous);

        assertThat(boundary).isNotNull();
        assertThat(boundary.offset()).isEqualTo(Files.size(file));
        assertThat(boundary.anchor()).isEqualTo(IncrementalFiles.lineAnchor(second));

        Path falseMarkers = tempDir.resolve("false-markers.jsonl");
        Files.writeString(
                falseMarkers,
                escaped
                        + '\n'
                        + "{\"payload\":{\"_paimon_agent_restore_boundary\":{"
                        + "\"source_type\":\"codex\"}}}\n");
        assertThat(
                        IncrementalFiles.findLastRestoreBoundary(
                                new JsonlTailReader(),
                                falseMarkers,
                                Files.size(falseMarkers),
                                previous))
                .isNull();
    }

    @Test
    void detectsAndRemapsAnAnchorMovedByAnInPlaceRewrite() throws Exception {
        Path transcript = tempDir.resolve("session.jsonl");
        String first = "{\"event\":\"first\"}";
        String anchor = "{\"event\":\"anchor\"}";
        Files.writeString(
                transcript,
                first + '\n' + anchor + '\n',
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW);

        String fileKey = IncrementalFiles.fileKey(transcript);
        long oldOffset = (first + '\n' + anchor + '\n').getBytes(StandardCharsets.UTF_8).length;
        SourceCursors.FileCursor cursor =
                SourceCursors.parseFileCursor(
                        SourceCursors.file(
                                oldOffset, fileKey, IncrementalFiles.lineAnchor(anchor)));
        assertThat(IncrementalFiles.anchorMatchesAtOffset(transcript, cursor)).isTrue();

        String longerPrefix = "{\"event\":\"a much longer replacement prefix\"}";
        Files.writeString(
                transcript,
                longerPrefix + '\n' + anchor + '\n' + "{\"event\":\"new\"}" + '\n',
                StandardCharsets.UTF_8,
                StandardOpenOption.TRUNCATE_EXISTING);

        assertThat(IncrementalFiles.fileKey(transcript)).isEqualTo(fileKey);
        assertThat(IncrementalFiles.anchorMatchesAtOffset(transcript, cursor)).isFalse();

        long remapped =
                IncrementalFiles.findOffsetAfterAnchor(
                        new JsonlTailReader(), transcript, cursor.anchor(), cursor.offset());
        long expected =
                (longerPrefix + '\n' + anchor + '\n')
                        .getBytes(StandardCharsets.UTF_8)
                        .length;
        assertThat(remapped).isEqualTo(expected);
    }

    @Test
    void boundedAnchorSearchIgnoresMatchingLinesAppendedAfterTheWakeUp() throws Exception {
        Path transcript = tempDir.resolve("growing.jsonl");
        String prefix = "{\"event\":\"prefix\"}";
        String anchor = "{\"event\":\"anchor\"}";
        Files.writeString(transcript, prefix + '\n');
        long boundary = Files.size(transcript);
        Files.writeString(transcript, anchor + '\n', StandardOpenOption.APPEND);

        String digest = IncrementalFiles.lineAnchor(anchor);
        assertThat(
                        IncrementalFiles.findOffsetAfterAnchor(
                                new JsonlTailReader(), transcript, digest, 0L, boundary))
                .isEqualTo(-1L);
        assertThat(
                        IncrementalFiles.findOffsetAfterAnchor(
                                new JsonlTailReader(), transcript, digest, 0L))
                .isEqualTo(Files.size(transcript));
    }

    @Test
    void boundedAnchorSearchKeepsMatchBeforePartialEofLine() throws Exception {
        Path transcript = tempDir.resolve("partial-tail.jsonl");
        String prefix = "{\"event\":\"prefix\"}";
        String anchor = "{\"event\":\"anchor\"}";
        String partialTail = "{\"event\":\"still-writing";
        Files.writeString(transcript, prefix + '\n' + anchor + '\n' + partialTail);
        long boundary = Files.size(transcript);
        long expectedOffset =
                (prefix + '\n' + anchor + '\n').getBytes(StandardCharsets.UTF_8).length;

        assertThat(
                        IncrementalFiles.findOffsetAfterAnchor(
                                new JsonlTailReader(),
                                transcript,
                                IncrementalFiles.lineAnchor(anchor),
                                expectedOffset,
                                boundary))
                .isEqualTo(expectedOffset);
    }

    private static String boundary(ObjectMapper mapper, ChatSession previous, int index)
            throws Exception {
        ObjectNode line = mapper.createObjectNode();
        line.put("index", index);
        line.set(
                IncrementalFiles.RESTORE_BOUNDARY_FIELD,
                IncrementalFiles.restoreBoundaryMarker(mapper, previous));
        return mapper.writeValueAsString(line);
    }

    private static ChatSession checkpoint() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        return new ChatSession(
                new SessionKey("codex", "session-1"),
                "title",
                "/tmp/project",
                false,
                "/tmp/source.jsonl",
                "source-cursor",
                7L,
                now,
                now,
                now,
                now);
    }
}
