package org.apache.paimon.agent.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

class IncrementalFilesTest {

    @TempDir Path tempDir;

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
}
