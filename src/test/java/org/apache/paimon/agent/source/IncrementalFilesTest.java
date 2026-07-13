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
}
