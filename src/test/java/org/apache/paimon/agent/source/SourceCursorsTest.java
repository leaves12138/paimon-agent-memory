package org.apache.paimon.agent.source;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SourceCursorsTest {

    @Test
    void requiresExactOffsetWithinTheSameFile() {
        String earlier = SourceCursors.file(100L, "same-file", "duplicate-anchor");
        String boundary = SourceCursors.file(200L, "same-file", "duplicate-anchor");

        assertThat(SourceCursors.samePhysicalPosition(earlier, boundary)).isFalse();
        assertThat(SourceCursors.sameLogicalBoundary(earlier, boundary)).isFalse();
        assertThat(SourceCursors.samePhysicalPosition(boundary, boundary)).isTrue();
        assertThat(SourceCursors.sameLogicalBoundary(boundary, boundary)).isTrue();
    }

    @Test
    void treatsAPhysicalAdvanceAsProgressAfterFileReplacementEvenWithTheSameAnchor() {
        String oldFile = SourceCursors.file(200L, "old-file", "boundary-anchor");
        String newFile = SourceCursors.file(240L, "new-file", "boundary-anchor");

        assertThat(SourceCursors.samePhysicalPosition(oldFile, newFile)).isFalse();
        assertThat(SourceCursors.sameLogicalBoundary(oldFile, newFile)).isTrue();
    }

    @Test
    void acceptsTheSameAnchorRemappedAtTheSameOffsetAcrossAReplacedFile() {
        String oldFile = SourceCursors.file(200L, "old-file", "boundary-anchor");
        String newFile = SourceCursors.file(200L, "new-file", "boundary-anchor");

        assertThat(SourceCursors.samePhysicalPosition(oldFile, newFile)).isFalse();
        assertThat(SourceCursors.sameLogicalBoundary(oldFile, newFile)).isTrue();
    }
}
