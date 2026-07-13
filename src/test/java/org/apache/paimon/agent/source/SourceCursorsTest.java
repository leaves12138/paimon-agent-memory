package org.apache.paimon.agent.source;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SourceCursorsTest {

    @Test
    void requiresExactOffsetWithinTheSameFile() {
        String earlier = SourceCursors.file(100L, "same-file", "duplicate-anchor");
        String boundary = SourceCursors.file(200L, "same-file", "duplicate-anchor");

        assertThat(SourceCursors.samePosition(earlier, boundary)).isFalse();
        assertThat(SourceCursors.samePosition(boundary, boundary)).isTrue();
    }

    @Test
    void acceptsAnAnchorRemappedAcrossAReplacedFile() {
        String oldFile = SourceCursors.file(200L, "old-file", "boundary-anchor");
        String newFile = SourceCursors.file(240L, "new-file", "boundary-anchor");

        assertThat(SourceCursors.samePosition(oldFile, newFile)).isTrue();
    }
}
