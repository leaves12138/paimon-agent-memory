package org.apache.paimon.agent.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ChatSessionTest {

    @Test
    void checkpointAndPendingCopiesPreserveSubagentSource() {
        String source =
                "{\"thread_spawn\":{\"parent_thread_id\":\"root\",\"depth\":1}}";
        Instant instant = Instant.parse("2026-01-01T00:00:00Z");
        ChatSession session =
                new ChatSession(
                        new SessionKey("codex", "child"),
                        "Child",
                        "/tmp/project",
                        false,
                        "/tmp/child.jsonl",
                        "byte:10",
                        3L,
                        instant,
                        instant,
                        instant,
                        instant,
                        source);

        assertThat(session.withCheckpoint("byte:20", 4L, instant).subagentSourceJson())
                .isEqualTo(source);
        assertThat(
                        session.withPendingCommit("byte:10", 3L, 4L, "byte:20", instant)
                                .subagentSourceJson())
                .isEqualTo(source);
        assertThat(session.withPendingBoundary(4L, "byte:20").subagentSourceJson())
                .isEqualTo(source);
        assertThat(session.withSubagentSourceJson(null).subagentSourceJson()).isNull();
        assertThat(session.withSubagentSourceJson(null).key()).isEqualTo(session.key());
    }
}
