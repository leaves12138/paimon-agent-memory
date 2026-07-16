package org.apache.paimon.agent.restore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreClientProcessGuardTest {

    @Test
    void recognizesCodexAndChatGptExecutables() {
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CODEX, "/Applications/ChatGPT.app/Contents/MacOS/ChatGPT"))
                .isTrue();
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CODEX, "/Users/test/bin/codex"))
                .isTrue();
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CODEX, "/Users/test/bin/codex-arm64"))
                .isTrue();
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CODEX, "C:\\Program Files\\OpenAI\\codex.exe"))
                .isTrue();
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CODEX, "/usr/bin/java", "codex"))
                .isFalse();
    }

    @Test
    void recognizesNativeAndNodeBasedClaudeExecutables() {
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CLAUDE, "/Users/test/bin/claude"))
                .isTrue();
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CLAUDE,
                                "/opt/homebrew/lib/node_modules/@anthropic-ai/claude-code/bin/claude.exe"))
                .isTrue();
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CLAUDE,
                                "/opt/homebrew/bin/node",
                                "/opt/claude-code/cli.js"))
                .isTrue();
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CLAUDE,
                                "/opt/homebrew/bin/node",
                                "/Users/test/project/claude-example.js"))
                .isFalse();
        assertThat(
                        RestoreClientProcessGuard.matchesCommand(
                                RestoreType.CLAUDE, "/Users/test/bin/codex"))
                .isFalse();
    }
}
