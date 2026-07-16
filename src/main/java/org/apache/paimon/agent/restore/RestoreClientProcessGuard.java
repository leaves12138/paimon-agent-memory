package org.apache.paimon.agent.restore;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/** Best-effort guard against racing a native Codex or Claude client process. */
public final class RestoreClientProcessGuard {

    private RestoreClientProcessGuard() {}

    public static void requireStopped(RestoreType type) {
        boolean running =
                ProcessHandle.allProcesses()
                        .filter(process -> process.pid() != ProcessHandle.current().pid())
                        .anyMatch(process -> matches(type, process.info()));
        if (running) {
            String client = type == RestoreType.CODEX ? "Codex/ChatGPT" : "Claude";
            throw new RestoreClientRunningException(
                    client + " appears to be running; fully quit it before restoring history");
        }
    }

    static boolean matches(RestoreType type, ProcessHandle.Info info) {
        return matchesCommand(
                type, info.command().orElse(""), info.arguments().orElse(new String[0]));
    }

    static boolean matchesCommand(RestoreType type, String command, String... arguments) {
        String executable = fileName(command).toLowerCase(Locale.ROOT);
        if (executable.endsWith(".exe")) {
            executable = executable.substring(0, executable.length() - ".exe".length());
        }
        if (type == RestoreType.CODEX) {
            return "chatgpt".equals(executable)
                    || "codex".equals(executable)
                    || executable.startsWith("codex-");
        }
        if ("claude".equals(executable) || executable.startsWith("claude-")) {
            return true;
        }
        if ("node".equals(executable)) {
            return arguments.length > 0 && isClaudeEntrypoint(arguments[0]);
        }
        return false;
    }

    private static boolean isClaudeEntrypoint(String value) {
        String normalized = value.replace('\\', '/').toLowerCase(Locale.ROOT);
        return normalized.endsWith("/claude")
                || normalized.contains("/claude-code/")
                || normalized.endsWith("/claude-code/cli.js");
    }

    private static String fileName(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        try {
            Path fileName = Paths.get(value.replace('\\', '/')).getFileName();
            return fileName == null ? value : fileName.toString();
        } catch (RuntimeException ignored) {
            return value;
        }
    }

}
