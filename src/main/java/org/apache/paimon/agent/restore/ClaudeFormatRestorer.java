package org.apache.paimon.agent.restore;

import org.apache.paimon.agent.model.ChatSession;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Restores native Claude Code project JSONL transcripts. */
final class ClaudeFormatRestorer implements ConversationFormatRestorer {

    private final Path claudeHome;
    private final Path targetProject;
    private final ObjectMapper objectMapper;
    private final Map<String, Path> transcriptPaths = new HashMap<>();

    ClaudeFormatRestorer(Path claudeHome, Path targetProject, ObjectMapper objectMapper)
            throws Exception {
        this.targetProject = canonicalProject(targetProject);
        this.claudeHome =
                RestoreFiles.canonicalTargetDirectory(claudeHome, true, "Claude target home");
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean exists(ChatSession session) throws Exception {
        return Files.exists(transcriptPath(session));
    }

    @Override
    public Path attachmentDirectory(ChatSession session) {
        try {
            return RestoreFiles.ensureContainedDirectory(
                    claudeHome,
                    Paths.get(
                            "restored-attachments",
                            localSessionId(session.key().sessionId()).toString()));
        } catch (java.io.IOException e) {
            throw new IllegalStateException(
                    "Unable to create a contained Claude attachment directory", e);
        }
    }

    @Override
    public void restore(ChatSession session, List<Path> orderedMessages, boolean overwrite)
            throws Exception {
        Path output = transcriptPath(session);
        if (Files.isSymbolicLink(output)) {
            throw new java.io.IOException(
                    "Claude transcript must not be a symbolic link: " + output);
        }
        if (!overwrite && Files.exists(output, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new FileAlreadyExistsException(
                    "Claude session appeared during restore: " + output);
        }

        String localSessionId = localSessionId(session.key().sessionId()).toString();
        String cwd = projectPath(session).toString();
        Set<String> usedUuids = new HashSet<>();
        Map<String, String> rewrittenUuids = new HashMap<>();
        String previousConversationUuid = null;
        List<String> lines = new ArrayList<>();

        int eventIndex = 0;
        for (Path message : orderedMessages) {
            JsonNode parsed = objectMapper.readTree(Files.readString(message, StandardCharsets.UTF_8));
            if (!(parsed instanceof ObjectNode)) {
                continue;
            }
            ObjectNode event = (ObjectNode) parsed;
            event.put("sessionId", localSessionId);
            event.put("cwd", cwd);

            String type = event.path("type").asText("");
            if ("user".equals(type) || "assistant".equals(type)) {
                String sourceUuid = text(event, "uuid");
                String uuid = usableUuid(sourceUuid, usedUuids);
                if (uuid == null) {
                    uuid = deterministicEventUuid(localSessionId, eventIndex, sourceUuid);
                    while (!usedUuids.add(uuid)) {
                        eventIndex++;
                        uuid = deterministicEventUuid(localSessionId, eventIndex, sourceUuid);
                    }
                } else {
                    usedUuids.add(uuid);
                }
                if (sourceUuid != null) {
                    rewrittenUuids.putIfAbsent(sourceUuid, uuid);
                }

                String sourceParent = text(event, "parentUuid");
                String parent = sourceParent == null ? null : rewrittenUuids.get(sourceParent);
                if (parent == null || !usedUuids.contains(parent) || parent.equals(uuid)) {
                    parent = previousConversationUuid;
                }
                event.put("uuid", uuid);
                if (parent == null) {
                    event.putNull("parentUuid");
                } else {
                    event.put("parentUuid", parent);
                }
                event.put("isSidechain", false);
                previousConversationUuid = uuid;
            }

            lines.add(objectMapper.writeValueAsString(event));
            eventIndex++;
        }

        if (!isBlank(session.title())) {
            ObjectNode title = objectMapper.createObjectNode();
            title.put("type", "custom-title");
            title.put("customTitle", session.title());
            title.put("sessionId", localSessionId);
            lines.add(objectMapper.writeValueAsString(title));
        }

        boolean reserved = false;
        boolean committed = false;
        try {
            if (!overwrite) {
                try {
                    Files.createFile(output);
                    RestoreFiles.setOwnerOnlyFilePermissions(output);
                    reserved = true;
                } catch (FileAlreadyExistsException race) {
                    throw new FileAlreadyExistsException(
                            "Claude session appeared during restore: " + output);
                }
            }
            RestoreFiles.setOwnerOnlyDirectoryPermissions(output.getParent());
            RestoreFiles.writeLinesAtomically(output, lines);
            committed = true;
        } finally {
            if (reserved && !committed) {
                Files.deleteIfExists(output);
            }
        }
    }

    private Path transcriptPath(ChatSession session) throws Exception {
        Path cached = transcriptPaths.get(session.key().sessionId());
        if (cached != null) {
            return cached;
        }
        Path result =
                RestoreFiles.resolveContainedFile(
                        claudeHome,
                        Paths.get(
                                "projects",
                                sanitizeProjectDirectory(projectPath(session).toString()),
                                localSessionId(session.key().sessionId()) + ".jsonl"));
        transcriptPaths.put(session.key().sessionId(), result);
        return result;
    }

    private Path projectPath(ChatSession session) {
        if (targetProject != null) {
            return targetProject;
        }
        if (!isBlank(session.cwd())) {
            try {
                Path source = Paths.get(session.cwd());
                if (Files.isDirectory(source)) {
                    return canonical(source);
                }
            } catch (RuntimeException ignored) {
                // Fall through to the current local working directory.
            }
        }
        return canonical(Paths.get(System.getProperty("user.dir")));
    }

    private static Path canonical(Path value) {
        Path absolute = value.toAbsolutePath().normalize();
        try {
            return absolute.toRealPath();
        } catch (Exception ignored) {
            return absolute;
        }
    }

    private static Path canonicalProject(Path value) throws java.io.IOException {
        if (value == null) {
            return null;
        }
        Path absolute = value.toAbsolutePath().normalize();
        if (!Files.isDirectory(absolute)) {
            throw new java.io.IOException(
                    "Claude target project is not an existing directory: " + absolute);
        }
        return absolute.toRealPath();
    }

    static String sanitizeProjectDirectory(String value) {
        String normalized = Normalizer.normalize(value, Normalizer.Form.NFC);
        StringBuilder sanitized = new StringBuilder(normalized.length());
        for (int index = 0; index < normalized.length(); index++) {
            char character = normalized.charAt(index);
            boolean asciiLetter =
                    (character >= 'a' && character <= 'z')
                            || (character >= 'A' && character <= 'Z');
            boolean asciiDigit = character >= '0' && character <= '9';
            sanitized.append(asciiLetter || asciiDigit ? character : '-');
        }
        if (sanitized.length() <= 200) {
            return sanitized.toString();
        }
        long positiveHash =
                normalized.hashCode() == Integer.MIN_VALUE
                        ? 2_147_483_648L
                        : Math.abs((long) normalized.hashCode());
        return sanitized.substring(0, 200) + "-" + Long.toString(positiveHash, 36);
    }

    private static UUID localSessionId(String sourceSessionId) {
        try {
            return UUID.fromString(sourceSessionId);
        } catch (RuntimeException ignored) {
            return UUID.nameUUIDFromBytes(
                    ("paimon-agent:claude:" + sourceSessionId)
                            .getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String usableUuid(String candidate, Set<String> used) {
        if (candidate == null) {
            return null;
        }
        try {
            String normalized = UUID.fromString(candidate).toString();
            return used.contains(normalized) ? null : normalized;
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static String deterministicEventUuid(
            String localSessionId, int eventIndex, String sourceUuid) {
        String identity =
                "paimon-agent:claude-event:"
                        + localSessionId
                        + ":"
                        + eventIndex
                        + ":"
                        + (sourceUuid == null ? "" : sourceUuid);
        return UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
