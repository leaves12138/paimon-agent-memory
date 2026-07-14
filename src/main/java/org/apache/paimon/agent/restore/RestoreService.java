package org.apache.paimon.agent.restore;

import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.sink.ChatRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Streams cloud messages through a disk staging area and emits native client history. */
public final class RestoreService {

    private static final Logger LOG = LoggerFactory.getLogger(RestoreService.class);

    private final ChatRepository repository;
    private final ObjectMapper objectMapper;

    public RestoreService(ChatRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    public RestoreSummary restore(RestoreOptions options) throws Exception {
        List<ChatSession> candidates = selectSessions(options);
        Set<String> blockedByPending = pendingSessionsAndDescendants(options.type(), candidates);
        List<ChatSession> stableCandidates = new ArrayList<>();
        for (ChatSession session : candidates) {
            if (blockedByPending.contains(session.key().sessionId())) {
                if (options.type() == RestoreType.CODEX) {
                    LOG.warn(
                            "Skipping CODEX session {} because it or a Codex ancestor has a Paimon commit still pending; retry after collection completes",
                            session.key().sessionId());
                } else {
                    LOG.warn(
                            "Skipping {} session {} because its Paimon commit is still pending; retry after collection completes",
                            options.type(),
                            session.key().sessionId());
                }
            } else {
                stableCandidates.add(session);
            }
        }
        if (stableCandidates.isEmpty()) {
            return new RestoreSummary(0, 0, blockedByPending.size());
        }

        Files.createDirectories(options.dataDirectory());
        RestoreFiles.setOwnerOnlyDirectoryPermissions(options.dataDirectory());
        Path stagingRoot =
                options
                        .dataDirectory()
                        .resolve("restore")
                        .resolve("staging-" + UUID.randomUUID());
        Files.createDirectories(stagingRoot);
        RestoreFiles.setOwnerOnlyDirectoryPermissions(stagingRoot.getParent());
        RestoreFiles.setOwnerOnlyDirectoryPermissions(stagingRoot);

        boolean complete = false;
        try (ConversationFormatRestorer format = createFormatRestorer(options)) {
            // Keep the complete graph available to Codex even when a pending branch is skipped;
            // the visible root still needs native V2 multi-agent metadata.
            format.prepare(candidates);
            Map<String, ChatSession> selected = new HashMap<>();
            int skippedSessions = blockedByPending.size();
            for (ChatSession session : stableCandidates) {
                boolean exists = format.exists(session);
                if (!options.overwrite() && exists) {
                    skippedSessions++;
                    LOG.info("Skipping existing {} session {}", options.type(), session.key().sessionId());
                } else {
                    selected.put(session.key().sessionId(), session);
                }
            }

            ContentRestorer contentRestorer = new ContentRestorer(objectMapper);
            Map<String, Path> messageDirectories = new HashMap<>();
            Map<String, Path> attachmentStagingDirectories = new HashMap<>();
            for (ChatSession session : selected.values()) {
                Path sessionDirectory =
                        stagingRoot.resolve(safeComponent(session.key().sessionId()));
                Path messageDirectory = sessionDirectory.resolve("messages");
                Path attachmentDirectory = sessionDirectory.resolve("attachments");
                Files.createDirectories(messageDirectory);
                Files.createDirectories(attachmentDirectory);
                RestoreFiles.setOwnerOnlyDirectoryPermissions(sessionDirectory);
                RestoreFiles.setOwnerOnlyDirectoryPermissions(messageDirectory);
                RestoreFiles.setOwnerOnlyDirectoryPermissions(attachmentDirectory);
                messageDirectories.put(session.key().sessionId(), messageDirectory);
                attachmentStagingDirectories.put(
                        session.key().sessionId(), attachmentDirectory);
            }

            Set<String> sessionIds = new HashSet<>(selected.keySet());
            int[] restoredMessages = {0};
            if (!sessionIds.isEmpty()) {
                repository.forEachMessage(
                        options.type().sourceType(),
                        sessionIds,
                        message -> {
                            ChatSession session = selected.get(message.sessionKey().sessionId());
                            if (session == null || "parse_error".equals(message.eventType())) {
                                return;
                            }
                            String restoredJson =
                                    contentRestorer.restore(
                                            message,
                                            attachmentStagingDirectories.get(
                                                    session.key().sessionId()),
                                            format.attachmentDirectory(session));
                            Path output =
                                    messageDirectories
                                            .get(session.key().sessionId())
                                            .resolve(messageFileName(message));
                            try {
                                Files.writeString(
                                        output,
                                        restoredJson,
                                        StandardOpenOption.CREATE_NEW,
                                        StandardOpenOption.WRITE);
                                RestoreFiles.setOwnerOnlyFilePermissions(output);
                                restoredMessages[0]++;
                            } catch (FileAlreadyExistsException duplicate) {
                                if (!Files.isRegularFile(
                                                output,
                                                java.nio.file.LinkOption.NOFOLLOW_LINKS)
                                        || !Files.readString(output).equals(restoredJson)) {
                                    throw new IOException(
                                            "Conflicting duplicate restored message file: "
                                                    + output,
                                            duplicate);
                                }
                                LOG.debug(
                                        "Ignoring duplicate restored message {}",
                                        message.messageId());
                            }
                        });
            }
            verifyStableSnapshot(selected);

            List<ChatSession> orderedSessions = new ArrayList<>(selected.values());
            orderedSessions.sort(
                    Comparator.<ChatSession>comparingInt(
                                    session -> restoreDepth(session, selected))
                            .thenComparing(
                                    ChatSession::createdAt,
                                    Comparator.nullsLast(Comparator.naturalOrder()))
                            .thenComparing(session -> session.key().sessionId()));
            int restoredSessions = 0;
            for (ChatSession session : orderedSessions) {
                List<Path> orderedMessages = listMessages(messageDirectories.get(session.key().sessionId()));
                List<Path> installedAttachments =
                        RestoreFiles.installStagedAttachments(
                                attachmentStagingDirectories.get(session.key().sessionId()),
                                format.attachmentDirectory(session));
                try {
                    format.restore(session, orderedMessages, options.overwrite());
                } catch (Exception failure) {
                    RestoreFiles.deleteInstalledAttachments(installedAttachments, failure);
                    throw failure;
                }
                restoredSessions++;
            }

            complete = true;
            return new RestoreSummary(restoredSessions, restoredMessages[0], skippedSessions);
        } finally {
            if (complete) {
                deleteRecursively(stagingRoot);
            } else {
                LOG.warn("Restore staging was retained after failure at {}", stagingRoot);
            }
        }
    }

    private List<ChatSession> selectSessions(RestoreOptions options) throws Exception {
        Map<String, ChatSession> available = new LinkedHashMap<>();
        for (ChatSession session : repository.loadSessions().values()) {
            if (options.type().sourceType().equals(session.key().sourceType())) {
                available.put(session.key().sessionId(), session);
            }
        }
        if (options.sessionId() == null) {
            if (options.type() == RestoreType.CODEX) {
                validateCodexGraph(available);
            }
            return new ArrayList<>(available.values());
        }

        ChatSession requested = available.get(options.sessionId());
        if (requested == null) {
            throw new IllegalArgumentException(
                    "No "
                            + options.type().sourceType()
                            + " session found with id "
                            + options.sessionId());
        }

        if (options.type() != RestoreType.CODEX) {
            return java.util.Collections.singletonList(requested);
        }

        Map<String, String> parents = validateCodexGraph(available);
        String rootId = rootSessionId(requested.key().sessionId(), available, parents);
        Map<String, ChatSession> selected = new LinkedHashMap<>();
        selected.put(rootId, available.get(rootId));

        // A Codex task can own hidden subagent threads. Restoring only the visible root must also
        // install its native descendants so references and future resume behavior remain intact.
        // Supplying a hidden child ID resolves to its complete visible root task as well.
        ArrayDeque<String> pendingParents = new ArrayDeque<>();
        pendingParents.add(rootId);
        while (!pendingParents.isEmpty()) {
            String parent = pendingParents.removeFirst();
            for (ChatSession candidate : available.values()) {
                String id = candidate.key().sessionId();
                if (selected.containsKey(id)
                        || !parent.equals(parents.get(id))) {
                    continue;
                }
                selected.put(id, candidate);
                pendingParents.addLast(id);
            }
        }
        return new ArrayList<>(selected.values());
    }

    private Set<String> pendingSessionsAndDescendants(
            RestoreType type, List<ChatSession> candidates) {
        Set<String> blocked = new HashSet<>();
        for (ChatSession session : candidates) {
            if (session.hasPendingCommit()) {
                blocked.add(session.key().sessionId());
            }
        }
        if (type != RestoreType.CODEX || blocked.isEmpty()) {
            return blocked;
        }

        boolean changed;
        do {
            changed = false;
            for (ChatSession session : candidates) {
                String parent = parentSessionId(session);
                if (parent != null
                        && blocked.contains(parent)
                        && blocked.add(session.key().sessionId())) {
                    changed = true;
                }
            }
        } while (changed);
        return blocked;
    }

    private Map<String, String> validateCodexGraph(Map<String, ChatSession> sessions) {
        Map<String, String> parents = new HashMap<>();
        for (ChatSession session : sessions.values()) {
            String id = session.key().sessionId();
            String parent = parentSessionId(session);
            if (session.subagentSourceJson() != null && parent == null) {
                throw new IllegalStateException(
                        "Codex subagent session " + id + " is missing parent_thread_id");
            }
            if (parent != null && !sessions.containsKey(parent)) {
                throw new IllegalStateException(
                        "Codex subagent session "
                                + id
                                + " references missing parent session "
                                + parent);
            }
            parents.put(id, parent);
        }
        for (String id : sessions.keySet()) {
            rootSessionId(id, sessions, parents);
        }
        return parents;
    }

    private static String rootSessionId(
            String sessionId,
            Map<String, ChatSession> sessions,
            Map<String, String> parents) {
        Set<String> visited = new HashSet<>();
        String current = sessionId;
        while (visited.add(current)) {
            String parent = parents.get(current);
            if (parent == null) {
                return current;
            }
            if (!sessions.containsKey(parent)) {
                throw new IllegalStateException(
                        "Codex subagent session "
                                + current
                                + " references missing parent session "
                                + parent);
            }
            current = parent;
        }
        throw new IllegalStateException(
                "Cycle in restored Codex subagent metadata at session " + sessionId);
    }

    private int restoreDepth(ChatSession session, Map<String, ChatSession> selected) {
        int depth = 0;
        Set<String> visited = new HashSet<>();
        String current = session.key().sessionId();
        while (visited.add(current)) {
            ChatSession value = selected.get(current);
            if (value == null) {
                break;
            }
            String parent = parentSessionId(value);
            if (parent == null || !selected.containsKey(parent)) {
                return depth;
            }
            depth++;
            current = parent;
        }
        throw new IllegalStateException(
                "Cycle in restored Codex subagent metadata at session "
                        + session.key().sessionId());
    }

    private String parentSessionId(ChatSession session) {
        String value = session.subagentSourceJson();
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            JsonNode source = objectMapper.readTree(value);
            if (source == null || !source.isObject()) {
                throw new IOException("subagent source is not a JSON object");
            }
            return findText(source, "parent_thread_id");
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Invalid Codex subagent source metadata for session "
                            + session.key().sessionId(),
                    failure);
        }
    }

    private static String findText(JsonNode node, String field) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isObject()) {
            JsonNode direct = node.get(field);
            if (direct != null && !direct.isNull() && direct.isValueNode()) {
                String value = direct.asText();
                if (!value.trim().isEmpty()) {
                    return value;
                }
            }
            java.util.Iterator<JsonNode> children = node.elements();
            while (children.hasNext()) {
                String value = findText(children.next(), field);
                if (value != null) {
                    return value;
                }
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) {
                String value = findText(child, field);
                if (value != null) {
                    return value;
                }
            }
        }
        return null;
    }

    private void verifyStableSnapshot(Map<String, ChatSession> selected) throws Exception {
        if (selected.isEmpty()) {
            return;
        }
        Map<SessionKey, ChatSession> latest = repository.loadSessions();
        for (ChatSession original : selected.values()) {
            ChatSession current = latest.get(original.key());
            if (current == null
                    || current.hasPendingCommit()
                    || current.lastCommitId() != original.lastCommitId()
                    || !Objects.equals(current.sourceCursor(), original.sourceCursor())) {
                throw new IllegalStateException(
                        "Session "
                                + original.key().sessionId()
                                + " changed while restore was reading Paimon; no client history was installed. Retry the restore.");
            }
        }
    }

    private ConversationFormatRestorer createFormatRestorer(RestoreOptions options)
            throws Exception {
        switch (options.type()) {
            case CODEX:
                return new CodexFormatRestorer(
                        options.target(), options.targetProject(), objectMapper);
            case CLAUDE:
                return new ClaudeFormatRestorer(
                        options.target(), options.targetProject(), objectMapper);
            default:
                throw new IllegalArgumentException("Unsupported restore type " + options.type());
        }
    }

    private static List<Path> listMessages(Path directory) throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(Collectors.toList());
        }
    }

    private static String messageFileName(ChatMessage message) {
        String safeId = message.messageId().replaceAll("[^A-Za-z0-9._-]", "_");
        if (safeId.length() > 100) {
            safeId = safeId.substring(0, 100);
        }
        return String.format(
                Locale.ROOT,
                "%020d-%s-%s.json",
                Math.max(0L, message.sequenceNumber()),
                safeId,
                sha256(message.messageId()));
    }

    private static String safeComponent(String value) {
        String sanitized = value.replaceAll("[^A-Za-z0-9._-]", "_");
        if (sanitized.length() > 100) {
            sanitized = sanitized.substring(0, 100);
        }
        return sanitized + "-" + sha256(value);
    }

    private static String sha256(String value) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte item : digest) {
                result.append(String.format(Locale.ROOT, "%02x", item & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            List<Path> ordered =
                    paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
            for (Path path : ordered) {
                Files.deleteIfExists(path);
            }
        }
    }
}
