package org.apache.paimon.agent.restore;

import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.source.IncrementalFiles;
import org.apache.paimon.agent.source.codex.CodexConversationSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Restores Codex rollout JSONL plus its native SQLite and session-index metadata. */
final class CodexFormatRestorer implements ConversationFormatRestorer {

    private static final DateTimeFormatter DAY =
            DateTimeFormatter.ofPattern("yyyy/MM/dd", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter FILE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss", Locale.ROOT)
                    .withZone(ZoneOffset.UTC);

    private final Path codexHome;
    private final Path stateDatabase;
    private final Path sessionIndex;
    private final Path globalState;
    private final Path targetProject;
    private final ObjectMapper objectMapper;
    private final Map<String, Path> existingRollouts = new HashMap<>();
    private final String cliVersion;
    private Map<String, String> rootSessionIds = new HashMap<>();
    private Set<String> multiAgentRootIds = new HashSet<>();

    CodexFormatRestorer(Path codexHome, ObjectMapper objectMapper) throws Exception {
        this(codexHome, null, objectMapper);
    }

    CodexFormatRestorer(Path codexHome, Path targetProject, ObjectMapper objectMapper)
            throws Exception {
        this.codexHome =
                RestoreFiles.canonicalTargetPath(codexHome, true, "Codex target home");
        this.stateDatabase = this.codexHome.resolve("state_5.sqlite");
        this.sessionIndex =
                RestoreFiles.resolveContainedFile(
                        this.codexHome, Paths.get("session_index.jsonl"));
        this.globalState =
                RestoreFiles.resolveContainedFile(
                        this.codexHome, Paths.get(".codex-global-state.json"));
        this.targetProject = canonicalProject(targetProject, "Codex target project");
        this.objectMapper = objectMapper;
        if (Files.isSymbolicLink(this.stateDatabase)
                || !Files.isRegularFile(
                        this.stateDatabase, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException(
                    "Codex state database does not exist at "
                            + this.stateDatabase
                            + " or is a symbolic link; start and stop Codex once before restoring history");
        }
        rejectSymbolicStateSidecars();
        validateStateDatabase();
        this.cliVersion = loadLatestCliVersion();
    }

    @Override
    public void prepare(List<ChatSession> sessions) throws Exception {
        Map<String, ChatSession> byId = new HashMap<>();
        for (ChatSession session : sessions) {
            byId.put(session.key().sessionId(), session);
        }

        Map<String, String> roots = new HashMap<>();
        Set<String> multiAgentRoots = new HashSet<>();
        for (ChatSession session : sessions) {
            String rootId = resolveRootSessionId(session, byId);
            roots.put(session.key().sessionId(), rootId);
            if (subagentMetadata(session) != null) {
                multiAgentRoots.add(rootId);
            }
        }
        rootSessionIds = roots;
        multiAgentRootIds = multiAgentRoots;
    }

    @Override
    public boolean exists(ChatSession session) throws Exception {
        try (Connection connection = openReadOnlyConnection()) {
            return exists(connection, session);
        }
    }

    @Override
    public boolean existsForInstall(ChatSession session) throws Exception {
        try (Connection connection = openConnection()) {
            return exists(connection, session);
        }
    }

    @Override
    public Path attachmentDirectory(ChatSession session) {
        try {
            return RestoreFiles.planContainedDirectory(
                    codexHome,
                    Paths.get(
                            "attachments",
                            "restored",
                            safeSessionId(session.key().sessionId())));
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unable to create a contained Codex attachment directory", e);
        }
    }

    @Override
    public void prepareForInstall() throws Exception {
        RestoreFiles.canonicalTargetDirectory(codexHome, false, "Codex target home");
    }

    @Override
    public Path prepareAttachmentDirectory(ChatSession session) throws Exception {
        return RestoreFiles.ensureContainedDirectory(
                codexHome,
                Paths.get(
                        "attachments",
                        "restored",
                        safeSessionId(session.key().sessionId())));
    }

    @Override
    public void restore(ChatSession session, List<Path> orderedMessages, boolean overwrite)
            throws Exception {
        List<String> events = new ArrayList<>();
        List<JsonNode> parsedMessages = new ArrayList<>();
        for (Path message : orderedMessages) {
            String json = Files.readString(message, StandardCharsets.UTF_8);
            events.add(json);
            parsedMessages.add(objectMapper.readTree(json));
        }

        Instant createdAt = firstNonNull(session.createdAt(), Instant.now());
        Instant updatedAt =
                firstNonNull(
                        session.updatedAt(),
                        firstNonNull(session.lastMessageAt(), createdAt));
        String cwd = localWorkingDirectory(session.cwd());
        Path rollout = rolloutPath(session, createdAt);
        List<RestoredTurn> turns = restoredTurns(parsedMessages);
        String preview = firstUserMessage(parsedMessages, turns);
        if (isBlank(preview)) {
            preview = isBlank(session.title()) ? "Restored Codex session" : session.title();
        }

        List<String> rolloutLines = new ArrayList<>();
        rolloutLines.add(sessionMeta(session, createdAt, cwd));
        rolloutLines.addAll(
                restoreTurnBoundaries(
                        session.key().sessionId(), events, parsedMessages, turns, createdAt));
        markRestoreBoundary(rolloutLines, session);
        installRolloutAndThread(
                session,
                rollout,
                rolloutLines,
                cwd,
                preview,
                createdAt,
                updatedAt,
                overwrite);
    }

    private void markRestoreBoundary(List<String> lines, ChatSession session)
            throws IOException {
        int last = lines.size() - 1;
        JsonNode parsed = objectMapper.readTree(lines.get(last));
        if (!(parsed instanceof ObjectNode)) {
            throw new IOException("Restored Codex boundary is not a JSON object");
        }
        ((ObjectNode) parsed)
                .set(
                        IncrementalFiles.RESTORE_BOUNDARY_FIELD,
                        IncrementalFiles.restoreBoundaryMarker(objectMapper, session));
        lines.set(last, objectMapper.writeValueAsString(parsed));
    }

    private void installRolloutAndThread(
            ChatSession session,
            Path rollout,
            List<String> rolloutLines,
            String cwd,
            String preview,
            Instant createdAt,
            Instant updatedAt,
            boolean overwrite)
            throws Exception {
        if (Files.isSymbolicLink(rollout)) {
            throw new IOException("Codex rollout must not be a symbolic link: " + rollout);
        }
        RestoreFiles.setOwnerOnlyDirectoryPermissions(rollout.getParent());
        boolean existed = Files.exists(rollout);
        if (existed && !overwrite) {
            throw new FileAlreadyExistsException(
                    "Refusing to replace an existing Codex rollout without --overwrite: "
                            + rollout);
        }

        Path backup = null;
        if (existed) {
            backup = Files.createTempFile(rollout.getParent(), ".paimon-agent-backup-", ".jsonl");
            Files.copy(
                    rollout,
                    backup,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.COPY_ATTRIBUTES);
            RestoreFiles.setOwnerOnlyFilePermissions(backup);
        } else if (!overwrite) {
            // Reserve the path with CREATE_NEW so a concurrent process cannot be overwritten.
            Files.createFile(rollout);
            RestoreFiles.setOwnerOnlyFilePermissions(rollout);
        }

        boolean committed = false;
        SessionIndexBackup sessionIndexBackup = null;
        GlobalStateBackup globalStateBackup = null;
        try {
            RestoreFiles.writeLinesAtomically(rollout, rolloutLines);
            boolean visibleRoot = subagentMetadata(session) == null;
            if (visibleRoot) {
                sessionIndexBackup = backupSessionIndex();
                upsertSessionIndex(
                        session.key().sessionId(),
                        isBlank(session.title()) ? preview : session.title(),
                        updatedAt);
            }
            Boolean projectless = restoredProjectless(session);
            if (visibleRoot && projectless != null) {
                globalStateBackup = backupGlobalState();
                updateProjectlessState(session.key().sessionId(), projectless);
            }
            upsertThread(session, rollout, cwd, preview, createdAt, updatedAt, overwrite);
            committed = true;
        } catch (Exception failure) {
            if (globalStateBackup != null) {
                try {
                    rollbackGlobalState(globalStateBackup);
                } catch (Exception rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
            }
            if (sessionIndexBackup != null) {
                try {
                    rollbackSessionIndex(sessionIndexBackup);
                } catch (Exception rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
            }
            try {
                rollbackRollout(rollout, backup, existed);
            } catch (Exception rollbackFailure) {
                failure.addSuppressed(rollbackFailure);
            }
            throw failure;
        } finally {
            if (committed && globalStateBackup != null) {
                deleteBackupQuietly(globalStateBackup.backup);
            }
            if (committed && sessionIndexBackup != null) {
                deleteBackupQuietly(sessionIndexBackup.backup);
            }
            if (committed) {
                deleteBackupQuietly(backup);
            }
        }
    }

    private Boolean restoredProjectless(ChatSession session) {
        if (targetProject != null) {
            return false;
        }
        if (CodexConversationSource.isGeneratedTaskWorkspace(session.cwd())) {
            return true;
        }
        return session.projectless();
    }

    private GlobalStateBackup backupGlobalState() throws IOException {
        requireRegularOrMissingGlobalState();
        boolean existed =
                Files.exists(globalState, java.nio.file.LinkOption.NOFOLLOW_LINKS);
        if (!existed) {
            return new GlobalStateBackup(false, null);
        }
        Path backup =
                Files.createTempFile(
                        codexHome, ".paimon-agent-global-state-backup-", ".json");
        try {
            Files.copy(
                    globalState,
                    backup,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.COPY_ATTRIBUTES);
            RestoreFiles.setOwnerOnlyFilePermissions(backup);
            return new GlobalStateBackup(true, backup);
        } catch (IOException failure) {
            try {
                Files.deleteIfExists(backup);
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private void updateProjectlessState(String sessionId, boolean projectless)
            throws Exception {
        requireRegularOrMissingGlobalState();
        ObjectNode state;
        if (Files.exists(globalState, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            JsonNode parsed = objectMapper.readTree(globalState.toFile());
            if (parsed == null || !parsed.isObject()) {
                throw new IOException(
                        "Codex global state must contain a JSON object: " + globalState);
            }
            state = (ObjectNode) parsed;
        } else {
            state = objectMapper.createObjectNode();
        }

        JsonNode existing = state.get("projectless-thread-ids");
        if (existing != null && !existing.isNull() && !existing.isArray()) {
            throw new IOException(
                    "Codex global state field projectless-thread-ids must be an array: "
                            + globalState);
        }

        ArrayNode updated = objectMapper.createArrayNode();
        Set<String> seen = new HashSet<>();
        if (existing != null && existing.isArray()) {
            for (JsonNode value : existing) {
                if (!value.isTextual()) {
                    throw new IOException(
                            "Codex global state field projectless-thread-ids must contain only strings: "
                                    + globalState);
                }
                String id = value.asText();
                if (!sessionId.equals(id) && seen.add(id)) {
                    updated.add(id);
                }
            }
        }
        if (projectless) {
            updated.add(sessionId);
        }
        state.set("projectless-thread-ids", updated);
        RestoreFiles.writeLinesAtomically(
                globalState,
                Collections.singletonList(objectMapper.writeValueAsString(state)));
    }

    private void requireRegularOrMissingGlobalState() throws IOException {
        if (Files.isSymbolicLink(globalState)) {
            throw new IOException(
                    "Codex global state must not be a symbolic link: " + globalState);
        }
        if (Files.exists(globalState, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                && !Files.isRegularFile(
                        globalState, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException(
                    "Codex global state is not a regular file: " + globalState);
        }
    }

    private void rollbackGlobalState(GlobalStateBackup backup) throws IOException {
        if (!backup.existed) {
            Files.deleteIfExists(globalState);
            return;
        }
        if (backup.backup == null || !Files.exists(backup.backup)) {
            throw new IOException(
                    "Codex global state backup disappeared before rollback: " + globalState);
        }
        moveReplacing(backup.backup, globalState);
    }

    private SessionIndexBackup backupSessionIndex() throws IOException {
        if (Files.isSymbolicLink(sessionIndex)) {
            throw new IOException(
                    "Codex session index must not be a symbolic link: " + sessionIndex);
        }
        boolean existed = Files.exists(sessionIndex, java.nio.file.LinkOption.NOFOLLOW_LINKS);
        if (!existed) {
            return new SessionIndexBackup(false, null);
        }
        if (!Files.isRegularFile(sessionIndex, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Codex session index is not a regular file: " + sessionIndex);
        }
        Path backup =
                Files.createTempFile(codexHome, ".paimon-agent-session-index-backup-", ".jsonl");
        try {
            Files.copy(
                    sessionIndex,
                    backup,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.COPY_ATTRIBUTES);
            RestoreFiles.setOwnerOnlyFilePermissions(backup);
            return new SessionIndexBackup(true, backup);
        } catch (IOException failure) {
            try {
                Files.deleteIfExists(backup);
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private void upsertSessionIndex(String sessionId, String title, Instant updatedAt)
            throws Exception {
        List<String> current =
                Files.exists(sessionIndex)
                        ? Files.readAllLines(sessionIndex, StandardCharsets.UTF_8)
                        : Collections.emptyList();
        ObjectNode entry = objectMapper.createObjectNode();
        entry.put("id", sessionId);
        entry.put("thread_name", title);
        entry.put("updated_at", updatedAt.toString());
        String encoded = objectMapper.writeValueAsString(entry);

        List<String> updated = new ArrayList<>(current.size() + 1);
        for (String line : current) {
            boolean matches = false;
            try {
                JsonNode candidate = objectMapper.readTree(line);
                matches =
                        candidate != null
                                && candidate.isObject()
                                && sessionId.equals(candidate.path("id").asText());
            } catch (IOException ignored) {
                // Preserve malformed records belonging to other sessions verbatim.
            }
            if (!matches) {
                updated.add(line);
            }
        }
        // Codex resolves its append-only index from the tail. Always place the restored metadata
        // last after removing stale duplicates so native name lookup observes this entry as newest.
        updated.add(encoded);
        RestoreFiles.writeLinesAtomically(sessionIndex, updated);
    }

    private void rollbackSessionIndex(SessionIndexBackup backup) throws IOException {
        if (!backup.existed) {
            Files.deleteIfExists(sessionIndex);
            return;
        }
        if (backup.backup == null || !Files.exists(backup.backup)) {
            throw new IOException(
                    "Codex session index backup disappeared before rollback: " + sessionIndex);
        }
        moveReplacing(backup.backup, sessionIndex);
    }

    private static void deleteBackupQuietly(Path backup) {
        if (backup != null) {
            try {
                Files.deleteIfExists(backup);
            } catch (IOException ignored) {
                // A private backup is safer than deleting restored state on cleanup failure.
            }
        }
    }

    private static void moveReplacing(Path source, Path target) throws IOException {
        try {
            Files.move(
                    source,
                    target,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static final class SessionIndexBackup {
        private final boolean existed;
        private final Path backup;

        private SessionIndexBackup(boolean existed, Path backup) {
            this.existed = existed;
            this.backup = backup;
        }
    }

    private static final class GlobalStateBackup {
        private final boolean existed;
        private final Path backup;

        private GlobalStateBackup(boolean existed, Path backup) {
            this.existed = existed;
            this.backup = backup;
        }
    }

    private static void rollbackRollout(Path rollout, Path backup, boolean existed)
            throws IOException {
        if (!existed) {
            Files.deleteIfExists(rollout);
            return;
        }
        if (backup == null || !Files.exists(backup)) {
            throw new IOException("Codex rollout backup disappeared before rollback: " + rollout);
        }
        moveReplacing(backup, rollout);
    }

    private String sessionMeta(ChatSession session, Instant createdAt, String cwd)
            throws Exception {
        SubagentMetadata subagent = subagentMetadata(session);
        ObjectNode root = objectMapper.createObjectNode();
        root.put("timestamp", createdAt.toString());
        root.put("type", "session_meta");
        ObjectNode payload = root.putObject("payload");
        payload.put("id", session.key().sessionId());
        payload.put(
                "session_id",
                subagent == null
                        ? session.key().sessionId()
                        : requiredRootSessionId(session));
        payload.put("timestamp", createdAt.toString());
        payload.put("cwd", cwd);
        payload.put("originator", "Codex Desktop");
        payload.put("cli_version", cliVersion);
        payload.putObject("context_window")
                .put(
                        "window_id",
                        restoredContextWindowId(
                                session.key().sessionId(), createdAt));
        if (subagent == null) {
            payload.put("source", "vscode");
            payload.put("thread_source", "user");
        } else {
            ObjectNode source = payload.putObject("source");
            source.set("subagent", subagent.source.deepCopy());
            payload.put("thread_source", "subagent");
            putNullable(payload, "agent_path", subagent.agentPath);
            putNullable(payload, "agent_nickname", subagent.agentNickname);
            putNullable(payload, "agent_role", subagent.agentRole);
            if (!isBlank(subagent.parentThreadId)) {
                payload.put("parent_thread_id", subagent.parentThreadId);
                payload.put("forked_from_id", subagent.parentThreadId);
            }
        }
        if (subagent != null || multiAgentRootIds.contains(session.key().sessionId())) {
            payload.put("multi_agent_version", "v2");
        }
        payload.put("model_provider", "openai");
        payload.putObject("base_instructions").put("text", "");
        payload.putArray("dynamic_tools");
        payload.put("history_mode", "legacy");
        return objectMapper.writeValueAsString(root);
    }

    private List<String> restoreTurnBoundaries(
            String sessionId,
            List<String> events,
            List<JsonNode> parsedEvents,
            List<RestoredTurn> turns,
            Instant fallbackTime)
            throws Exception {
        Map<Integer, RestoredTurn> starts = new HashMap<>();
        Map<Integer, RestoredTurn> users = new HashMap<>();
        Map<Integer, RestoredTurn> agents = new HashMap<>();
        for (RestoredTurn turn : turns) {
            starts.put(turn.startIndex, turn);
            users.put(turn.userIndex, turn);
            if (turn.agentIndex >= 0) {
                agents.put(turn.agentIndex, turn);
            }
        }

        List<String> result = new ArrayList<>();
        for (int index = 0; index < events.size(); index++) {
            RestoredTurn start = starts.get(index);
            if (start != null) {
                result.add(
                        taskStarted(
                                turnId(sessionId, start.userIndex),
                                eventTime(parsedEvents.get(index), fallbackTime)));
            }

            result.add(events.get(index));

            RestoredTurn user = users.get(index);
            if (user != null) {
                result.add(userMessage(parsedEvents.get(index), fallbackTime));
                if (user.agentIndex < 0) {
                    result.add(
                            taskComplete(
                                    turnId(sessionId, user.userIndex),
                                    null,
                                    eventTime(parsedEvents.get(index), fallbackTime)));
                }
            }

            RestoredTurn agent = agents.get(index);
            if (agent != null) {
                String answer = messageText(parsedEvents.get(index), "output_text");
                result.add(agentMessage(parsedEvents.get(index), answer, fallbackTime));
                result.add(
                        taskComplete(
                                turnId(sessionId, agent.userIndex),
                                answer,
                                eventTime(parsedEvents.get(index), fallbackTime)));
            }
        }
        return result;
    }

    private String taskStarted(String turnId, String timestamp) throws Exception {
        ObjectNode root = event(timestamp, "event_msg");
        ObjectNode payload = root.putObject("payload");
        payload.put("type", "task_started");
        payload.put("turn_id", turnId);
        payload.putNull("model_context_window");
        payload.put("collaboration_mode_kind", "default");
        return objectMapper.writeValueAsString(root);
    }

    private String userMessage(JsonNode event, Instant fallbackTime) throws Exception {
        JsonNode sourcePayload = event.path("payload");
        ObjectNode root = event(eventTime(event, fallbackTime), "event_msg");
        ObjectNode payload = root.putObject("payload");
        payload.put("type", "user_message");
        payload.put("message", messageText(event, "input_text"));
        ArrayNode images = payload.putArray("images");
        ArrayNode localImages = payload.putArray("local_images");
        for (JsonNode content : sourcePayload.path("content")) {
            if (!"input_image".equals(content.path("type").asText())) {
                continue;
            }
            String value = content.path("image_url").asText();
            if (isBlank(value)) {
                continue;
            }
            if (isLocalImage(value)) {
                localImages.add(value);
            } else {
                images.add(value);
            }
        }
        payload.putArray("text_elements");
        return objectMapper.writeValueAsString(root);
    }

    private String agentMessage(JsonNode event, String answer, Instant fallbackTime)
            throws Exception {
        ObjectNode root = event(eventTime(event, fallbackTime), "event_msg");
        ObjectNode payload = root.putObject("payload");
        payload.put("type", "agent_message");
        payload.put("message", answer);
        String phase = event.path("payload").path("phase").asText();
        payload.put("phase", isBlank(phase) ? "final_answer" : phase);
        payload.putNull("memory_citation");
        return objectMapper.writeValueAsString(root);
    }

    private String taskComplete(String turnId, String answer, String timestamp) throws Exception {
        ObjectNode root = event(timestamp, "event_msg");
        ObjectNode payload = root.putObject("payload");
        payload.put("type", "task_complete");
        payload.put("turn_id", turnId);
        if (answer == null) {
            payload.putNull("last_agent_message");
        } else {
            payload.put("last_agent_message", answer);
        }
        return objectMapper.writeValueAsString(root);
    }

    private ObjectNode event(String timestamp, String type) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("timestamp", timestamp);
        root.put("type", type);
        return root;
    }

    private static List<RestoredTurn> restoredTurns(List<JsonNode> events) {
        List<RestoredTurn> result = new ArrayList<>();
        int segmentStart = 0;
        int userIndex = -1;
        int agentIndex = -1;
        for (int index = 0; index < events.size(); index++) {
            JsonNode event = events.get(index);
            if (isMessage(event, "user")) {
                if (userIndex >= 0 && agentIndex >= 0) {
                    result.add(new RestoredTurn(segmentStart, userIndex, agentIndex));
                    segmentStart = index;
                    userIndex = -1;
                    agentIndex = -1;
                }
                if (userIndex < 0) {
                    userIndex = index;
                } else if (isContextUser(events.get(userIndex))) {
                    // Codex records environment context as a user response_item immediately before
                    // the actual prompt. Keep it in the turn, but use the visible prompt as user input.
                    userIndex = index;
                } else if (isContextUser(event)) {
                    // A late context record belongs to the current prompt and must not become a turn.
                } else if (sameNonBlankTimestamp(events.get(userIndex), event)) {
                    // Forked/imported context can contain many historical user records stamped at
                    // exactly the same instant. Preserve the previous behavior of selecting the last
                    // visible prompt for that synthetic context segment.
                    userIndex = index;
                } else {
                    // A later real user record with no assistant response closes a cancelled or
                    // user-only turn instead of silently replacing it.
                    result.add(new RestoredTurn(segmentStart, userIndex, -1));
                    segmentStart = index;
                    userIndex = index;
                }
            } else if (isMessage(event, "assistant") && userIndex >= 0) {
                agentIndex = index;
                if ("final_answer".equals(event.path("payload").path("phase").asText())) {
                    result.add(new RestoredTurn(segmentStart, userIndex, agentIndex));
                    segmentStart = index + 1;
                    userIndex = -1;
                    agentIndex = -1;
                }
            }
        }
        if (userIndex >= 0) {
            result.add(new RestoredTurn(segmentStart, userIndex, agentIndex));
        }
        return result;
    }

    private static boolean isContextUser(JsonNode event) {
        if (!isMessage(event, "user")) {
            return false;
        }
        String text = messageText(event, "input_text").trim();
        return text.startsWith("<environment_context>")
                || text.startsWith("# AGENTS.md instructions\n\n<INSTRUCTIONS>");
    }

    private static boolean sameNonBlankTimestamp(JsonNode first, JsonNode second) {
        String left = first.path("timestamp").asText();
        String right = second.path("timestamp").asText();
        return !isBlank(left) && left.equals(right);
    }

    private static boolean isMessage(JsonNode event, String role) {
        JsonNode payload = event.path("payload");
        return "response_item".equals(event.path("type").asText())
                && "message".equals(payload.path("type").asText())
                && role.equals(payload.path("role").asText());
    }

    private static String messageText(JsonNode event, String contentType) {
        List<String> values = new ArrayList<>();
        JsonNode payload = event.path("payload");
        for (JsonNode content : payload.path("content")) {
            if (contentType.equals(content.path("type").asText())) {
                String value = content.path("text").asText();
                if (!isBlank(value)) {
                    values.add(value);
                }
            }
        }
        if (values.isEmpty()) {
            String value = payload.path("message").asText();
            if (!isBlank(value)) {
                values.add(value);
            }
        }
        return String.join("\n", values);
    }

    private static String eventTime(JsonNode event, Instant fallback) {
        String timestamp = event.path("timestamp").asText();
        return isBlank(timestamp) ? fallback.toString() : timestamp;
    }

    private static String turnId(String sessionId, int userIndex) {
        return UUID.nameUUIDFromBytes(
                        (sessionId + ':' + userIndex).getBytes(StandardCharsets.UTF_8))
                .toString();
    }

    private static boolean isLocalImage(String value) {
        return value.startsWith("/")
                || value.startsWith("file:")
                || value.matches("^[A-Za-z]:[\\\\/].*");
    }

    private Path rolloutPath(ChatSession session, Instant createdAt) throws IOException {
        Path existing = existingRollouts.get(session.key().sessionId());
        if (existing != null && existing.startsWith(codexHome)) {
            return RestoreFiles.resolveContainedFile(codexHome, codexHome.relativize(existing));
        }
        String name =
                "rollout-"
                        + FILE_TIME.format(createdAt)
                        + "-"
                        + safeSessionId(session.key().sessionId())
                        + ".jsonl";
        if (session.archived()) {
            return RestoreFiles.resolveContainedFile(
                    codexHome, Paths.get("archived_sessions", name));
        }
        return RestoreFiles.resolveContainedFile(
                codexHome, Paths.get("sessions", DAY.format(createdAt), name));
    }

    private void upsertThread(
            ChatSession session,
            Path rollout,
            String cwd,
            String preview,
            Instant createdAt,
            Instant updatedAt,
            boolean overwrite)
            throws Exception {
        try (Connection connection = openConnection()) {
            connection.setAutoCommit(false);
            try {
                Map<String, Column> columns = columns(connection);
                boolean exists = threadExists(connection, session.key().sessionId());
                if (exists && !overwrite) {
                    throw new FileAlreadyExistsException(
                            "Codex session appeared during restore: "
                                    + session.key().sessionId());
                }
                SubagentMetadata subagent = subagentMetadata(session);
                Map<String, Object> values =
                        threadValues(
                                session,
                                rollout,
                                cwd,
                                preview,
                                createdAt,
                                updatedAt,
                                subagent);
                if (exists) {
                    updateThread(connection, columns, values);
                } else {
                    insertThread(connection, columns, values);
                }
                updateThreadSpawnEdge(connection, session.key().sessionId(), subagent);
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        }
    }

    private Map<String, Object> threadValues(
            ChatSession session,
            Path rollout,
            String cwd,
            String preview,
            Instant createdAt,
            Instant updatedAt,
            SubagentMetadata subagent)
            throws Exception {
        long createdMillis = createdAt.toEpochMilli();
        long updatedMillis = updatedAt.toEpochMilli();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", session.key().sessionId());
        values.put("rollout_path", rollout.toAbsolutePath().normalize().toString());
        values.put("created_at", createdMillis / 1000L);
        values.put("updated_at", updatedMillis / 1000L);
        values.put("source", threadSourceJson(subagent));
        values.put("model_provider", "openai");
        values.put("cwd", cwd);
        values.put("title", isBlank(session.title()) ? preview : session.title());
        values.put("sandbox_policy", "{\"type\":\"read-only\"}");
        values.put("approval_mode", "on-request");
        values.put("tokens_used", 0L);
        values.put("has_user_event", 0L);
        values.put("archived", session.archived() ? 1L : 0L);
        values.put("archived_at", session.archived() ? updatedMillis / 1000L : null);
        values.put("cli_version", cliVersion);
        values.put("first_user_message", preview);
        values.put("memory_mode", "enabled");
        values.put("created_at_ms", createdMillis);
        values.put("updated_at_ms", updatedMillis);
        values.put("thread_source", subagent == null ? "user" : "subagent");
        values.put("agent_path", subagent == null ? null : subagent.agentPath);
        values.put("agent_nickname", subagent == null ? null : subagent.agentNickname);
        values.put("agent_role", subagent == null ? null : subagent.agentRole);
        values.put("preview", preview);
        values.put("recency_at", updatedMillis / 1000L);
        values.put("recency_at_ms", updatedMillis);
        values.put("history_mode", "legacy");
        return values;
    }

    private String threadSourceJson(SubagentMetadata subagent) throws Exception {
        if (subagent == null) {
            return "vscode";
        }
        ObjectNode source = objectMapper.createObjectNode();
        source.set("subagent", subagent.source.deepCopy());
        return objectMapper.writeValueAsString(source);
    }

    private String requiredRootSessionId(ChatSession session) throws IOException {
        String rootId = rootSessionIds.get(session.key().sessionId());
        if (isBlank(rootId)) {
            throw new IOException(
                    "Codex restore graph was not prepared for subagent session "
                            + session.key().sessionId());
        }
        return rootId;
    }

    private String resolveRootSessionId(
            ChatSession session, Map<String, ChatSession> sessions) throws IOException {
        Set<String> visited = new HashSet<>();
        ChatSession current = session;
        while (visited.add(current.key().sessionId())) {
            SubagentMetadata subagent = subagentMetadata(current);
            if (subagent == null) {
                return current.key().sessionId();
            }
            if (isBlank(subagent.parentThreadId)) {
                throw new IOException(
                        "Codex subagent session "
                                + current.key().sessionId()
                                + " is missing parent_thread_id");
            }
            ChatSession parent = sessions.get(subagent.parentThreadId);
            if (parent == null) {
                throw new IOException(
                        "Codex subagent session "
                                + current.key().sessionId()
                                + " references missing parent session "
                                + subagent.parentThreadId);
            }
            current = parent;
        }
        throw new IOException(
                "Cycle in Codex subagent restore graph at session "
                        + session.key().sessionId());
    }

    private static String restoredContextWindowId(String sessionId, Instant createdAt) {
        byte[] digest;
        try {
            digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(
                                    ("paimon-context-window:" + sessionId)
                                            .getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
        long timestampMillis = Math.max(0L, createdAt.toEpochMilli()) & 0x0000ffffffffffffL;
        long randomA = ((digest[0] & 0xffL) << 4) | ((digest[1] & 0xf0L) >>> 4);
        long mostSignificant = (timestampMillis << 16) | 0x7000L | randomA;
        long randomB = 0L;
        for (int index = 2; index < 10; index++) {
            randomB = (randomB << 8) | (digest[index] & 0xffL);
        }
        long leastSignificant =
                (randomB & 0x3fffffffffffffffL) | 0x8000000000000000L;
        return new UUID(mostSignificant, leastSignificant).toString();
    }

    private SubagentMetadata subagentMetadata(ChatSession session) throws IOException {
        String value = session.subagentSourceJson();
        if (isBlank(value)) {
            return null;
        }
        JsonNode source = objectMapper.readTree(value);
        if (source == null || !source.isObject()) {
            throw new IOException(
                    "Invalid Codex subagent source metadata for session "
                            + session.key().sessionId());
        }
        return new SubagentMetadata(
                source,
                findText(source, "parent_thread_id"),
                findText(source, "agent_path"),
                findText(source, "agent_nickname"),
                findText(source, "agent_role"));
    }

    private static String findText(JsonNode node, String field) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isObject()) {
            JsonNode direct = node.get(field);
            if (direct != null && !direct.isNull() && direct.isValueNode()) {
                String value = direct.asText();
                if (!isBlank(value)) {
                    return value;
                }
            }
            java.util.Iterator<JsonNode> children = node.elements();
            while (children.hasNext()) {
                String value = findText(children.next(), field);
                if (!isBlank(value)) {
                    return value;
                }
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) {
                String value = findText(child, field);
                if (!isBlank(value)) {
                    return value;
                }
            }
        }
        return null;
    }

    private static void putNullable(ObjectNode target, String field, String value) {
        if (value == null) {
            target.putNull(field);
        } else {
            target.put(field, value);
        }
    }

    private static void updateThreadSpawnEdge(
            Connection connection, String childThreadId, SubagentMetadata subagent)
            throws Exception {
        if (!tableExists(connection, "thread_spawn_edges")) {
            return;
        }
        try (PreparedStatement delete =
                connection.prepareStatement(
                        "DELETE FROM thread_spawn_edges WHERE child_thread_id = ?")) {
            delete.setString(1, childThreadId);
            delete.executeUpdate();
        }
        if (subagent == null || isBlank(subagent.parentThreadId)) {
            return;
        }
        try (PreparedStatement insert =
                connection.prepareStatement(
                        "INSERT INTO thread_spawn_edges "
                                + "(parent_thread_id, child_thread_id, status) VALUES (?, ?, ?)")) {
            insert.setString(1, subagent.parentThreadId);
            insert.setString(2, childThreadId);
            insert.setString(3, "open");
            insert.executeUpdate();
        }
    }

    private static void insertThread(
            Connection connection, Map<String, Column> columns, Map<String, Object> known)
            throws Exception {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Column column : columns.values()) {
            if (known.containsKey(column.name)) {
                values.put(column.name, known.get(column.name));
            } else if (column.notNull && column.defaultValue == null) {
                throw new IllegalStateException(
                        "Unsupported required Codex threads column without a default: "
                                + column.name);
            }
        }
        String names =
                values.keySet().stream()
                        .map(CodexFormatRestorer::quote)
                        .collect(java.util.stream.Collectors.joining(","));
        String placeholders =
                values.keySet().stream()
                        .map(ignored -> "?")
                        .collect(java.util.stream.Collectors.joining(","));
        String sql = "INSERT INTO threads (" + names + ") VALUES (" + placeholders + ")";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, values, columns);
            statement.executeUpdate();
        }
    }

    private static void updateThread(
            Connection connection, Map<String, Column> columns, Map<String, Object> known)
            throws Exception {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : known.entrySet()) {
            if (!"id".equals(entry.getKey()) && columns.containsKey(entry.getKey())) {
                values.put(entry.getKey(), entry.getValue());
            }
        }
        String assignments =
                values.keySet().stream()
                        .map(name -> quote(name) + " = ?")
                        .collect(java.util.stream.Collectors.joining(","));
        String sql = "UPDATE threads SET " + assignments + " WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, values, columns);
            statement.setString(values.size() + 1, String.valueOf(known.get("id")));
            statement.executeUpdate();
        }
    }

    private static void bind(
            PreparedStatement statement,
            Map<String, Object> values,
            Map<String, Column> columns)
            throws Exception {
        int position = 1;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            Object value = entry.getValue();
            if (value == null) {
                statement.setNull(position++, sqlType(columns.get(entry.getKey()).type));
            } else {
                statement.setObject(position++, value);
            }
        }
    }

    private String loadLatestCliVersion() throws Exception {
        try (Connection connection = openReadOnlyConnection()) {
            Map<String, Column> available = columns(connection);
            if (!available.containsKey("cli_version")) {
                return "paimon-agent-restore";
            }
            String ordering =
                    available.containsKey("updated_at_ms")
                            ? "COALESCE(updated_at_ms, updated_at * 1000)"
                            : "updated_at";
            try (Statement statement = connection.createStatement();
                    ResultSet rows =
                        statement.executeQuery(
                                "SELECT cli_version FROM threads WHERE cli_version <> '' "
                                        + "ORDER BY "
                                        + ordering
                                        + " DESC LIMIT 1")) {
                return rows.next() ? rows.getString(1) : "paimon-agent-restore";
            }
        }
    }

    private void validateStateDatabase() throws Exception {
        try (Connection connection = openReadOnlyConnection()) {
            if (!tableExists(connection, "_sqlx_migrations")
                    || !tableExists(connection, "threads")) {
                throw new IllegalStateException(
                        "Codex state database is not fully initialized at "
                                + stateDatabase
                                + "; start and stop Codex once before restoring history");
            }
            Set<String> required =
                    new HashSet<>(
                            java.util.Arrays.asList(
                                    "id",
                                    "rollout_path",
                                    "created_at",
                                    "updated_at",
                                    "source",
                                    "model_provider",
                                    "cwd",
                                    "title",
                                    "sandbox_policy",
                                    "approval_mode"));
            required.removeAll(columns(connection).keySet());
            if (!required.isEmpty()) {
                throw new IllegalStateException(
                        "Unsupported Codex threads schema; missing columns " + required);
            }
        }
    }

    private void rejectSymbolicStateSidecars() throws IOException {
        for (String suffix : new String[] {"-wal", "-shm", "-journal"}) {
            Path sidecar = Paths.get(stateDatabase.toString() + suffix);
            if (Files.isSymbolicLink(sidecar)) {
                throw new IOException(
                        "Codex state database sidecar must not be a symbolic link: " + sidecar);
            }
        }
    }

    private static boolean tableExists(Connection connection, String tableName) throws Exception {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")) {
            statement.setString(1, tableName);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private Connection openConnection() throws Exception {
        Connection connection =
                DriverManager.getConnection("jdbc:sqlite:" + stateDatabase.toAbsolutePath());
        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA busy_timeout=10000");
                statement.execute("PRAGMA foreign_keys=ON");
                statement.execute("PRAGMA journal_mode=WAL");
            }
            return connection;
        } catch (Exception failure) {
            try {
                connection.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private Connection openReadOnlyConnection() throws Exception {
        Path wal = Paths.get(stateDatabase.toString() + "-wal");
        Path shm = Paths.get(stateDatabase.toString() + "-shm");
        boolean sidecarsPresent = Files.exists(wal) || Files.exists(shm);
        String parameters = sidecarsPresent ? "?mode=ro" : "?mode=ro&immutable=1";
        Connection connection =
                DriverManager.getConnection(
                        "jdbc:sqlite:" + stateDatabase.toUri() + parameters);
        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA busy_timeout=10000");
                statement.execute("PRAGMA query_only=ON");
            }
            return connection;
        } catch (Exception failure) {
            try {
                connection.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private boolean exists(Connection connection, ChatSession session) throws Exception {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT rollout_path FROM threads WHERE id = ?")) {
            statement.setString(1, session.key().sessionId());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return false;
                }
                String value = rows.getString(1);
                if (value != null && !value.trim().isEmpty()) {
                    existingRollouts.put(
                            session.key().sessionId(),
                            Paths.get(value).toAbsolutePath().normalize());
                }
                return true;
            }
        }
    }

    private static Map<String, Column> columns(Connection connection) throws Exception {
        Map<String, Column> result = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("PRAGMA table_info(threads)")) {
            while (rows.next()) {
                Column column =
                        new Column(
                                rows.getString("name"),
                                rows.getString("type"),
                                rows.getInt("notnull") != 0,
                                rows.getString("dflt_value"));
                result.put(column.name, column);
            }
        }
        return result;
    }

    private static boolean threadExists(Connection connection, String sessionId) throws Exception {
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT 1 FROM threads WHERE id = ?")) {
            statement.setString(1, sessionId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static String firstUserMessage(
            List<JsonNode> events, List<RestoredTurn> turns) {
        for (RestoredTurn turn : turns) {
            String value = messageText(events.get(turn.userIndex), "input_text");
            if (!isBlank(value)) {
                return value.length() <= 2_000 ? value : value.substring(0, 2_000);
            }
        }
        return null;
    }

    private String localWorkingDirectory(String source) {
        if (targetProject != null) {
            return targetProject.toString();
        }
        if (!isBlank(source)) {
            try {
                Path path = Paths.get(source).toAbsolutePath().normalize();
                if (Files.isDirectory(path)) {
                    return path.toRealPath().toString();
                }
            } catch (IOException | RuntimeException ignored) {
                // Fall through to a local directory adjacent to the isolated target home.
            }
        }
        Path parent = codexHome.getParent();
        return (parent == null ? codexHome : parent).toString();
    }

    private static String safeSessionId(String value) {
        // Codex discovers rollout files by their native "-<uuid>.jsonl" suffix. Preserve that
        // suffix for real Codex sessions; appending a collision hash makes thread/read work via
        // the SQLite row but hides the restored session from thread/list.
        try {
            return UUID.fromString(value).toString();
        } catch (IllegalArgumentException ignored) {
            // Keep non-Codex/test identifiers path-safe and collision-resistant.
        }
        String safe = value.replaceAll("[^A-Za-z0-9._-]", "_");
        if (safe.isEmpty()) {
            safe = "session";
        }
        if (safe.length() > 100) {
            safe = safe.substring(0, 100);
        }
        return safe + '-' + stableHash(value);
    }

    private static String stableHash(String value) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(16);
            for (int index = 0; index < 8; index++) {
                result.append(String.format(Locale.ROOT, "%02x", digest[index] & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static Path canonicalProject(Path value, String description) throws IOException {
        if (value == null) {
            return null;
        }
        Path absolute = value.toAbsolutePath().normalize();
        if (!Files.isDirectory(absolute)) {
            throw new IOException(description + " is not an existing directory: " + absolute);
        }
        return absolute.toRealPath();
    }

    private static String quote(String value) {
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static int sqlType(String type) {
        String normalized = type == null ? "" : type.toUpperCase(Locale.ROOT);
        if (normalized.contains("INT")) {
            return Types.BIGINT;
        }
        if (normalized.contains("REAL") || normalized.contains("NUM")) {
            return Types.DOUBLE;
        }
        if (normalized.contains("BLOB")) {
            return Types.BLOB;
        }
        return Types.VARCHAR;
    }

    private static <T> T firstNonNull(T value, T fallback) {
        return value == null ? fallback : value;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static final class Column {
        private final String name;
        private final String type;
        private final boolean notNull;
        private final String defaultValue;

        private Column(String name, String type, boolean notNull, String defaultValue) {
            this.name = name;
            this.type = type;
            this.notNull = notNull;
            this.defaultValue = defaultValue;
        }
    }

    private static final class SubagentMetadata {
        private final JsonNode source;
        private final String parentThreadId;
        private final String agentPath;
        private final String agentNickname;
        private final String agentRole;

        private SubagentMetadata(
                JsonNode source,
                String parentThreadId,
                String agentPath,
                String agentNickname,
                String agentRole) {
            this.source = source;
            this.parentThreadId = parentThreadId;
            this.agentPath = agentPath;
            this.agentNickname = agentNickname;
            this.agentRole = agentRole;
        }
    }

    private static final class RestoredTurn {
        private final int startIndex;
        private final int userIndex;
        private final int agentIndex;

        private RestoredTurn(int startIndex, int userIndex, int agentIndex) {
            this.startIndex = startIndex;
            this.userIndex = userIndex;
            this.agentIndex = agentIndex;
        }
    }
}
