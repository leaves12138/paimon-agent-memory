package org.apache.paimon.agent.source.codex;

import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.MessageIds;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.source.AttachmentReference;
import org.apache.paimon.agent.source.AttachmentResolver;
import org.apache.paimon.agent.source.ConversationSource;
import org.apache.paimon.agent.source.JsonlRecord;
import org.apache.paimon.agent.source.JsonlTailReader;
import org.apache.paimon.agent.source.IncrementalFiles;
import org.apache.paimon.agent.source.ResolvedAttachments;
import org.apache.paimon.agent.source.SourceCursors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Incrementally reads Codex rollout JSONL files and the current thread metadata database. */
public final class CodexConversationSource implements ConversationSource {

    public static final String SOURCE_TYPE = "codex";

    private static final Logger LOG = LoggerFactory.getLogger(CodexConversationSource.class);
    private static final Pattern MENTIONED_FILE =
            Pattern.compile("(?m)^##[ \\t]+(.+):[ \\t]+(.+?)[ \\t]*$");
    private static final Pattern INLINE_IMAGE_PATH =
            Pattern.compile("<image\\b[^>]*\\bpath=\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);

    private final Path codexHome;
    private final ObjectMapper objectMapper;
    private final AttachmentResolver attachmentResolver;
    private final JsonlTailReader tailReader;
    private int nextThreadIndex;

    public CodexConversationSource(
            Path codexHome, ObjectMapper objectMapper, AttachmentResolver attachmentResolver) {
        this.codexHome = codexHome;
        this.objectMapper = objectMapper;
        this.attachmentResolver = attachmentResolver;
        this.tailReader = new JsonlTailReader();
    }

    @Override
    public String sourceType() {
        return SOURCE_TYPE;
    }

    @Override
    public List<SessionBatch> scan(
            Map<SessionKey, ChatSession> checkpoints,
            int maxRecords,
            Set<SessionKey> onlySessions)
            throws Exception {
        if (!Files.isDirectory(codexHome)) {
            LOG.warn("Codex home does not exist: {}", codexHome);
            return java.util.Collections.emptyList();
        }

        Map<String, CodexThread> threads = loadThreads();
        addUnindexedRollouts(threads);
        List<CodexThread> ordered =
                threads.values().stream()
                        .sorted(
                                Comparator.comparing(
                                        CodexThread::updatedAt,
                                        Comparator.nullsLast(Comparator.reverseOrder())))
                        .collect(Collectors.toList());

        List<SessionBatch> batches = new ArrayList<>();
        int remaining = maxRecords;
        int threadCount = ordered.size();
        int threadStart = threadCount == 0 ? 0 : Math.floorMod(nextThreadIndex, threadCount);
        if (threadCount > 0) {
            nextThreadIndex = (threadStart + 1) % threadCount;
        }
        for (int threadOffset = 0; threadOffset < threadCount; threadOffset++) {
            if (remaining <= 0) {
                break;
            }
            CodexThread thread = ordered.get((threadStart + threadOffset) % threadCount);
            SessionKey key = new SessionKey(SOURCE_TYPE, thread.sessionId);
            if (!onlySessions.isEmpty() && !onlySessions.contains(key)) {
                continue;
            }
            ChatSession previous = checkpoints.get(key);
            SessionBatch batch;
            try {
                batch = scanThread(thread, previous, remaining);
            } catch (AttachmentResolver.RetryableAttachmentException | IOException failure) {
                if (isInterruptedFailure(failure)) {
                    throw failure;
                }
                LOG.warn(
                        "Unable to scan Codex session {} from {}; this session will be retried",
                        thread.sessionId,
                        thread.rolloutPath,
                        failure);
                continue;
            }
            if (batch != null) {
                batches.add(batch);
                remaining -= batch.sourceRecordsRead();
            }
        }
        return batches;
    }

    private SessionBatch scanThread(CodexThread thread, ChatSession previous, int maxRecords)
            throws IOException {
        SourceCursors.FileCursor priorCursor =
                SourceCursors.parseFileCursor(previous == null ? null : previous.sourceCursor());
        SourceCursors.FileCursor targetCursor =
                SourceCursors.parseFileCursor(
                        previous == null ? null : previous.pendingCursor());
        long startOffset = priorCursor.offset();
        String currentFileKey =
                Files.isRegularFile(thread.rolloutPath)
                        ? IncrementalFiles.fileKey(thread.rolloutPath)
                        : null;
        if (Files.isRegularFile(thread.rolloutPath)
                && (Files.size(thread.rolloutPath) < startOffset
                        || (priorCursor.fileKey() != null
                                && !java.util.Objects.equals(
                                        priorCursor.fileKey(), currentFileKey))
                        || !IncrementalFiles.anchorMatchesAtOffset(
                                thread.rolloutPath, priorCursor))) {
            long recovered =
                    IncrementalFiles.findOffsetAfterAnchor(
                            tailReader,
                            thread.rolloutPath,
                            priorCursor.anchor(),
                            priorCursor.offset());
            if (recovered < 0) {
                LOG.warn(
                        "Codex rollout was rewritten and its checkpoint anchor disappeared; "
                                + "leaving session {} unchanged to avoid duplicate append rows",
                        thread.sessionId);
                return null;
            }
            startOffset = recovered;
        }
        long targetOffset = -1L;
        if (previous != null && previous.hasPendingCommit()) {
            targetOffset = targetCursor.offset();
            if ((targetCursor.fileKey() != null
                            && !java.util.Objects.equals(
                                    targetCursor.fileKey(), currentFileKey))
                    || (Files.isRegularFile(thread.rolloutPath)
                            && !IncrementalFiles.anchorMatchesAtOffset(
                                    thread.rolloutPath, targetCursor))) {
                targetOffset =
                        IncrementalFiles.findOffsetAfterAnchor(
                                tailReader,
                                thread.rolloutPath,
                                targetCursor.anchor(),
                                targetCursor.offset());
                if (targetOffset < 0) {
                    LOG.warn(
                            "Pending Codex boundary disappeared for session {}; recovery is paused",
                            thread.sessionId);
                    return null;
                }
            }
        }
        List<JsonlRecord> records =
                Files.isRegularFile(thread.rolloutPath)
                        ? tailReader.read(thread.rolloutPath, startOffset, maxRecords)
                        : java.util.Collections.emptyList();

        SessionKey key = new SessionKey(SOURCE_TYPE, thread.sessionId);
        List<ChatMessage> messages = new ArrayList<>();
        long processedOffset = startOffset;
        int processedRecords = 0;
        String lastAnchor = priorCursor.anchor();
        String title =
                !isBlank(thread.title)
                        ? thread.title
                        : previous == null ? null : previous.title();
        String cwd =
                !isBlank(thread.cwd) ? thread.cwd : previous == null ? null : previous.cwd();
        Instant lastMessageAt = previous == null ? null : previous.lastMessageAt();
        Instant now = Instant.now();

        for (JsonlRecord record : records) {
            if (!record.lineTerminated()) {
                break;
            }
            if (targetOffset >= 0 && record.endOffset() > targetOffset) {
                break;
            }
            processedRecords++;
            if (record.json().trim().isEmpty()) {
                processedOffset = record.endOffset();
                lastAnchor = IncrementalFiles.lineAnchor(record.json());
                continue;
            }

            JsonNode event;
            try {
                event = objectMapper.readTree(record.json());
            } catch (IOException e) {
                LOG.warn(
                        "Skipping malformed Codex JSONL record at {}:{}",
                        thread.rolloutPath,
                        record.startOffset());
                processedOffset = record.endOffset();
                lastAnchor = IncrementalFiles.lineAnchor(record.json());
                messages.add(
                        parseErrorMessage(
                                key,
                                thread.sessionId,
                                record,
                                now));
                continue;
            }
            processedOffset = record.endOffset();
            lastAnchor = IncrementalFiles.lineAnchor(record.json());

            if ("session_meta".equals(text(event, "type"))) {
                JsonNode payload = event.path("payload");
                if (isBlank(cwd)) {
                    cwd = text(payload, "cwd");
                }
                continue;
            }

            EventClassification classification = classify(event);
            if (classification == null) {
                continue;
            }

            ExtractedEvent extracted = extractAttachments(event, thread.rolloutPath);
            ResolvedAttachments resolved =
                    attachmentResolver.resolve(extracted.sanitizedEvent, extracted.references);
            Instant eventTime = parseInstant(text(event, "timestamp"));
            if (eventTime != null) {
                lastMessageAt = eventTime;
            }
            if (isBlank(title) && "user".equals(classification.role)) {
                title = firstUserText(event);
            }
            messages.add(
                    new ChatMessage(
                            MessageIds.fromSourcePosition(
                                    key,
                                    thread.sessionId,
                                    record.startOffset(),
                                    classification.eventType),
                            key,
                            record.startOffset(),
                            classification.role,
                            classification.eventType,
                            resolved.contentJson(),
                            resolved.payloads(),
                            eventTime,
                            now));
        }

        long lastCommitId = previous == null ? -1L : previous.lastCommitId();
        Instant createdAt = thread.createdAt != null ? thread.createdAt : fileTime(thread.rolloutPath, true);
        Instant updatedAt = thread.updatedAt != null ? thread.updatedAt : fileTime(thread.rolloutPath, false);
        ChatSession session =
                new ChatSession(
                        key,
                        title,
                        cwd,
                        thread.archived,
                        thread.rolloutPath.toAbsolutePath().normalize().toString(),
                        SourceCursors.file(processedOffset, currentFileKey, lastAnchor),
                        lastCommitId,
                        createdAt,
                        updatedAt,
                        lastMessageAt,
                        now);

        boolean cursorChanged = processedOffset != startOffset;
        boolean pendingBoundaryReached =
                previous != null
                        && previous.hasPendingCommit()
                        && processedOffset == targetOffset;
        if (!pendingBoundaryReached
                && !cursorChanged
                && messages.isEmpty()
                && !metadataChanged(previous, session)) {
            return null;
        }
        return new SessionBatch(
                session,
                messages,
                processedRecords,
                previous == null ? null : previous.sourceCursor(),
                previous == null ? -1L : previous.lastCommitId());
    }

    private Map<String, CodexThread> loadThreads() {
        Path database = findStateDatabase();
        if (database == null) {
            return new HashMap<>();
        }
        Map<String, CodexThread> result = new HashMap<>();
        String jdbcUrl = "jdbc:sqlite:file:" + database.toAbsolutePath() + "?mode=ro";
        try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
            Set<String> columns = threadColumns(connection);
            String sql =
                    "SELECT id, rollout_path, title, cwd, archived, created_at, updated_at, "
                            + (columns.contains("created_at_ms")
                                    ? "created_at_ms"
                                    : "NULL AS created_at_ms")
                            + ", "
                            + (columns.contains("updated_at_ms")
                                    ? "updated_at_ms"
                                    : "NULL AS updated_at_ms")
                            + " FROM threads";
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA query_only=ON");
                statement.execute("PRAGMA busy_timeout=5000");
                try (ResultSet rows = statement.executeQuery(sql)) {
                    while (rows.next()) {
                        String id = rows.getString("id");
                        String rollout = rows.getString("rollout_path");
                        if (isBlank(id) || isBlank(rollout)) {
                            continue;
                        }
                        long createdMillis =
                                nullableLong(rows, "created_at_ms", "created_at");
                        long updatedMillis =
                                nullableLong(rows, "updated_at_ms", "updated_at");
                        result.put(
                                id,
                                new CodexThread(
                                        id,
                                        Paths.get(rollout),
                                        rows.getString("title"),
                                        rows.getString("cwd"),
                                        rows.getInt("archived") != 0,
                                        epochMillis(createdMillis),
                                        epochMillis(updatedMillis)));
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Unable to read Codex thread metadata from {}", database, e);
        }
        return result;
    }

    private static Set<String> threadColumns(Connection connection) throws Exception {
        Set<String> result = new HashSet<>();
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("PRAGMA table_info(threads)")) {
            while (rows.next()) {
                result.add(rows.getString("name"));
            }
        }
        return result;
    }

    private void addUnindexedRollouts(Map<String, CodexThread> threads) throws IOException {
        Set<Path> indexedPaths =
                threads.values().stream()
                        .map(thread -> thread.rolloutPath.toAbsolutePath().normalize())
                        .collect(Collectors.toCollection(HashSet::new));
        for (Path directory :
                new Path[] {codexHome.resolve("sessions"), codexHome.resolve("archived_sessions")}) {
            if (!Files.isDirectory(directory)) {
                continue;
            }
            try (Stream<Path> paths = Files.walk(directory)) {
                for (Path path :
                        paths.filter(Files::isRegularFile)
                                .filter(file -> file.getFileName().toString().endsWith(".jsonl"))
                                .collect(Collectors.toList())) {
                    Path normalized = path.toAbsolutePath().normalize();
                    if (indexedPaths.contains(normalized)) {
                        continue;
                    }
                    CodexThread discovered = discoverThread(path, directory.endsWith("archived_sessions"));
                    if (discovered != null) {
                        threads.putIfAbsent(discovered.sessionId, discovered);
                    }
                }
            }
        }
    }

    private CodexThread discoverThread(Path path, boolean archived) {
        try {
            List<JsonlRecord> records = tailReader.read(path, 0, 30);
            for (JsonlRecord record : records) {
                JsonNode event = objectMapper.readTree(record.json());
                if ("session_meta".equals(text(event, "type"))) {
                    JsonNode payload = event.path("payload");
                    String id = firstNonBlank(text(payload, "id"), text(payload, "session_id"));
                    if (isBlank(id)) {
                        break;
                    }
                    return new CodexThread(
                            id,
                            path,
                            null,
                            text(payload, "cwd"),
                            archived,
                            parseInstant(firstNonBlank(text(payload, "timestamp"), text(event, "timestamp"))),
                            fileTime(path, false));
                }
            }
        } catch (Exception e) {
            LOG.debug("Unable to inspect Codex rollout {}", path, e);
        }
        return null;
    }

    private Path findStateDatabase() {
        Path preferred = codexHome.resolve("state_5.sqlite");
        if (Files.isRegularFile(preferred)) {
            return preferred;
        }
        try (Stream<Path> paths = Files.list(codexHome)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().matches("state_[0-9]+\\.sqlite"))
                    .max(Comparator.comparing(path -> path.getFileName().toString()))
                    .orElse(null);
        } catch (IOException e) {
            return null;
        }
    }

    private static EventClassification classify(JsonNode event) {
        String topType = text(event, "type");
        String payloadType = text(event.path("payload"), "type");
        if ("response_item".equals(topType)) {
            if ("message".equals(payloadType)) {
                String role = text(event.path("payload"), "role");
                return new EventClassification(role == null ? "unknown" : role, payloadType);
            }
            if (payloadType != null && payloadType.endsWith("_call")) {
                return new EventClassification("assistant", payloadType);
            }
            if (payloadType != null && payloadType.endsWith("_call_output")) {
                return new EventClassification("tool", payloadType);
            }
        }
        if ("event_msg".equals(topType) && "image_generation_end".equals(payloadType)) {
            return new EventClassification("assistant", payloadType);
        }
        return null;
    }

    private ExtractedEvent extractAttachments(JsonNode event, Path sourcePath) {
        JsonNode sanitized = event.deepCopy();
        List<AttachmentReference> references = new ArrayList<>();
        JsonNode payload = sanitized.path("payload");
        if ("response_item".equals(text(sanitized, "type"))
                && "message".equals(text(payload, "type"))) {
            JsonNode content = payload.path("content");
            if (content.isArray()) {
                Set<String> inlineImagePaths = new HashSet<>();
                String pendingInlineImagePath = null;
                for (JsonNode item : content) {
                    String contentType = text(item, "type");
                    if ("input_text".equals(contentType)) {
                        String wrapperPath = inlineImageWrapperPath(text(item, "text"));
                        if (wrapperPath != null) {
                            pendingInlineImagePath =
                                    normalizedPathReference(wrapperPath, sourcePath.getParent());
                        }
                    } else if ("input_image".equals(contentType)) {
                        JsonNode imageUrl = item.get("image_url");
                        if (imageUrl != null && imageUrl.isTextual()) {
                            String value = imageUrl.asText();
                            AttachmentReference.Kind kind = attachmentKind(value);
                            references.add(
                                    new AttachmentReference(
                                            kind, value, null, dataUriMime(value), sourcePath.getParent()));
                            if (item instanceof com.fasterxml.jackson.databind.node.ObjectNode) {
                                ((com.fasterxml.jackson.databind.node.ObjectNode) item)
                                        .put("image_url", "paimon-blob:" + (references.size() - 1));
                            }
                            if (pendingInlineImagePath != null
                                    && kind != AttachmentReference.Kind.REMOTE_URL) {
                                inlineImagePaths.add(pendingInlineImagePath);
                            }
                        }
                        pendingInlineImagePath = null;
                    }
                }
                for (JsonNode item : content) {
                    if ("input_text".equals(text(item, "type"))) {
                        addMentionedFiles(
                                references,
                                text(item, "text"),
                                sourcePath.getParent(),
                                inlineImagePaths);
                    }
                }
            }
        } else if ("event_msg".equals(text(sanitized, "type"))
                && "image_generation_end".equals(text(payload, "type"))) {
            String savedPath = text(payload, "saved_path");
            String result = text(payload, "result");
            if (isAllowedCodexAttachment(savedPath) && Files.isRegularFile(Paths.get(savedPath))) {
                references.add(
                        new AttachmentReference(
                                AttachmentReference.Kind.LOCAL_PATH,
                                savedPath,
                                fileName(savedPath),
                                "image/png",
                                sourcePath.getParent()));
            } else {
                if (!isBlank(result)) {
                    references.add(
                            new AttachmentReference(
                                    AttachmentReference.Kind.BASE64,
                                    result,
                                    null,
                                    "image/png",
                                    sourcePath.getParent()));
                }
            }
            if (!isBlank(result)
                    && !references.isEmpty()
                    && payload instanceof com.fasterxml.jackson.databind.node.ObjectNode) {
                ((com.fasterxml.jackson.databind.node.ObjectNode) payload)
                        .put("result", "paimon-blob:0");
            }
        }
        return new ExtractedEvent(sanitized, references);
    }

    private void addMentionedFiles(
            List<AttachmentReference> result,
            String text,
            Path baseDirectory,
            Set<String> inlineImagePaths) {
        if (isBlank(text)) {
            return;
        }
        Matcher matcher = MENTIONED_FILE.matcher(text);
        while (matcher.find()) {
            String value = matcher.group(2).trim();
            if (inlineImagePaths.contains(normalizedPathReference(value, baseDirectory))) {
                continue;
            }
            if (!isAllowedCodexAttachment(value)) {
                continue;
            }
            result.add(
                    new AttachmentReference(
                            AttachmentReference.Kind.LOCAL_PATH,
                            value,
                            matcher.group(1).trim(),
                            null,
                            baseDirectory));
        }
    }

    private static String inlineImageWrapperPath(String value) {
        if (isBlank(value)) {
            return null;
        }
        Matcher matcher = INLINE_IMAGE_PATH.matcher(value);
        String lastPath = null;
        while (matcher.find()) {
            lastPath = matcher.group(1);
        }
        return lastPath;
    }

    private static String normalizedPathReference(String value, Path baseDirectory) {
        try {
            Path path;
            if (value.startsWith("file:")) {
                path = Paths.get(java.net.URI.create(value));
            } else if (value.equals("~") || value.startsWith("~/")) {
                String suffix = value.equals("~") ? "" : value.substring(2);
                path = Paths.get(System.getProperty("user.home"), suffix);
            } else {
                path = Paths.get(value);
                if (!path.isAbsolute() && baseDirectory != null) {
                    path = baseDirectory.resolve(path);
                }
            }
            return path.toAbsolutePath().normalize().toString();
        } catch (RuntimeException e) {
            return value;
        }
    }

    private boolean isAllowedCodexAttachment(String value) {
        if (isBlank(value)) {
            return false;
        }
        try {
            Path candidate = Paths.get(normalizedPathReference(value, null));
            return candidate.startsWith(codexHome.resolve("attachments").toAbsolutePath().normalize())
                    || candidate.startsWith(
                            codexHome.resolve("generated_images").toAbsolutePath().normalize());
        } catch (Exception e) {
            return false;
        }
    }

    private static String firstUserText(JsonNode event) {
        JsonNode payload = event.path("payload");
        String value = firstNonBlank(text(payload, "message"), text(payload, "text"));
        if (isBlank(value) && payload.path("content").isArray()) {
            for (JsonNode item : payload.path("content")) {
                if ("input_text".equals(text(item, "type"))) {
                    value = text(item, "text");
                    break;
                }
            }
        }
        if (value == null) {
            return null;
        }
        String singleLine = value.replaceAll("\\s+", " ").trim();
        return singleLine.length() <= 120 ? singleLine : singleLine.substring(0, 120);
    }

    private static boolean metadataChanged(ChatSession previous, ChatSession current) {
        if (previous == null) {
            return true;
        }
        return !java.util.Objects.equals(previous.title(), current.title())
                || !java.util.Objects.equals(previous.cwd(), current.cwd())
                || previous.archived() != current.archived()
                || !java.util.Objects.equals(previous.sourcePath(), current.sourcePath())
                || !java.util.Objects.equals(previous.updatedAt(), current.updatedAt());
    }

    private static long nullableLong(ResultSet rows, String millisColumn, String secondsColumn)
            throws Exception {
        long value = rows.getLong(millisColumn);
        if (!rows.wasNull() && value > 0) {
            return value;
        }
        value = rows.getLong(secondsColumn);
        return rows.wasNull() ? 0L : value * 1000L;
    }

    private static Instant epochMillis(long value) {
        return value <= 0 ? null : Instant.ofEpochMilli(value);
    }

    private static Instant fileTime(Path path, boolean creation) {
        try {
            java.nio.file.attribute.BasicFileAttributes attributes =
                    Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes.class);
            FileTime time = creation ? attributes.creationTime() : attributes.lastModifiedTime();
            return time.toInstant();
        } catch (IOException e) {
            return null;
        }
    }

    private static Instant parseInstant(String value) {
        if (isBlank(value)) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static String firstNonBlank(String first, String second) {
        return isBlank(first) ? second : first;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static boolean isInterruptedFailure(Throwable failure) {
        if (Thread.currentThread().isInterrupted()) {
            return true;
        }
        Throwable current = failure;
        while (current != null) {
            if (current instanceof InterruptedException
                    || current instanceof java.io.InterruptedIOException
                    || current instanceof java.nio.channels.ClosedByInterruptException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String fileName(String value) {
        try {
            return Paths.get(value).getFileName().toString();
        } catch (Exception e) {
            return null;
        }
    }

    private ChatMessage parseErrorMessage(
            SessionKey key, String sourceIdentity, JsonlRecord record, Instant ingestedAt) {
        com.fasterxml.jackson.databind.node.ObjectNode value = objectMapper.createObjectNode();
        value.put("_paimon_parse_error", true);
        value.put("raw_line", record.json());
        return new ChatMessage(
                MessageIds.fromSourcePosition(
                        key, sourceIdentity, record.startOffset(), "parse_error"),
                key,
                record.startOffset(),
                "unknown",
                "parse_error",
                value.toString(),
                java.util.Collections.emptyList(),
                null,
                ingestedAt);
    }

    private static AttachmentReference.Kind attachmentKind(String value) {
        if (value.startsWith("data:")) {
            return AttachmentReference.Kind.DATA_URI;
        }
        if (value.startsWith("http://") || value.startsWith("https://")) {
            return AttachmentReference.Kind.REMOTE_URL;
        }
        return AttachmentReference.Kind.LOCAL_PATH;
    }

    private static String dataUriMime(String value) {
        if (!value.startsWith("data:")) {
            return null;
        }
        int end = value.indexOf(';');
        return end > 5 ? value.substring(5, end) : null;
    }

    private static final class EventClassification {
        private final String role;
        private final String eventType;

        private EventClassification(String role, String eventType) {
            this.role = role;
            this.eventType = eventType;
        }
    }

    private static final class ExtractedEvent {
        private final JsonNode sanitizedEvent;
        private final List<AttachmentReference> references;

        private ExtractedEvent(
                JsonNode sanitizedEvent, List<AttachmentReference> references) {
            this.sanitizedEvent = sanitizedEvent;
            this.references = references;
        }
    }

    private static final class CodexThread {
        private final String sessionId;
        private final Path rolloutPath;
        private final String title;
        private final String cwd;
        private final boolean archived;
        private final Instant createdAt;
        private final Instant updatedAt;

        private CodexThread(
                String sessionId,
                Path rolloutPath,
                String title,
                String cwd,
                boolean archived,
                Instant createdAt,
                Instant updatedAt) {
            this.sessionId = sessionId;
            this.rolloutPath = rolloutPath;
            this.title = title;
            this.cwd = cwd;
            this.archived = archived;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
        }

        private Instant updatedAt() {
            return updatedAt;
        }
    }
}
