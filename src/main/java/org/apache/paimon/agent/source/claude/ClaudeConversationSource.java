package org.apache.paimon.agent.source.claude;

import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.MessageIds;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.source.AttachmentReference;
import org.apache.paimon.agent.source.AttachmentResolver;
import org.apache.paimon.agent.source.ConversationSource;
import org.apache.paimon.agent.source.IncrementalFiles;
import org.apache.paimon.agent.source.JsonlRecord;
import org.apache.paimon.agent.source.JsonlTailReader;
import org.apache.paimon.agent.source.ResolvedAttachments;
import org.apache.paimon.agent.source.ScanFileSnapshot;
import org.apache.paimon.agent.source.SourceCursors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Incrementally reads top-level Claude Code project transcripts. */
public final class ClaudeConversationSource implements ConversationSource {

    public static final String SOURCE_TYPE = "claude";

    private static final Logger LOG = LoggerFactory.getLogger(ClaudeConversationSource.class);

    private final Path claudeHome;
    private final ObjectMapper objectMapper;
    private final AttachmentResolver attachmentResolver;
    private final JsonlTailReader tailReader;
    private int nextTranscriptIndex;

    public ClaudeConversationSource(
            Path claudeHome, ObjectMapper objectMapper, AttachmentResolver attachmentResolver) {
        this.claudeHome = claudeHome;
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
        try (ScanCycle cycle = openScanCycle()) {
            return cycle.scan(checkpoints, maxRecords, onlySessions);
        }
    }

    @Override
    public ScanCycle openScanCycle() throws Exception {
        ScanWindow window = captureScanWindow();
        return new ScanCycle() {
            @Override
            public List<SessionBatch> scan(
                    Map<SessionKey, ChatSession> checkpoints,
                    int maxRecords,
                    Set<SessionKey> onlySessions)
                    throws Exception {
                return scanWindow(window, checkpoints, maxRecords, onlySessions);
            }
        };
    }

    private ScanWindow captureScanWindow() throws Exception {
        Path projects = claudeHome.resolve("projects");
        if (!Files.isDirectory(projects)) {
            LOG.debug("Claude projects directory does not exist: {}", projects);
            return new ScanWindow(
                    java.util.Collections.emptyMap(),
                    java.util.Collections.emptyList(),
                    java.util.Collections.emptyMap());
        }

        Map<String, ClaudeMetadata> metadata = loadIndexes(projects);
        List<Path> transcripts = discoverTranscripts(projects);
        Map<Path, ScanFileSnapshot> files = new HashMap<>();
        for (Path transcript : transcripts) {
            try {
                ScanFileSnapshot snapshot = ScanFileSnapshot.capture(transcript);
                if (snapshot != null) {
                    files.put(transcript, snapshot);
                }
            } catch (IOException e) {
                LOG.debug(
                        "Claude transcript disappeared while opening the scan cycle: {}",
                        transcript,
                        e);
            }
        }
        return new ScanWindow(metadata, transcripts, files);
    }

    private List<SessionBatch> scanWindow(
            ScanWindow window,
            Map<SessionKey, ChatSession> checkpoints,
            int maxRecords,
            Set<SessionKey> onlySessions)
            throws Exception {
        List<SessionBatch> batches = new ArrayList<>();
        int remaining = maxRecords;
        int transcriptCount = window.transcripts.size();
        int transcriptStart =
                transcriptCount == 0 ? 0 : Math.floorMod(nextTranscriptIndex, transcriptCount);
        if (transcriptCount > 0) {
            nextTranscriptIndex = (transcriptStart + 1) % transcriptCount;
        }
        for (int transcriptOffset = 0;
                transcriptOffset < transcriptCount;
                transcriptOffset++) {
            if (remaining <= 0) {
                break;
            }
            Path transcript =
                    window.transcripts.get(
                            (transcriptStart + transcriptOffset) % transcriptCount);
            String sessionId = stripJsonl(transcript.getFileName().toString());
            SessionKey key = new SessionKey(SOURCE_TYPE, sessionId);
            if (!onlySessions.isEmpty() && !onlySessions.contains(key)) {
                continue;
            }
            SessionBatch batch;
            try {
                batch =
                        scanTranscript(
                                transcript,
                                sessionId,
                                window.metadata.get(sessionId),
                                window.files.get(transcript),
                                checkpoints.get(key),
                                remaining);
            } catch (AttachmentResolver.RetryableAttachmentException | IOException failure) {
                if (isInterruptedFailure(failure)) {
                    throw failure;
                }
                LOG.warn(
                        "Unable to scan Claude session {} from {}; this session will be retried",
                        sessionId,
                        transcript,
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

    private SessionBatch scanTranscript(
            Path transcript,
            String sessionId,
            ClaudeMetadata metadata,
            ScanFileSnapshot fileSnapshot,
            ChatSession previous,
            int maxRecords)
            throws IOException {
        if (fileSnapshot == null || !fileSnapshot.canRead(transcript)) {
            LOG.debug(
                    "Claude transcript {} changed identity or was truncated during this scan "
                            + "cycle; deferring it to the next wake-up",
                    transcript);
            return null;
        }
        SourceCursors.FileCursor priorCursor =
                SourceCursors.parseFileCursor(previous == null ? null : previous.sourceCursor());
        SourceCursors.FileCursor targetCursor =
                SourceCursors.parseFileCursor(
                        previous == null ? null : previous.pendingCursor());
        String currentFileKey = fileSnapshot.fileKey();
        long startOffset = priorCursor.offset();
        String startAnchor = priorCursor.anchor();
        boolean checkpointRemapped = false;
        String currentSourcePath = transcript.toAbsolutePath().normalize().toString();
        boolean checkpointMismatch =
                fileSnapshot.size() < startOffset
                        || (previous != null
                                && !Objects.equals(previous.sourcePath(), currentSourcePath))
                        || (priorCursor.fileKey() != null
                                && !Objects.equals(priorCursor.fileKey(), currentFileKey))
                        || !IncrementalFiles.anchorMatchesAtOffset(transcript, priorCursor);
        boolean missingCursorIdentity =
                previous != null
                        && priorCursor.fileKey() == null
                        && priorCursor.anchor() == null;
        IncrementalFiles.RestoreBoundary restoreBoundary =
                !checkpointMismatch && !missingCursorIdentity
                        ? null
                        : IncrementalFiles.findLastRestoreBoundary(
                                tailReader, transcript, fileSnapshot.size(), previous);
        if (restoreBoundary != null) {
            startOffset = restoreBoundary.offset();
            startAnchor = restoreBoundary.anchor();
            checkpointRemapped = true;
            LOG.info(
                    "Resuming restored Claude session {} after its local restore boundary",
                    sessionId);
        } else if (checkpointMismatch) {
            long recovered =
                    IncrementalFiles.findOffsetAfterAnchor(
                            tailReader,
                            transcript,
                            priorCursor.anchor(),
                            priorCursor.offset(),
                            fileSnapshot.size());
            if (recovered < 0) {
                LOG.warn(
                        "Claude transcript {} was rewritten and its checkpoint anchor disappeared; "
                                + "the session is paused to avoid duplicate append rows",
                        transcript);
                return null;
            }
            startOffset = recovered;
            checkpointRemapped = true;
        }
        long targetOffset = -1L;
        if (previous != null && previous.hasPendingCommit()) {
            targetOffset = targetCursor.offset();
            if ((targetCursor.fileKey() != null
                            && !Objects.equals(targetCursor.fileKey(), currentFileKey))
                    || !IncrementalFiles.anchorMatchesAtOffset(transcript, targetCursor)) {
                targetOffset =
                        IncrementalFiles.findOffsetAfterAnchor(
                                tailReader,
                                transcript,
                                targetCursor.anchor(),
                                targetCursor.offset(),
                                fileSnapshot.size());
                if (targetOffset < 0) {
                    LOG.warn(
                            "Pending Claude boundary disappeared for session {}; recovery is paused",
                            sessionId);
                    return null;
                }
            }
        }

        List<JsonlRecord> records =
                tailReader.read(transcript, startOffset, maxRecords, fileSnapshot.size());
        List<ChatMessage> messages = new ArrayList<>();
        SessionKey key = new SessionKey(SOURCE_TYPE, sessionId);
        long processedOffset = startOffset;
        int processedRecords = 0;
        String lastAnchor = startAnchor;
        String title =
                previous != null && !isBlank(previous.title())
                        ? previous.title()
                        : metadata == null ? null : metadata.title();
        String cwd =
                previous != null && !isBlank(previous.cwd())
                        ? previous.cwd()
                        : metadata == null ? null : metadata.projectPath;
        Instant createdAt =
                previous != null && previous.createdAt() != null
                        ? previous.createdAt()
                        : metadata == null ? null : metadata.createdAt;
        Instant updatedAt =
                maxInstant(
                        previous == null ? null : previous.updatedAt(),
                        metadata == null ? null : metadata.updatedAt);
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
            processedOffset = record.endOffset();
            lastAnchor = IncrementalFiles.lineAnchor(record.json());
            if (record.json().trim().isEmpty()) {
                continue;
            }

            JsonNode event;
            try {
                event = objectMapper.readTree(record.json());
            } catch (IOException e) {
                LOG.warn(
                        "Skipping malformed Claude JSONL record at {}:{}",
                        transcript,
                        record.startOffset());
                messages.add(parseErrorMessage(key, sessionId, record, now));
                continue;
            }

            String eventSessionId = text(event, "sessionId");
            if (!isBlank(eventSessionId) && !sessionId.equals(eventSessionId)) {
                LOG.warn(
                        "Ignoring Claude event whose sessionId {} does not match transcript {}",
                        eventSessionId,
                        sessionId);
                continue;
            }

            String eventType = text(event, "type");
            if ("custom-title".equals(eventType)) {
                title = firstNonBlank(text(event, "customTitle"), text(event.path("custom-title"), "customTitle"));
                continue;
            }
            if ("ai-title".equals(eventType)) {
                String aiTitle = firstNonBlank(text(event, "aiTitle"), text(event.path("ai-title"), "aiTitle"));
                if (!isBlank(aiTitle)) {
                    title = aiTitle;
                }
                continue;
            }
            if (!isConversationEvent(eventType, event)) {
                continue;
            }

            String eventCwd = text(event, "cwd");
            if (!isBlank(eventCwd)) {
                cwd = eventCwd;
            }
            Instant eventTime = parseInstant(event.get("timestamp"));
            if (createdAt == null && eventTime != null) {
                createdAt = eventTime;
            }
            if (eventTime != null) {
                updatedAt = eventTime;
                lastMessageAt = eventTime;
            }
            if (isBlank(title) && "user".equals(eventType)) {
                title = firstUserText(event.path("message").path("content"));
            }

            ExtractedEvent extracted = extractAttachments(event, transcript.getParent());
            ResolvedAttachments resolved =
                    attachmentResolver.resolve(extracted.sanitizedEvent, extracted.references);
            String role = firstNonBlank(text(event.path("message"), "role"), eventType);
            String uuid = text(event, "uuid");
            String identity = isBlank(uuid) ? transcript.toString() : uuid;
            messages.add(
                    new ChatMessage(
                            MessageIds.fromSourcePosition(
                                    key, identity, isBlank(uuid) ? record.startOffset() : 0L, eventType),
                            key,
                            record.startOffset(),
                            role,
                            eventType,
                            resolved.contentJson(),
                            resolved.payloads(),
                            eventTime,
                            now));
        }

        if (createdAt == null) {
            createdAt = fileSnapshot.creationTime();
        }
        if (updatedAt == null) {
            updatedAt = fileSnapshot.lastModifiedTime();
        }
        ChatSession session =
                new ChatSession(
                        key,
                        title,
                        cwd,
                        false,
                        currentSourcePath,
                        SourceCursors.file(processedOffset, currentFileKey, lastAnchor),
                        previous == null ? -1L : previous.lastCommitId(),
                        createdAt,
                        updatedAt,
                        lastMessageAt,
                        now,
                        null);
        boolean pendingBoundaryReached =
                previous != null
                        && previous.hasPendingCommit()
                        && processedOffset == targetOffset;
        if (!pendingBoundaryReached
                && processedOffset == startOffset
                && !checkpointRemapped
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

    private ExtractedEvent extractAttachments(JsonNode event, Path baseDirectory) {
        JsonNode sanitized = event.deepCopy();
        List<AttachmentReference> references = new ArrayList<>();
        extractRecursively(sanitized, baseDirectory, references);
        return new ExtractedEvent(sanitized, references);
    }

    private void extractRecursively(
            JsonNode node, Path baseDirectory, List<AttachmentReference> references) {
        if (node == null || node.isValueNode()) {
            return;
        }
        if (node.isArray()) {
            for (JsonNode child : node) {
                extractRecursively(child, baseDirectory, references);
            }
            return;
        }

        String type = text(node, "type");
        if (("image".equals(type) || "document".equals(type)) && node instanceof ObjectNode) {
            JsonNode source = node.path("source");
            if (source instanceof ObjectNode && "base64".equals(text(source, "type"))) {
                String data = text(source, "data");
                if (!isBlank(data)) {
                    int index = references.size();
                    references.add(
                            new AttachmentReference(
                                    AttachmentReference.Kind.BASE64,
                                    data,
                                    text(node, "title"),
                                    text(source, "media_type"),
                                    baseDirectory));
                    ((ObjectNode) source).put("data", "paimon-blob:" + index);
                }
            }
            JsonNode file = node.get("file");
            String filePath = filePath(file);
            if (!isBlank(filePath)) {
                addLocalReference(references, filePath, baseDirectory);
            }
        }
        if ("file".equals(type)) {
            String path =
                    firstNonBlank(
                            text(node, "filePath"),
                            firstNonBlank(text(node, "path"), filePath(node.get("file"))));
            if (!isBlank(path)) {
                addLocalReference(references, path, baseDirectory);
            }
        }

        List<JsonNode> children = new ArrayList<>();
        node.elements().forEachRemaining(children::add);
        for (JsonNode child : children) {
            extractRecursively(child, baseDirectory, references);
        }
    }

    private static void addLocalReference(
            List<AttachmentReference> references, String filePath, Path baseDirectory) {
        for (AttachmentReference reference : references) {
            if (reference.kind() == AttachmentReference.Kind.LOCAL_PATH
                    && reference.value().equals(filePath)) {
                return;
            }
        }
        references.add(
                new AttachmentReference(
                        AttachmentReference.Kind.LOCAL_PATH,
                        filePath,
                        fileName(filePath),
                        null,
                        baseDirectory));
    }

    private Map<String, ClaudeMetadata> loadIndexes(Path projects) throws IOException {
        Map<String, ClaudeMetadata> result = new HashMap<>();
        try (Stream<Path> directories = Files.list(projects)) {
            for (Path directory :
                    directories.filter(Files::isDirectory).collect(Collectors.toList())) {
                Path index = directory.resolve("sessions-index.json");
                if (!Files.isRegularFile(index)) {
                    continue;
                }
                try {
                    JsonNode root = objectMapper.readTree(index.toFile());
                    for (JsonNode entry : root.path("entries")) {
                        String sessionId = text(entry, "sessionId");
                        if (isBlank(sessionId)) {
                            continue;
                        }
                        result.put(
                                sessionId,
                                new ClaudeMetadata(
                                        firstNonBlank(
                                                text(entry, "summary"), text(entry, "firstPrompt")),
                                        firstNonBlank(
                                                text(entry, "projectPath"), text(root, "originalPath")),
                                        parseInstant(entry.get("created")),
                                        parseInstant(entry.get("modified"))));
                    }
                } catch (Exception e) {
                    LOG.debug("Ignoring incomplete Claude session index {}", index, e);
                }
            }
        }
        return result;
    }

    private static List<Path> discoverTranscripts(Path projects) throws IOException {
        List<Path> result = new ArrayList<>();
        try (Stream<Path> directories = Files.list(projects)) {
            for (Path directory :
                    directories.filter(Files::isDirectory).collect(Collectors.toList())) {
                try (Stream<Path> files = Files.list(directory)) {
                    files.filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString().endsWith(".jsonl"))
                            .filter(
                                    path ->
                                            !path.getFileName()
                                                    .toString()
                                                    .equals("sessions-index.jsonl"))
                            .forEach(result::add);
                }
            }
        }
        result.sort(
                Comparator.comparingLong(ClaudeConversationSource::lastModifiedMillis)
                        .reversed());
        return result;
    }

    private static long lastModifiedMillis(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException e) {
            return 0L;
        }
    }

    private static boolean isConversationEvent(String type, JsonNode event) {
        return "user".equals(type)
                || "assistant".equals(type)
                || ("system".equals(type) && event.has("message"))
                || "attachment".equals(type);
    }

    private static String firstUserText(JsonNode content) {
        String value = null;
        if (content.isTextual()) {
            value = content.asText();
        } else if (content.isArray()) {
            for (JsonNode item : content) {
                if ("text".equals(text(item, "type"))) {
                    value = text(item, "text");
                    break;
                }
            }
        }
        if (isBlank(value)) {
            return null;
        }
        String singleLine = value.replaceAll("\\s+", " ").trim();
        return singleLine.length() <= 120 ? singleLine : singleLine.substring(0, 120);
    }

    private static String filePath(JsonNode file) {
        if (file == null || file.isNull()) {
            return null;
        }
        if (file.isTextual()) {
            return file.asText();
        }
        return firstNonBlank(text(file, "filePath"), text(file, "path"));
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
        ObjectNode value = objectMapper.createObjectNode();
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

    private static boolean metadataChanged(ChatSession previous, ChatSession current) {
        if (previous == null) {
            return true;
        }
        return !Objects.equals(previous.title(), current.title())
                || !Objects.equals(previous.cwd(), current.cwd())
                || !Objects.equals(previous.sourcePath(), current.sourcePath())
                || !Objects.equals(previous.updatedAt(), current.updatedAt());
    }

    private static Instant parseInstant(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            long value = node.asLong();
            return Instant.ofEpochMilli(value < 10_000_000_000L ? value * 1000L : value);
        }
        try {
            return Instant.parse(node.asText());
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static Instant maxInstant(Instant first, Instant second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return first.isAfter(second) ? first : second;
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

    private static String stripJsonl(String value) {
        return value.substring(0, value.length() - ".jsonl".length());
    }

    private static final class ExtractedEvent {
        private final JsonNode sanitizedEvent;
        private final List<AttachmentReference> references;

        private ExtractedEvent(JsonNode sanitizedEvent, List<AttachmentReference> references) {
            this.sanitizedEvent = sanitizedEvent;
            this.references = references;
        }
    }

    private static final class ScanWindow {
        private final Map<String, ClaudeMetadata> metadata;
        private final List<Path> transcripts;
        private final Map<Path, ScanFileSnapshot> files;

        private ScanWindow(
                Map<String, ClaudeMetadata> metadata,
                List<Path> transcripts,
                Map<Path, ScanFileSnapshot> files) {
            this.metadata = metadata;
            this.transcripts = transcripts;
            this.files = files;
        }
    }

    private static final class ClaudeMetadata {
        private final String summary;
        private final String projectPath;
        private final Instant createdAt;
        private final Instant updatedAt;

        private ClaudeMetadata(
                String summary, String projectPath, Instant createdAt, Instant updatedAt) {
            this.summary = summary;
            this.projectPath = projectPath;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
        }

        private String title() {
            return summary;
        }
    }
}
