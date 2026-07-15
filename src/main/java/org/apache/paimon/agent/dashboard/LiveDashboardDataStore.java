package org.apache.paimon.agent.dashboard;

import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.service.PendingDataSnapshot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Merges the collector's immutable pending snapshot with rows already readable from Paimon. */
public final class LiveDashboardDataStore implements DashboardDataStore {

    private final DashboardDataStore uploaded;
    private final Supplier<PendingDataSnapshot> pendingData;
    private final LongSupplier commitGeneration;
    private final int maxRows;
    private final ObjectMapper objectMapper;

    private boolean pendingStateObserved;
    private boolean pendingWasNonEmpty;
    private long pendingCommitIdentifier = -1L;
    private boolean commitGenerationObserved;
    private long observedCommitGeneration;
    private volatile boolean closed;

    public LiveDashboardDataStore(
            DashboardDataStore uploaded,
            Supplier<PendingDataSnapshot> pendingData,
            int maxRows) {
        this(uploaded, pendingData, () -> 0L, maxRows);
    }

    public LiveDashboardDataStore(
            DashboardDataStore uploaded,
            Supplier<PendingDataSnapshot> pendingData,
            LongSupplier commitGeneration,
            int maxRows) {
        this.uploaded = Objects.requireNonNull(uploaded, "uploaded");
        this.pendingData = Objects.requireNonNull(pendingData, "pendingData");
        this.commitGeneration =
                Objects.requireNonNull(commitGeneration, "commitGeneration");
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be greater than zero");
        }
        this.maxRows = maxRows;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public DashboardOverview overview() throws Exception {
        ensureOpen();
        snapshot();
        return uploaded.overview();
    }

    @Override
    public DashboardPage<DashboardSession> listSessions(SessionQuery query) throws Exception {
        ensureOpen();
        Objects.requireNonNull(query, "query");
        validatePage(query.getPage(), query.getPageSize());
        PendingDataSnapshot pending = snapshot();
        DashboardPage<DashboardSession> uploadedPage =
                uploaded.listSessions(
                        new SessionQuery(
                                query.getSourceType(),
                                query.getSearch(),
                                query.getArchived(),
                                1,
                                maxRows));
        Set<String> pendingSubagentKeys = new HashSet<>();
        for (SessionBatch batch : pending.batches()) {
            ChatSession session = batch.session();
            if (session.subagentSourceJson() != null) {
                pendingSubagentKeys.add(
                        sessionKey(session.key().sourceType(), session.key().sessionId()));
            }
        }
        Map<String, DashboardSession> rows = new LinkedHashMap<>();
        for (DashboardSession session : uploadedPage.getItems()) {
            String key = sessionKey(session.getSourceType(), session.getSessionId());
            if (session.getSubagentSourceJson() == null
                    && !pendingSubagentKeys.contains(key)) {
                rows.put(key, session);
            }
        }
        for (SessionBatch batch : pending.batches()) {
            ChatSession session = batch.session();
            String key =
                    sessionKey(session.key().sourceType(), session.key().sessionId());
            if (pendingSubagentKeys.contains(key)) {
                // A legacy uploaded row can still have a null marker until the schema backfill
                // reaches it. The pending snapshot is newer, so use it as a tombstone for the
                // sidebar instead of briefly showing the subagent as a top-level session.
                rows.remove(key);
                continue;
            }
            if (matches(session, query)) {
                DashboardSession row = pendingSession(session, pending.commitIdentifier());
                rows.put(key, row);
            }
        }

        List<DashboardSession> matches = new ArrayList<>(rows.values());
        matches.sort(
                Comparator.comparing(
                                (DashboardSession session) ->
                                        session.getStorageStatus()
                                                != DashboardStorageStatus.PENDING)
                        .thenComparing(
                                DashboardSession::getLastMessageAt,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(
                                DashboardSession::getUpdatedAt,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(DashboardSession::getSourceType)
                        .thenComparing(DashboardSession::getSessionId));
        boolean truncated = uploadedPage.isTruncated() || matches.size() > maxRows;
        return page(limit(matches), query.getPage(), query.getPageSize(), truncated);
    }

    @Override
    public DashboardPage<DashboardMessage> listMessages(MessageQuery query) throws Exception {
        ensureOpen();
        Objects.requireNonNull(query, "query");
        validatePage(query.getPage(), query.getPageSize());
        PendingDataSnapshot pending = snapshot();
        DashboardPage<DashboardMessage> uploadedPage =
                uploaded.listMessages(
                        new MessageQuery(
                                query.getSourceType(),
                                query.getSessionId(),
                                query.getRole(),
                                query.getEventType(),
                                query.getSearch(),
                                query.isConversationOnly(),
                                1,
                                maxRows));
        Map<String, DashboardMessage> rows = new LinkedHashMap<>();
        for (DashboardMessage message : uploadedPage.getItems()) {
            rows.put(messageKey(message), message);
        }
        for (SessionBatch batch : pending.batches()) {
            for (ChatMessage message : batch.messages()) {
                String preview =
                        query.isConversationOnly()
                                ? DashboardContentPreview.conversationPreviewMessage(
                                        message.contentJson(),
                                        message.role(),
                                        message.eventType())
                                : DashboardContentPreview.previewMessage(
                                        message.contentJson(),
                                        message.role(),
                                        message.eventType());
                if (matches(message, query, preview)) {
                    DashboardMessage row = pendingMessage(message, preview);
                    rows.put(messageKey(row), row);
                }
            }
        }

        List<DashboardMessage> matches = new ArrayList<>(rows.values());
        matches.sort(
                Comparator.comparing(
                                (DashboardMessage message) ->
                                        message.getStorageStatus()
                                                != DashboardStorageStatus.PENDING)
                        .thenComparing(
                                DashboardMessage::getCreatedAt,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(
                                DashboardMessage::getIngestedAt,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(DashboardMessage::getSourceType)
                        .thenComparing(DashboardMessage::getSessionId)
                        .thenComparing(
                                DashboardMessage::getSequenceNo, Comparator.reverseOrder())
                        .thenComparing(DashboardMessage::getMessageId));
        boolean truncated = uploadedPage.isTruncated() || matches.size() > maxRows;
        return page(limit(matches), query.getPage(), query.getPageSize(), truncated);
    }

    @Override
    public Optional<DashboardMessageDetail> messageDetail(
            String sourceType, String sessionId, String messageId, long sequenceNo)
            throws Exception {
        ensureOpen();
        Optional<ChatMessage> pending =
                pendingMessage(sourceType, sessionId, messageId, sequenceNo);
        if (pending.isPresent()) {
            return Optional.of(detail(pending.get()));
        }
        return uploaded.messageDetail(sourceType, sessionId, messageId, sequenceNo);
    }

    @Override
    public Optional<AttachmentData> attachment(
            String sourceType,
            String sessionId,
            String messageId,
            long sequenceNo,
            int index,
            long maxBytes)
            throws Exception {
        ensureOpen();
        if (index < 0) {
            throw new IllegalArgumentException("attachment index must not be negative");
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be greater than zero");
        }
        Optional<ChatMessage> pending =
                pendingMessage(sourceType, sessionId, messageId, sequenceNo);
        if (!pending.isPresent()) {
            return uploaded.attachment(
                    sourceType, sessionId, messageId, sequenceNo, index, maxBytes);
        }
        ChatMessage message = pending.get();
        if (index >= message.attachments().size()) {
            return Optional.empty();
        }
        AttachmentPayload payload = message.attachments().get(index);
        if (payload.isMissing()) {
            return Optional.empty();
        }
        if (payload.size() > maxBytes) {
            throw new DashboardAttachmentTooLargeException(payload.size(), maxBytes);
        }
        AttachmentMetadata metadata = attachmentMetadata(message.contentJson()).get(index);
        return Optional.of(
                new AttachmentData(
                        payload.bytes(),
                        valueOrDefault(
                                metadata == null ? null : metadata.mimeType,
                                "application/octet-stream"),
                        valueOrDefault(
                                metadata == null ? null : metadata.fileName,
                                "attachment-" + index)));
    }

    private Optional<ChatMessage> pendingMessage(
            String sourceType, String sessionId, String messageId, long sequenceNo) {
        ChatMessage match = null;
        for (SessionBatch batch : snapshot().batches()) {
            for (ChatMessage message : batch.messages()) {
                if (message.sequenceNumber() == sequenceNo
                        && message.messageId().equals(messageId)
                        && message.sessionKey().sourceType().equals(sourceType)
                        && message.sessionKey().sessionId().equals(sessionId)) {
                    if (match != null) {
                        throw new IllegalStateException(
                                "More than one pending message matched "
                                        + sourceType
                                        + '/'
                                        + sessionId
                                        + '/'
                                        + messageId
                                        + '/'
                                        + sequenceNo);
                    }
                    match = message;
                }
            }
        }
        return Optional.ofNullable(match);
    }

    private DashboardMessageDetail detail(ChatMessage message) {
        Map<Integer, AttachmentMetadata> metadata = attachmentMetadata(message.contentJson());
        List<DashboardAttachment> attachments = new ArrayList<>();
        int count = message.attachments().size();
        for (Integer index : metadata.keySet()) {
            count = Math.max(count, index + 1);
        }
        for (int index = 0; index < count; index++) {
            AttachmentPayload payload =
                    index < message.attachments().size()
                            ? message.attachments().get(index)
                            : AttachmentPayload.missing();
            AttachmentMetadata item = metadata.get(index);
            boolean present = !payload.isMissing();
            attachments.add(
                    new DashboardAttachment(
                            index,
                            present,
                            present ? payload.size() : metadataSize(item),
                            item == null ? null : item.mimeType,
                            item == null ? null : item.fileName,
                            item == null ? (present ? "stored" : "missing") : item.status,
                            item == null ? null : item.sha256));
        }
        return new DashboardMessageDetail(
                message.messageId(),
                message.sessionKey().sourceType(),
                message.sessionKey().sessionId(),
                message.sequenceNumber(),
                message.role(),
                message.eventType(),
                message.contentJson(),
                attachments,
                message.createdAt(),
                message.ingestedAt(),
                DashboardStorageStatus.PENDING);
    }

    private synchronized PendingDataSnapshot snapshot() {
        PendingDataSnapshot value = pendingData.get();
        value = value == null ? PendingDataSnapshot.empty() : value;
        long currentCommitGeneration = commitGeneration.getAsLong();
        boolean nonEmpty = !value.isEmpty();
        long commitIdentifier = nonEmpty ? value.commitIdentifier() : -1L;
        boolean generationAdvanced =
                commitGenerationObserved
                        && currentCommitGeneration != observedCommitGeneration;
        boolean observedPendingCommitCompleted =
                pendingStateObserved
                        && pendingWasNonEmpty
                        && (!nonEmpty || commitIdentifier != pendingCommitIdentifier);
        if (generationAdvanced || observedPendingCommitCompleted) {
            // pendingData() and commitGeneration() are lock-free immutable collector views. Once
            // the generation advances, the Paimon commit has completed. Keep the pending
            // transition fallback for standalone/legacy callers which do not provide a
            // generation. Drop uploaded caches before removing the overlay, otherwise newly
            // committed rows can disappear for the cache TTL.
            uploaded.invalidate();
        }
        pendingStateObserved = true;
        pendingWasNonEmpty = nonEmpty;
        pendingCommitIdentifier = commitIdentifier;
        commitGenerationObserved = true;
        observedCommitGeneration = currentCommitGeneration;
        return value;
    }

    @Override
    public void invalidate() {
        ensureOpen();
        uploaded.invalidate();
    }

    private static DashboardSession pendingSession(ChatSession session, long commitIdentifier) {
        Long pendingCommit = commitIdentifier < 0 ? null : commitIdentifier;
        return new DashboardSession(
                session.key().sourceType(),
                session.key().sessionId(),
                session.title(),
                session.cwd(),
                session.archived(),
                session.sourcePath(),
                session.sourceCursor(),
                session.lastCommitId(),
                pendingCommit,
                session.sourceCursor(),
                session.createdAt(),
                session.updatedAt(),
                session.lastMessageAt(),
                session.ingestedAt(),
                session.subagentSourceJson(),
                session.projectless(),
                DashboardStorageStatus.PENDING);
    }

    private static DashboardMessage pendingMessage(ChatMessage message, String contentPreview) {
        return new DashboardMessage(
                message.messageId(),
                message.sessionKey().sourceType(),
                message.sessionKey().sessionId(),
                message.sequenceNumber(),
                message.role(),
                message.eventType(),
                contentPreview,
                message.contentJson().length(),
                message.createdAt(),
                message.ingestedAt(),
                DashboardStorageStatus.PENDING);
    }

    private static boolean matches(ChatSession session, SessionQuery query) {
        String sourceType = normalize(query.getSourceType());
        if (sourceType != null && !sourceType.equals(session.key().sourceType())) {
            return false;
        }
        if (query.getArchived() != null && query.getArchived() != session.archived()) {
            return false;
        }
        String search = normalizedSearch(query.getSearch());
        return search == null
                || contains(session.key().sessionId(), search)
                || contains(session.title(), search)
                || contains(session.cwd(), search);
    }

    private static boolean matches(
            ChatMessage message, MessageQuery query, String contentPreview) {
        if (!matches(normalize(query.getSourceType()), message.sessionKey().sourceType())
                || !matches(normalize(query.getSessionId()), message.sessionKey().sessionId())
                || !matches(normalize(query.getRole()), message.role())
                || !matches(normalize(query.getEventType()), message.eventType())
                || (query.isConversationOnly()
                        && !MessageQuery.isConversationalMessage(
                                message.role(), message.eventType()))) {
            return false;
        }
        if (query.isConversationOnly() && contentPreview.isEmpty()) {
            return false;
        }
        String search = normalizedSearch(query.getSearch());
        return search == null
                || contains(message.messageId(), search)
                || contains(message.contentJson(), search);
    }

    private static boolean matches(String expected, String actual) {
        return expected == null || expected.equals(actual);
    }

    private Map<Integer, AttachmentMetadata> attachmentMetadata(String contentJson) {
        if (contentJson == null || contentJson.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            JsonNode manifest = objectMapper.readTree(contentJson).path("_paimon_attachments");
            if (!manifest.isArray()) {
                return Collections.emptyMap();
            }
            Map<Integer, AttachmentMetadata> result = new HashMap<>();
            int ordinal = 0;
            for (JsonNode item : manifest) {
                int index =
                        item.path("index").isIntegralNumber()
                                ? item.path("index").asInt()
                                : ordinal;
                if (index >= 0) {
                    result.put(
                            index,
                            new AttachmentMetadata(
                                    text(item, "mime_type"),
                                    text(item, "file_name"),
                                    text(item, "status"),
                                    text(item, "sha256"),
                                    item.path("size").isIntegralNumber()
                                            ? item.path("size").asLong()
                                            : -1L));
                }
                ordinal++;
            }
            return result;
        } catch (IOException ignored) {
            return Collections.emptyMap();
        }
    }

    private <T> List<T> limit(List<T> rows) {
        return rows.size() <= maxRows ? rows : new ArrayList<>(rows.subList(0, maxRows));
    }

    private static <T> DashboardPage<T> page(
            List<T> matches, int page, int pageSize, boolean truncated) {
        long offset = (page - 1L) * pageSize;
        if (offset >= matches.size()) {
            return new DashboardPage<>(
                    Collections.emptyList(), page, pageSize, matches.size(), truncated);
        }
        int from = (int) offset;
        int to = (int) Math.min((long) matches.size(), offset + pageSize);
        return new DashboardPage<>(
                matches.subList(from, to), page, pageSize, matches.size(), truncated);
    }

    private void validatePage(int page, int pageSize) {
        if (page <= 0) {
            throw new IllegalArgumentException("page must be greater than zero");
        }
        if (pageSize <= 0 || pageSize > maxRows) {
            throw new IllegalArgumentException(
                    "pageSize must be between 1 and maxRows=" + maxRows);
        }
    }

    private static String sessionKey(String sourceType, String sessionId) {
        return sourceType + '\u0000' + sessionId;
    }

    private static String messageKey(DashboardMessage message) {
        return message.getSourceType()
                + '\u0000'
                + message.getSessionId()
                + '\u0000'
                + message.getMessageId()
                + '\u0000'
                + message.getSequenceNo();
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static String normalizedSearch(String value) {
        String normalized = normalize(value);
        return normalized == null ? null : normalized.toLowerCase(Locale.ROOT);
    }

    private static boolean contains(String value, String lowerCaseSearch) {
        return value != null && value.toLowerCase(Locale.ROOT).contains(lowerCaseSearch);
    }

    private static long metadataSize(AttachmentMetadata metadata) {
        return metadata == null ? -1L : metadata.size;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() || !value.isValueNode() ? null : value.asText();
    }

    private static String valueOrDefault(String value, String defaultValue) {
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Dashboard data store is closed");
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        uploaded.close();
    }

    private static final class AttachmentMetadata {
        private final String mimeType;
        private final String fileName;
        private final String status;
        private final String sha256;
        private final long size;

        private AttachmentMetadata(
                String mimeType, String fileName, String status, String sha256, long size) {
            this.mimeType = mimeType;
            this.fileName = fileName;
            this.status = status;
            this.sha256 = sha256;
            this.size = size;
        }
    }
}
