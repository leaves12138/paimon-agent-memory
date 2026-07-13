package org.apache.paimon.agent.dashboard;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.agent.config.AgentConfiguration;
import org.apache.paimon.agent.sink.PaimonChatRepository;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/** Bounded, projection-aware dashboard reads over the Paimon chat tables. */
public final class PaimonDashboardDataStore implements DashboardDataStore {

    private static final int CONTENT_PREVIEW_LENGTH = 240;
    private static final int[] SESSION_COLUMNS =
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
    private static final int[] SESSION_OVERVIEW_COLUMNS = new int[] {0, 4, 8, 13};
    // Deliberately excludes column 7 (attachments / ARRAY<BLOB>).
    private static final int[] MESSAGE_LIST_COLUMNS = new int[] {0, 1, 2, 3, 4, 5, 6, 8, 9};
    private static final int[] MESSAGE_OVERVIEW_COLUMNS = new int[] {0, 1, 9};
    private static final int[] MESSAGE_DETAIL_COLUMNS =
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    private static final int[] MESSAGE_ATTACHMENT_COLUMNS = new int[] {0, 1, 2, 3, 6, 7};

    private final Table sessionsTable;
    private final Table messagesTable;
    private final Table descriptorMessagesTable;
    private final int maxScanRows;
    private final ObjectMapper objectMapper;
    private final PaimonChatRepository ownedRepository;

    private boolean closed;

    /** Uses an already initialized repository; the caller retains ownership of it. */
    public PaimonDashboardDataStore(PaimonChatRepository repository, int maxScanRows) {
        this(
                Objects.requireNonNull(repository, "repository").sessionsTableForRead(),
                repository.messagesTableForRead(),
                maxScanRows,
                null);
    }

    private PaimonDashboardDataStore(
            Table sessionsTable,
            Table messagesTable,
            int maxScanRows,
            PaimonChatRepository ownedRepository) {
        if (maxScanRows <= 0 || maxScanRows == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "maxScanRows must be between 1 and " + (Integer.MAX_VALUE - 1));
        }
        this.sessionsTable = Objects.requireNonNull(sessionsTable, "sessionsTable");
        this.messagesTable = Objects.requireNonNull(messagesTable, "messagesTable");
        this.descriptorMessagesTable =
                messagesTable.copy(
                        Collections.singletonMap(
                                CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true"));
        this.maxScanRows = maxScanRows;
        this.objectMapper = new ObjectMapper();
        this.ownedRepository = ownedRepository;
    }

    /** Opens an independent read-only repository and owns it until {@link #close()}. */
    public static PaimonDashboardDataStore open(
            AgentConfiguration configuration, int maxScanRows) throws Exception {
        if (maxScanRows <= 0 || maxScanRows == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "maxScanRows must be between 1 and " + (Integer.MAX_VALUE - 1));
        }
        PaimonChatRepository repository = new PaimonChatRepository(configuration);
        try {
            repository.initializeForRestore();
            return new PaimonDashboardDataStore(
                    repository.sessionsTableForRead(),
                    repository.messagesTableForRead(),
                    maxScanRows,
                    repository);
        } catch (Exception failure) {
            try {
                repository.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    @Override
    public DashboardOverview overview() throws Exception {
        ensureOpen();
        OverviewAccumulator result = new OverviewAccumulator();
        boolean sessionsTruncated =
                scan(
                        sessionsTable,
                        SESSION_OVERVIEW_COLUMNS,
                        null,
                        row -> {
                            String sourceType = requiredString(row, 0, "source_type");
                            result.sessionCount++;
                            result.sessionCountBySource.merge(sourceType, 1L, Long::sum);
                            if (!row.isNullAt(1) && row.getBoolean(1)) {
                                result.archivedSessionCount++;
                            } else {
                                result.activeSessionCount++;
                            }
                            if (!row.isNullAt(2)) {
                                result.pendingSessionCount++;
                            }
                            result.lastIngestedAt =
                                    latest(result.lastIngestedAt, nullableTimestamp(row, 3));
                        });
        boolean messagesTruncated =
                scan(
                        descriptorMessagesTable,
                        MESSAGE_OVERVIEW_COLUMNS,
                        null,
                        row -> {
                            String sourceType = requiredString(row, 1, "source_type");
                            result.messageCount++;
                            result.messageCountBySource.merge(sourceType, 1L, Long::sum);
                            result.lastIngestedAt =
                                    latest(result.lastIngestedAt, nullableTimestamp(row, 2));
                        });
        return new DashboardOverview(
                result.sessionCount,
                result.messageCount,
                result.activeSessionCount,
                result.archivedSessionCount,
                result.pendingSessionCount,
                result.sessionCountBySource,
                result.messageCountBySource,
                result.lastIngestedAt,
                sessionsTruncated,
                messagesTruncated);
    }

    @Override
    public DashboardPage<DashboardSession> listSessions(SessionQuery query) throws Exception {
        ensureOpen();
        Objects.requireNonNull(query, "query");
        validatePage(query.getPage(), query.getPageSize());
        String sourceType = normalize(query.getSourceType());
        String search = normalizedSearch(query.getSearch());
        Predicate filter = sessionPredicate(sourceType, query.getArchived());
        List<DashboardSession> matches = new ArrayList<>();
        boolean truncated =
                scan(
                        sessionsTable,
                        SESSION_COLUMNS,
                        filter,
                        row -> {
                            DashboardSession session = session(row);
                            if (!matchesSessionExact(
                                    session, sourceType, query.getArchived())) {
                                return;
                            }
                            if (search == null
                                    || containsIgnoreCase(session.getSessionId(), search)
                                    || containsIgnoreCase(session.getTitle(), search)
                                    || containsIgnoreCase(session.getCwd(), search)) {
                                matches.add(session);
                            }
                        });
        matches.sort(
                Comparator.comparing(
                                DashboardSession::getLastMessageAt,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(
                                DashboardSession::getUpdatedAt,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(DashboardSession::getSourceType)
                        .thenComparing(DashboardSession::getSessionId));
        return page(matches, query.getPage(), query.getPageSize(), truncated);
    }

    @Override
    public DashboardPage<DashboardMessage> listMessages(MessageQuery query) throws Exception {
        ensureOpen();
        Objects.requireNonNull(query, "query");
        validatePage(query.getPage(), query.getPageSize());
        MessageFilter values = MessageFilter.from(query);
        Predicate filter = messagePredicate(values, false, 0L, null);
        List<DashboardMessage> matches = new ArrayList<>();
        boolean truncated =
                scan(
                        descriptorMessagesTable,
                        MESSAGE_LIST_COLUMNS,
                        filter,
                        row -> {
                            DashboardMessage message = message(row);
                            if (!values.matches(message)) {
                                return;
                            }
                            String contentJson = requiredString(row, 6, "content_json");
                            if (values.search == null
                                    || containsIgnoreCase(
                                            message.getMessageId(), values.search)
                                    || containsIgnoreCase(contentJson, values.search)) {
                                matches.add(message);
                            }
                        });
        matches.sort(
                Comparator.comparing(
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
        return page(matches, query.getPage(), query.getPageSize(), truncated);
    }

    @Override
    public Optional<DashboardMessageDetail> messageDetail(
            String sourceType, String sessionId, String messageId, long sequenceNo)
            throws Exception {
        ensureOpen();
        MessageKey key = new MessageKey(sourceType, sessionId, messageId, sequenceNo);
        List<DashboardMessageDetail> matches = new ArrayList<>(1);
        boolean truncated =
                scan(
                        descriptorMessagesTable,
                        MESSAGE_DETAIL_COLUMNS,
                        messagePredicate(key.filter, true, sequenceNo, key.messageId),
                        row -> {
                            if (key.matches(row, 0, 1, 2, 3)) {
                                matches.add(detail(row));
                            }
                        });
        if (truncated) {
            throw new DashboardScanLimitExceededException(
                    descriptorMessagesTable.name(), maxScanRows);
        }
        return unique(matches, key);
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
        if (maxBytes <= 0 || maxBytes > Integer.MAX_VALUE - 8L) {
            throw new IllegalArgumentException(
                    "maxBytes must be between 1 and " + (Integer.MAX_VALUE - 8L));
        }
        MessageKey key = new MessageKey(sourceType, sessionId, messageId, sequenceNo);
        List<AttachmentData> matches = new ArrayList<>(1);
        int[] matchingRows = new int[1];
        boolean truncated =
                scan(
                        descriptorMessagesTable,
                        MESSAGE_ATTACHMENT_COLUMNS,
                        messagePredicate(key.filter, true, sequenceNo, key.messageId),
                        row -> {
                            if (!key.matches(row, 0, 1, 2, 3)) {
                                return;
                            }
                            matchingRows[0]++;
                            if (matchingRows[0] > 1) {
                                return;
                            }
                            if (row.isNullAt(5)) {
                                return;
                            }
                            InternalArray attachments = row.getArray(5);
                            if (index >= attachments.size()
                                    || attachments.isNullAt(index)) {
                                return;
                            }
                            Blob blob = attachments.getBlob(index);
                            long descriptorSize = descriptorLength(blob);
                            if (descriptorSize > maxBytes) {
                                throw new DashboardAttachmentTooLargeException(
                                        descriptorSize, maxBytes);
                            }
                            AttachmentMetadata metadata =
                                    attachmentMetadata(nullableString(row, 4)).get(index);
                            byte[] bytes = readBounded(blob, maxBytes);
                            matches.add(
                                    new AttachmentData(
                                            bytes,
                                            valueOrDefault(
                                                    metadata == null
                                                            ? null
                                                            : metadata.mimeType,
                                                    "application/octet-stream"),
                                            valueOrDefault(
                                                    metadata == null
                                                            ? null
                                                            : metadata.fileName,
                                                    "attachment-" + index)));
                        });
        if (truncated) {
            throw new DashboardScanLimitExceededException(
                    descriptorMessagesTable.name(), maxScanRows);
        }
        if (matchingRows[0] > 1) {
            throw duplicateMessage(key);
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
    }

    private DashboardMessageDetail detail(InternalRow row) {
        String contentJson = requiredString(row, 6, "content_json");
        Map<Integer, AttachmentMetadata> metadata = attachmentMetadata(contentJson);
        List<DashboardAttachment> attachments = new ArrayList<>();
        if (!row.isNullAt(7)) {
            InternalArray values = row.getArray(7);
            for (int index = 0; index < values.size(); index++) {
                AttachmentMetadata item = metadata.get(index);
                boolean present = !values.isNullAt(index);
                long size = present ? descriptorLength(values.getBlob(index)) : metadataSize(item);
                attachments.add(
                        new DashboardAttachment(
                                index,
                                present,
                                size,
                                item == null ? null : item.mimeType,
                                item == null ? null : item.fileName,
                                item == null ? (present ? "stored" : "missing") : item.status,
                                item == null ? null : item.sha256));
            }
        }
        for (Map.Entry<Integer, AttachmentMetadata> entry : metadata.entrySet()) {
            if (entry.getKey() >= attachments.size()) {
                AttachmentMetadata item = entry.getValue();
                attachments.add(
                        new DashboardAttachment(
                                entry.getKey(),
                                false,
                                metadataSize(item),
                                item.mimeType,
                                item.fileName,
                                item.status,
                                item.sha256));
            }
        }
        attachments.sort(Comparator.comparingInt(DashboardAttachment::getIndex));
        return new DashboardMessageDetail(
                requiredString(row, 0, "message_id"),
                requiredString(row, 1, "source_type"),
                requiredString(row, 2, "session_id"),
                row.isNullAt(3) ? 0L : row.getLong(3),
                nullableString(row, 4),
                nullableString(row, 5),
                contentJson,
                attachments,
                nullableTimestamp(row, 8),
                nullableTimestamp(row, 9));
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
                int index = item.path("index").isIntegralNumber() ? item.path("index").asInt() : ordinal;
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

    private Predicate sessionPredicate(String sourceType, Boolean archived) {
        PredicateBuilder builder = new PredicateBuilder(sessionsTable.rowType());
        List<Predicate> predicates = new ArrayList<>();
        if (sourceType != null) {
            predicates.add(
                    builder.equal(
                            builder.indexOf("source_type"), BinaryString.fromString(sourceType)));
        }
        if (archived != null) {
            predicates.add(builder.equal(builder.indexOf("archived"), archived));
        }
        return predicates.isEmpty() ? null : PredicateBuilder.and(predicates);
    }

    private Predicate messagePredicate(
            MessageFilter values, boolean includeSequence, long sequenceNo, String messageId) {
        PredicateBuilder builder = new PredicateBuilder(messagesTable.rowType());
        List<Predicate> predicates = new ArrayList<>();
        addStringPredicate(predicates, builder, "source_type", values.sourceType);
        addStringPredicate(predicates, builder, "session_id", values.sessionId);
        addStringPredicate(predicates, builder, "role", values.role);
        addStringPredicate(predicates, builder, "event_type", values.eventType);
        addStringPredicate(predicates, builder, "message_id", messageId);
        if (includeSequence) {
            predicates.add(builder.equal(builder.indexOf("sequence_no"), sequenceNo));
        }
        return predicates.isEmpty() ? null : PredicateBuilder.and(predicates);
    }

    private static void addStringPredicate(
            List<Predicate> result, PredicateBuilder builder, String field, String value) {
        if (value != null) {
            result.add(
                    builder.equal(builder.indexOf(field), BinaryString.fromString(value)));
        }
    }

    private boolean scan(Table table, int[] projection, Predicate filter, RowConsumer consumer)
            throws Exception {
        ReadBuilder builder =
                table.newReadBuilder().withProjection(projection).withLimit(maxScanRows + 1);
        if (filter != null) {
            builder.withFilter(filter);
        }
        int scanned = 0;
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        scanned++;
                        if (scanned > maxScanRows) {
                            return true;
                        }
                        consumer.accept(row);
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return false;
    }

    private static DashboardSession session(InternalRow row) {
        return new DashboardSession(
                requiredString(row, 0, "source_type"),
                requiredString(row, 1, "session_id"),
                nullableString(row, 2),
                nullableString(row, 3),
                !row.isNullAt(4) && row.getBoolean(4),
                nullableString(row, 5),
                nullableString(row, 6),
                row.isNullAt(7) ? 0L : row.getLong(7),
                row.isNullAt(8) ? null : row.getLong(8),
                nullableString(row, 9),
                nullableTimestamp(row, 10),
                nullableTimestamp(row, 11),
                nullableTimestamp(row, 12),
                nullableTimestamp(row, 13));
    }

    private static DashboardMessage message(InternalRow row) {
        String contentJson = requiredString(row, 6, "content_json");
        return new DashboardMessage(
                requiredString(row, 0, "message_id"),
                requiredString(row, 1, "source_type"),
                requiredString(row, 2, "session_id"),
                row.isNullAt(3) ? 0L : row.getLong(3),
                nullableString(row, 4),
                nullableString(row, 5),
                preview(contentJson),
                contentJson.length(),
                nullableTimestamp(row, 7),
                nullableTimestamp(row, 8));
    }

    private static String preview(String contentJson) {
        String compact = contentJson.replaceAll("\\s+", " ").trim();
        return compact.length() <= CONTENT_PREVIEW_LENGTH
                ? compact
                : compact.substring(0, CONTENT_PREVIEW_LENGTH) + "\u2026";
    }

    private static boolean matchesSessionExact(
            DashboardSession session, String sourceType, Boolean archived) {
        return (sourceType == null || sourceType.equals(session.getSourceType()))
                && (archived == null || archived == session.isArchived());
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
        if (pageSize <= 0 || pageSize > maxScanRows) {
            throw new IllegalArgumentException(
                    "pageSize must be between 1 and maxScanRows=" + maxScanRows);
        }
    }

    private static <T> Optional<T> unique(List<T> matches, MessageKey key) {
        if (matches.size() > 1) {
            throw duplicateMessage(key);
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
    }

    private static IllegalStateException duplicateMessage(MessageKey key) {
        return new IllegalStateException(
                "More than one ai_chat_messages row matched "
                        + key.sourceType
                        + "/"
                        + key.sessionId
                        + "/"
                        + key.messageId
                        + "/"
                        + key.sequenceNo);
    }

    private static long descriptorLength(Blob blob) {
        BlobDescriptor descriptor = blob.toDescriptor();
        if (descriptor.length() < 0) {
            throw new IllegalStateException("Paimon returned a negative BLOB length");
        }
        return descriptor.length();
    }

    private static byte[] readBounded(Blob blob, long maxBytes) throws IOException {
        try (InputStream input = blob.newInputStream();
                ByteArrayOutputStream output =
                        new ByteArrayOutputStream((int) Math.min(maxBytes, 8192L))) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                long next = (long) output.size() + read;
                if (next > maxBytes) {
                    throw new DashboardAttachmentTooLargeException(next, maxBytes);
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }

    private static long metadataSize(AttachmentMetadata metadata) {
        return metadata == null ? -1L : metadata.size;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() || !value.isValueNode() ? null : value.asText();
    }

    private static String requiredString(InternalRow row, int position, String field) {
        if (row.isNullAt(position)) {
            throw new IllegalStateException("Paimon row has null required field " + field);
        }
        return row.getString(position).toString();
    }

    private static String nullableString(InternalRow row, int position) {
        return row.isNullAt(position) ? null : row.getString(position).toString();
    }

    private static Instant nullableTimestamp(InternalRow row, int position) {
        return row.isNullAt(position) ? null : row.getTimestamp(position, 3).toInstant();
    }

    private static Instant latest(Instant first, Instant second) {
        if (first == null) {
            return second;
        }
        return second != null && second.isAfter(first) ? second : first;
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

    private static boolean containsIgnoreCase(String value, String lowerCaseSearch) {
        return value != null && value.toLowerCase(Locale.ROOT).contains(lowerCaseSearch);
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
        if (ownedRepository != null) {
            ownedRepository.close();
        }
    }

    @FunctionalInterface
    private interface RowConsumer {
        void accept(InternalRow row) throws Exception;
    }

    private static final class OverviewAccumulator {
        private long sessionCount;
        private long messageCount;
        private long activeSessionCount;
        private long archivedSessionCount;
        private long pendingSessionCount;
        private final Map<String, Long> sessionCountBySource = new TreeMap<>();
        private final Map<String, Long> messageCountBySource = new TreeMap<>();
        private Instant lastIngestedAt;
    }

    private static final class AttachmentMetadata {
        private final String mimeType;
        private final String fileName;
        private final String status;
        private final String sha256;
        private final long size;

        private AttachmentMetadata(
                String mimeType,
                String fileName,
                String status,
                String sha256,
                long size) {
            this.mimeType = mimeType;
            this.fileName = fileName;
            this.status = status;
            this.sha256 = sha256;
            this.size = size;
        }
    }

    private static final class MessageFilter {
        private final String sourceType;
        private final String sessionId;
        private final String role;
        private final String eventType;
        private final String search;

        private MessageFilter(
                String sourceType,
                String sessionId,
                String role,
                String eventType,
                String search) {
            this.sourceType = sourceType;
            this.sessionId = sessionId;
            this.role = role;
            this.eventType = eventType;
            this.search = search;
        }

        private static MessageFilter from(MessageQuery query) {
            return new MessageFilter(
                    normalize(query.getSourceType()),
                    normalize(query.getSessionId()),
                    normalize(query.getRole()),
                    normalize(query.getEventType()),
                    normalizedSearch(query.getSearch()));
        }

        private boolean matches(DashboardMessage message) {
            return (sourceType == null || sourceType.equals(message.getSourceType()))
                    && (sessionId == null || sessionId.equals(message.getSessionId()))
                    && (role == null || role.equals(message.getRole()))
                    && (eventType == null || eventType.equals(message.getEventType()));
        }
    }

    private static final class MessageKey {
        private final String sourceType;
        private final String sessionId;
        private final String messageId;
        private final long sequenceNo;
        private final MessageFilter filter;

        private MessageKey(
                String sourceType, String sessionId, String messageId, long sequenceNo) {
            this.sourceType = requiredKey(sourceType, "sourceType");
            this.sessionId = requiredKey(sessionId, "sessionId");
            this.messageId = requiredKey(messageId, "messageId");
            this.sequenceNo = sequenceNo;
            this.filter = new MessageFilter(this.sourceType, this.sessionId, null, null, null);
        }

        private boolean matches(
                InternalRow row,
                int messagePosition,
                int sourcePosition,
                int sessionPosition,
                int sequencePosition) {
            return messageId.equals(requiredString(row, messagePosition, "message_id"))
                    && sourceType.equals(requiredString(row, sourcePosition, "source_type"))
                    && sessionId.equals(requiredString(row, sessionPosition, "session_id"))
                    && !row.isNullAt(sequencePosition)
                    && sequenceNo == row.getLong(sequencePosition);
        }

        private static String requiredKey(String value, String name) {
            String normalized = normalize(value);
            if (normalized == null) {
                throw new IllegalArgumentException(name + " must not be empty");
            }
            return normalized;
        }
    }
}
