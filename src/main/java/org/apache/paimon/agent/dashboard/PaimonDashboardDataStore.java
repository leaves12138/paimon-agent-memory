package org.apache.paimon.agent.dashboard;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.agent.config.AgentConfiguration;
import org.apache.paimon.agent.sink.PaimonChatRepository;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.RangeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Bounded, projection-aware dashboard reads over the Paimon chat tables. */
public final class PaimonDashboardDataStore implements DashboardDataStore {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDashboardDataStore.class);
    private static final Duration QUERY_CACHE_TTL = Duration.ofMinutes(5);
    private static final int MESSAGE_SCAN_PARALLELISM = 4;
    private static final String MESSAGE_SPLIT_TARGET_SIZE = "32mb";
    private static final long MESSAGE_SCAN_SHUTDOWN_TIMEOUT_MILLIS = 5_000L;
    private static final int SESSION_CACHE_ENTRIES = 16;
    private static final long SESSION_CACHE_ROWS = 20_000L;
    private static final int MESSAGE_CACHE_ENTRIES = 24;
    private static final long MESSAGE_CACHE_ROWS = 75_000L;
    private static final long MESSAGE_CACHE_PREVIEW_CHARS = 16L * 1024L * 1024L;

    private static final int[] SESSION_COLUMNS =
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
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
    private final AutoCloseable ownedRepository;
    private final DashboardQueryCache<Boolean, DashboardOverview> overviewCache;
    private final DashboardQueryCache<SessionCacheKey, CachedRows<DashboardSession>> sessionCache;
    private final DashboardQueryCache<MessageCacheKey, CachedRows<DashboardMessage>> messageCache;
    private final ExecutorService messageScanExecutor;
    private final long messageScanShutdownTimeoutMillis;
    private final Object closeLifecycleLock = new Object();

    private volatile boolean closed;
    private boolean resourcesClosed;

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
        this(
                sessionsTable,
                messagesTable,
                maxScanRows,
                ownedRepository,
                Executors.newFixedThreadPool(
                        MESSAGE_SCAN_PARALLELISM + 1, new MessageScanThreadFactory()),
                MESSAGE_SCAN_SHUTDOWN_TIMEOUT_MILLIS);
    }

    PaimonDashboardDataStore(
            Table sessionsTable,
            Table messagesTable,
            int maxScanRows,
            AutoCloseable ownedRepository,
            ExecutorService messageScanExecutor,
            long messageScanShutdownTimeoutMillis) {
        if (maxScanRows <= 0 || maxScanRows == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "maxScanRows must be between 1 and " + (Integer.MAX_VALUE - 1));
        }
        if (messageScanShutdownTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("messageScanShutdownTimeoutMillis must be positive");
        }
        this.sessionsTable = Objects.requireNonNull(sessionsTable, "sessionsTable");
        this.messagesTable = Objects.requireNonNull(messagesTable, "messagesTable");
        Map<String, String> descriptorOptions = new HashMap<>();
        descriptorOptions.put(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true");
        descriptorOptions.put(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "full");
        // Global-index results are IndexedSplits. A smaller read-only split target keeps a session
        // spread across many historical append files from being read serially as one 128 MiB split.
        descriptorOptions.put(
                CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), MESSAGE_SPLIT_TARGET_SIZE);
        this.descriptorMessagesTable = messagesTable.copy(descriptorOptions);
        this.maxScanRows = maxScanRows;
        this.objectMapper = new ObjectMapper();
        this.ownedRepository = ownedRepository;
        this.overviewCache =
                new DashboardQueryCache<>(QUERY_CACHE_TTL, 1, 1L, ignored -> 1L);
        this.sessionCache =
                new DashboardQueryCache<>(
                        QUERY_CACHE_TTL,
                        SESSION_CACHE_ENTRIES,
                        SESSION_CACHE_ROWS,
                        CachedRows::weight);
        this.messageCache =
                new DashboardQueryCache<>(
                        QUERY_CACHE_TTL,
                        MESSAGE_CACHE_ENTRIES,
                        MESSAGE_CACHE_ROWS,
                        CachedRows::weight,
                        MESSAGE_CACHE_PREVIEW_CHARS,
                        PaimonDashboardDataStore::messagePreviewCharacters);
        this.messageScanExecutor =
                Objects.requireNonNull(messageScanExecutor, "messageScanExecutor");
        this.messageScanShutdownTimeoutMillis = messageScanShutdownTimeoutMillis;
        // Keep one executor slot outside the per-query read parallelism so startup warming cannot
        // make the first user query run with only three readers.
        this.messageScanExecutor.submit(this::warmMessageManifests);
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
        return overviewCache.get(Boolean.TRUE, this::loadOverview);
    }

    private DashboardOverview loadOverview() throws Exception {
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
        SessionCacheKey cacheKey = new SessionCacheKey(sourceType, search, query.getArchived());
        CachedRows<DashboardSession> cached =
                sessionCache.get(
                        cacheKey,
                        () -> loadSessions(sourceType, search, query.getArchived()));
        return page(
                cached.rows,
                query.getPage(),
                query.getPageSize(),
                cached.truncated);
    }

    private CachedRows<DashboardSession> loadSessions(
            String sourceType, String search, Boolean archived) throws Exception {
        Predicate filter = sessionPredicate(sourceType, archived);
        List<DashboardSession> matches = new ArrayList<>();
        boolean truncated =
                scan(
                        sessionsTable,
                        SESSION_COLUMNS,
                        filter,
                        row -> {
                            DashboardSession session = session(row);
                            if (!matchesSessionExact(session, sourceType, archived)) {
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
        return new CachedRows<>(matches, truncated);
    }

    @Override
    public DashboardPage<DashboardMessage> listMessages(MessageQuery query) throws Exception {
        ensureOpen();
        Objects.requireNonNull(query, "query");
        validatePage(query.getPage(), query.getPageSize());
        MessageFilter values = MessageFilter.from(query);
        MessageCacheKey cacheKey = new MessageCacheKey(values);
        CachedRows<DashboardMessage> cached =
                messageCache.get(cacheKey, () -> loadMessages(values));
        return page(
                cached.rows,
                query.getPage(),
                query.getPageSize(),
                cached.truncated);
    }

    private CachedRows<DashboardMessage> loadMessages(MessageFilter values) throws Exception {
        Predicate filter = messagePredicate(values, false, 0L, null);
        List<DashboardMessage> matches = new ArrayList<>();
        boolean truncated =
                scan(
                        descriptorMessagesTable,
                        MESSAGE_LIST_COLUMNS,
                        messagePlanningPredicate(values, false, 0L, null),
                        filter,
                        row -> {
                            String contentJson = requiredString(row, 6, "content_json");
                            DashboardMessage message =
                                    message(row, values.conversationOnly);
                            if (!values.matches(message)
                                    || (values.conversationOnly
                                            && message.getContentPreview().isEmpty())) {
                                return;
                            }
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
        return new CachedRows<>(matches, truncated);
    }

    @Override
    public Optional<DashboardMessageDetail> messageDetail(
            String sourceType, String sessionId, String messageId, long sequenceNo)
            throws Exception {
        ensureOpen();
        MessageKey key = new MessageKey(sourceType, sessionId, messageId, sequenceNo);
        List<DashboardMessageDetail> matches = new ArrayList<>(1);
        Predicate filter =
                messagePredicate(key.filter, true, sequenceNo, key.messageId);
        boolean truncated =
                scan(
                        descriptorMessagesTable,
                        MESSAGE_DETAIL_COLUMNS,
                        messagePlanningPredicate(
                                key.filter, true, sequenceNo, key.messageId),
                        filter,
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
        Predicate filter =
                messagePredicate(key.filter, true, sequenceNo, key.messageId);
        boolean truncated =
                scan(
                        descriptorMessagesTable,
                        MESSAGE_ATTACHMENT_COLUMNS,
                        messagePlanningPredicate(
                                key.filter, true, sequenceNo, key.messageId),
                        filter,
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
        predicates.add(builder.isNull(builder.indexOf("subagent_source_json")));
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
        if (values.conversationOnly) {
            predicates.add(conversationalMessagePredicate(builder));
        }
        addStringPredicate(predicates, builder, "message_id", messageId);
        if (includeSequence) {
            predicates.add(builder.equal(builder.indexOf("sequence_no"), sequenceNo));
        }
        return predicates.isEmpty() ? null : PredicateBuilder.and(predicates);
    }

    private static Predicate conversationalMessagePredicate(PredicateBuilder builder) {
        int role = builder.indexOf("role");
        int eventType = builder.indexOf("event_type");
        Predicate userOrAssistant =
                PredicateBuilder.or(
                        builder.equal(role, BinaryString.fromString("user")),
                        builder.equal(role, BinaryString.fromString("assistant")));
        Predicate standardEvent =
                PredicateBuilder.or(
                        builder.isNull(eventType),
                        builder.equal(eventType, BinaryString.fromString("")),
                        builder.equal(eventType, BinaryString.fromString("message")),
                        builder.equal(eventType, BinaryString.fromString("user")),
                        builder.equal(eventType, BinaryString.fromString("assistant")));
        Predicate standardMessage = PredicateBuilder.and(userOrAssistant, standardEvent);
        Predicate generatedImage =
                PredicateBuilder.and(
                        builder.equal(role, BinaryString.fromString("assistant")),
                        builder.equal(
                                eventType,
                                BinaryString.fromString("image_generation_end")));
        Predicate attachmentRole =
                PredicateBuilder.and(
                        builder.equal(role, BinaryString.fromString("attachment")),
                        PredicateBuilder.or(
                                builder.isNull(eventType),
                                builder.equal(eventType, BinaryString.fromString("")),
                                builder.equal(
                                        eventType,
                                        BinaryString.fromString("attachment"))));
        Predicate allowedAttachmentRole =
                PredicateBuilder.or(
                        builder.isNull(role),
                        PredicateBuilder.and(
                                builder.notEqual(
                                        role, BinaryString.fromString("system")),
                                builder.notEqual(
                                        role, BinaryString.fromString("developer")),
                                builder.notEqual(
                                        role, BinaryString.fromString("tool"))));
        Predicate attachmentEvent =
                PredicateBuilder.and(
                        builder.equal(
                                eventType, BinaryString.fromString("attachment")),
                        allowedAttachmentRole);
        return PredicateBuilder.or(
                standardMessage, generatedImage, attachmentRole, attachmentEvent);
    }

    private Predicate messagePlanningPredicate(
            MessageFilter values, boolean includeSequence, long sequenceNo, String messageId) {
        if (values.sessionId == null) {
            return messagePredicate(values, includeSequence, sequenceNo, messageId);
        }

        // Only session_id has a global index. In FULL search mode, including any unindexed field
        // in this compound predicate makes GlobalIndexCoverage add that field's uncovered ranges,
        // which broadens the indexed result back toward a full-table scan. Plan with session_id
        // alone, then execute the complete predicate in the reader and retain the callers' existing
        // values.matches / key.matches checks for source isolation and exactness.
        PredicateBuilder builder = new PredicateBuilder(messagesTable.rowType());
        return builder.equal(
                builder.indexOf("session_id"), BinaryString.fromString(values.sessionId));
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
        return scan(table, projection, filter, filter, consumer);
    }

    private boolean scan(
            Table table,
            int[] projection,
            Predicate planningFilter,
            Predicate readFilter,
            RowConsumer consumer)
            throws Exception {
        ReadBuilder planningBuilder = readBuilder(table, projection, planningFilter);
        TableScan.Plan plan = planningBuilder.newScan().plan();
        List<Split> splits =
                table == descriptorMessagesTable && planningFilter != null
                        ? independentMessageSplits(plan.splits())
                        : plan.splits();
        if (splits.size() <= 1) {
            try (RecordReader<InternalRow> reader =
                    readBuilder(table, projection, readFilter)
                            .newRead()
                            .executeFilter()
                            .createReader(splits)) {
                return scanReader(reader, consumer);
            }
        }

        return scanParallel(table, projection, readFilter, splits, consumer);
    }

    private ReadBuilder readBuilder(Table table, int[] projection, Predicate filter) {
        ReadBuilder builder =
                table.newReadBuilder().withProjection(projection).withLimit(maxScanRows + 1);
        if (filter != null) {
            builder.withFilter(filter);
        }
        return builder;
    }

    private void warmMessageManifests() {
        try {
            descriptorMessagesTable
                    .newReadBuilder()
                    .withProjection(MESSAGE_OVERVIEW_COLUMNS)
                    .newScan()
                    .plan();
        } catch (Throwable failure) {
            if (!closed) {
                LOG.debug("Unable to warm dashboard message manifests", failure);
            }
        }
    }

    private boolean scanReader(RecordReader<InternalRow> reader, RowConsumer consumer)
            throws Exception {
        int scanned = 0;
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
        return false;
    }

    private boolean scanParallel(
            Table table,
            int[] projection,
            Predicate filter,
            List<Split> splits,
            RowConsumer consumer)
            throws Exception {
        ParallelScanState state = new ParallelScanState();
        Object consumerLock = new Object();
        CountDownLatch taskCompletion = new CountDownLatch(splits.size());
        List<Future<Void>> futures = new ArrayList<>(splits.size());
        for (int index = 0; index < splits.size(); index++) {
            Split split = splits.get(index);
            TrackedScanTask task =
                    new TrackedScanTask(
                            () -> {
                                try {
                                    TableRead read =
                                            readBuilder(table, projection, filter)
                                                    .newRead()
                                                    .executeFilter();
                                    try (RecordReader<InternalRow> reader =
                                            read.createReader(split)) {
                                        scanParallelReader(
                                                reader, state, consumerLock, consumer);
                                    }
                                } catch (Exception | Error failure) {
                                    state.stop.set(true);
                                    throw failure;
                                }
                                return null;
                            },
                            taskCompletion);
            futures.add(task);
            try {
                messageScanExecutor.execute(task);
            } catch (RuntimeException | Error submissionFailure) {
                // execute() rejected this task, so neither run() nor its completion signal is
                // guaranteed. Account for it and the tasks which were never constructed.
                task.cancel(false);
                for (int skipped = index + 1; skipped < splits.size(); skipped++) {
                    taskCompletion.countDown();
                }
                cancelParallelScan(state, futures, taskCompletion, submissionFailure);
                rethrow(submissionFailure);
                throw new AssertionError("Unreachable after rethrow");
            }
        }
        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException interrupted) {
            cancelParallelScan(state, futures, taskCompletion, interrupted);
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause();
            cancelParallelScan(state, futures, taskCompletion, cause);
            rethrow(cause);
        } catch (CancellationException cancelled) {
            IllegalStateException failure =
                    new IllegalStateException("Parallel dashboard scan was cancelled", cancelled);
            cancelParallelScan(state, futures, taskCompletion, failure);
            throw failure;
        }
        return state.truncated.get();
    }

    private void cancelParallelScan(
            ParallelScanState state,
            List<? extends Future<?>> futures,
            CountDownLatch taskCompletion,
            Throwable primaryFailure) {
        state.stop.set(true);
        try {
            cancelAndAwaitParallelTasks(
                    futures, taskCompletion, messageScanShutdownTimeoutMillis);
        } catch (InterruptedException | TimeoutException cleanupFailure) {
            primaryFailure.addSuppressed(cleanupFailure);
        }
    }

    private void scanParallelReader(
            RecordReader<InternalRow> reader,
            ParallelScanState state,
            Object consumerLock,
            RowConsumer consumer)
            throws Exception {
        RecordReader.RecordIterator<InternalRow> batch;
        while (!state.stop.get() && (batch = reader.readBatch()) != null) {
            try {
                InternalRow row;
                while (!state.stop.get() && (row = batch.next()) != null) {
                    int position = state.scanned.incrementAndGet();
                    if (position > maxScanRows) {
                        state.truncated.set(true);
                        state.stop.set(true);
                        break;
                    }
                    synchronized (consumerLock) {
                        consumer.accept(row);
                    }
                }
            } catch (Exception | Error failure) {
                state.stop.set(true);
                throw failure;
            } finally {
                batch.releaseBatch();
            }
        }
    }

    static List<Split> independentMessageSplits(List<Split> planned) {
        List<Split> result = new ArrayList<>();
        for (Split split : planned) {
            if (!(split instanceof DataSplit)) {
                result.add(split);
                continue;
            }
            DataSplit dataSplit = (DataSplit) split;
            List<List<DataFileMeta>> groups;
            try {
                groups =
                        new RangeHelper<DataFileMeta>(DataFileMeta::nonNullRowIdRange)
                                .mergeOverlappingRanges(dataSplit.dataFiles());
            } catch (RuntimeException incompatibleSplit) {
                result.add(split);
                continue;
            }
            if (groups.size() <= 1) {
                result.add(split);
                continue;
            }

            int parallelism = Math.min(MESSAGE_SCAN_PARALLELISM, groups.size());
            List<List<DataFileMeta>> bins = new ArrayList<>(parallelism);
            long[] binWeights = new long[parallelism];
            for (int index = 0; index < parallelism; index++) {
                bins.add(new ArrayList<>());
            }
            for (List<DataFileMeta> group : groups) {
                int target = lightest(binWeights);
                bins.get(target).addAll(group);
                for (DataFileMeta file : group) {
                    binWeights[target] += Math.max(1L, file.fileSize());
                }
            }
            for (List<DataFileMeta> files : bins) {
                Set<DataFileMeta> selected = new HashSet<>(files);
                result.add(
                        dataSplit
                                .filterDataFile(selected::contains)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "Unable to split Paimon data-evolution scan")));
            }
        }
        return result;
    }

    private static int lightest(long[] weights) {
        int result = 0;
        for (int index = 1; index < weights.length; index++) {
            if (weights[index] < weights[result]) {
                result = index;
            }
        }
        return result;
    }

    static void cancelAndAwaitParallelTasks(
            List<? extends Future<?>> futures,
            CountDownLatch taskCompletion,
            long timeoutMillis)
            throws InterruptedException, TimeoutException {
        if (timeoutMillis <= 0L) {
            throw new IllegalArgumentException("timeoutMillis must be positive");
        }
        for (Future<?> future : futures) {
            future.cancel(true);
        }

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        InterruptedException interrupted = null;
        boolean completed = taskCompletion.getCount() == 0L;
        while (!completed) {
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0L) {
                break;
            }
            try {
                completed = taskCompletion.await(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException failure) {
                if (interrupted == null) {
                    interrupted = failure;
                } else {
                    interrupted.addSuppressed(failure);
                }
            }
        }

        if (interrupted != null) {
            Thread.currentThread().interrupt();
        }
        if (!completed) {
            TimeoutException timeout =
                    new TimeoutException(
                            "Parallel dashboard reader tasks did not terminate after cancellation");
            if (interrupted != null) {
                timeout.addSuppressed(interrupted);
            }
            throw timeout;
        }
        if (interrupted != null) {
            throw interrupted;
        }
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
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
                nullableTimestamp(row, 13),
                nullableString(row, 14),
                row.isNullAt(8)
                        ? DashboardStorageStatus.UPLOADED
                        : DashboardStorageStatus.PENDING);
    }

    private static DashboardMessage message(InternalRow row, boolean conversationOnly) {
        String contentJson = requiredString(row, 6, "content_json");
        String role = nullableString(row, 4);
        String eventType = nullableString(row, 5);
        String contentPreview =
                conversationOnly
                        ? DashboardContentPreview.conversationPreviewMessage(
                                contentJson, role, eventType)
                        : DashboardContentPreview.previewMessage(
                                contentJson, role, eventType);
        return new DashboardMessage(
                requiredString(row, 0, "message_id"),
                requiredString(row, 1, "source_type"),
                requiredString(row, 2, "session_id"),
                row.isNullAt(3) ? 0L : row.getLong(3),
                role,
                eventType,
                contentPreview,
                contentJson.length(),
                nullableTimestamp(row, 7),
                nullableTimestamp(row, 8));
    }

    private static boolean matchesSessionExact(
            DashboardSession session, String sourceType, Boolean archived) {
        return (sourceType == null || sourceType.equals(session.getSourceType()))
                && (archived == null || archived == session.isArchived())
                && session.getSubagentSourceJson() == null;
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
    public void invalidate() {
        ensureOpen();
        overviewCache.clear();
        sessionCache.clear();
        messageCache.clear();
    }

    @Override
    public void close() throws Exception {
        synchronized (closeLifecycleLock) {
            if (resourcesClosed) {
                return;
            }
            closed = true;
            overviewCache.clear();
            sessionCache.clear();
            messageCache.clear();

            ExecutorShutdown shutdown = shutdownMessageScanExecutor();
            Exception failure = null;
            if (!shutdown.terminated) {
                failure =
                        new IllegalStateException(
                                "Dashboard message reader tasks did not terminate; the owned "
                                        + "repository remains open to avoid a concurrent close");
            } else {
                try {
                    if (ownedRepository != null) {
                        ownedRepository.close();
                    }
                } catch (Exception closeFailure) {
                    failure = closeFailure;
                } finally {
                    resourcesClosed = true;
                }
            }

            if (shutdown.interrupted != null) {
                Thread.currentThread().interrupt();
                if (failure == null) {
                    failure = shutdown.interrupted;
                } else {
                    failure.addSuppressed(shutdown.interrupted);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private ExecutorShutdown shutdownMessageScanExecutor() {
        messageScanExecutor.shutdown();
        InterruptedException interrupted = null;
        boolean terminated = false;
        try {
            terminated =
                    messageScanExecutor.awaitTermination(
                            messageScanShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) {
            interrupted = failure;
        }

        if (!terminated) {
            List<Runnable> neverStarted = messageScanExecutor.shutdownNow();
            // ThreadPoolExecutor returns queued FutureTasks from shutdownNow without cancelling
            // them. Mark them cancelled so any request waiting in Future.get() is released and a
            // TrackedScanTask can signal that it will never start.
            for (Runnable task : neverStarted) {
                if (task instanceof Future<?>) {
                    ((Future<?>) task).cancel(false);
                }
            }
            try {
                terminated =
                        messageScanExecutor.awaitTermination(
                                messageScanShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException failure) {
                if (interrupted == null) {
                    interrupted = failure;
                } else {
                    interrupted.addSuppressed(failure);
                }
            }
        }

        if (!terminated) {
            LOG.error(
                    "Dashboard message reader tasks did not terminate after cancellation; "
                            + "repository resources remain open");
        }
        return new ExecutorShutdown(terminated, interrupted);
    }

    @FunctionalInterface
    private interface RowConsumer {
        void accept(InternalRow row) throws Exception;
    }

    private static final class CachedRows<T> {
        private final List<T> rows;
        private final boolean truncated;

        private CachedRows(List<T> rows, boolean truncated) {
            this.rows = Collections.unmodifiableList(new ArrayList<>(rows));
            this.truncated = truncated;
        }

        private long weight() {
            return Math.max(1L, rows.size());
        }
    }

    private static long messagePreviewCharacters(CachedRows<DashboardMessage> cached) {
        long characters = 0L;
        for (DashboardMessage message : cached.rows) {
            String preview = message.getContentPreview();
            if (preview != null) {
                characters += preview.length();
            }
        }
        return characters;
    }

    private static final class SessionCacheKey {
        private final String sourceType;
        private final String search;
        private final Boolean archived;

        private SessionCacheKey(String sourceType, String search, Boolean archived) {
            this.sourceType = sourceType;
            this.search = search;
            this.archived = archived;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof SessionCacheKey)) {
                return false;
            }
            SessionCacheKey that = (SessionCacheKey) other;
            return Objects.equals(sourceType, that.sourceType)
                    && Objects.equals(search, that.search)
                    && Objects.equals(archived, that.archived);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceType, search, archived);
        }
    }

    private static final class MessageCacheKey {
        private final String sourceType;
        private final String sessionId;
        private final String role;
        private final String eventType;
        private final String search;
        private final boolean conversationOnly;

        private MessageCacheKey(MessageFilter filter) {
            this.sourceType = filter.sourceType;
            this.sessionId = filter.sessionId;
            this.role = filter.role;
            this.eventType = filter.eventType;
            this.search = filter.search;
            this.conversationOnly = filter.conversationOnly;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof MessageCacheKey)) {
                return false;
            }
            MessageCacheKey that = (MessageCacheKey) other;
            return Objects.equals(sourceType, that.sourceType)
                    && Objects.equals(sessionId, that.sessionId)
                    && Objects.equals(role, that.role)
                    && Objects.equals(eventType, that.eventType)
                    && Objects.equals(search, that.search)
                    && conversationOnly == that.conversationOnly;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    sourceType, sessionId, role, eventType, search, conversationOnly);
        }
    }

    private static final class ParallelScanState {
        private final AtomicInteger scanned = new AtomicInteger();
        private final AtomicBoolean truncated = new AtomicBoolean();
        private final AtomicBoolean stop = new AtomicBoolean();
    }

    /**
     * A Future whose completion latch represents actual runner exit, not Future cancellation.
     * FutureTask transitions to cancelled before an interrupt-responsive callable has necessarily
     * returned, which is too early for repository lifecycle decisions.
     */
    private static final class TrackedScanTask extends FutureTask<Void> {
        private final CountDownLatch completion;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean completionSignalled = new AtomicBoolean();

        private TrackedScanTask(Callable<Void> callable, CountDownLatch completion) {
            super(callable);
            this.completion = completion;
        }

        @Override
        public void run() {
            started.set(true);
            try {
                super.run();
            } finally {
                signalCompletion();
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled && !started.get()) {
                signalCompletion();
            }
            return cancelled;
        }

        private void signalCompletion() {
            if (completionSignalled.compareAndSet(false, true)) {
                completion.countDown();
            }
        }
    }

    private static final class ExecutorShutdown {
        private final boolean terminated;
        private final InterruptedException interrupted;

        private ExecutorShutdown(boolean terminated, InterruptedException interrupted) {
            this.terminated = terminated;
            this.interrupted = interrupted;
        }
    }

    private static final class MessageScanThreadFactory implements ThreadFactory {
        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread =
                    new Thread(
                            runnable,
                            "paimon-agent-dashboard-message-read-"
                                    + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
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
        private final boolean conversationOnly;

        private MessageFilter(
                String sourceType,
                String sessionId,
                String role,
                String eventType,
                String search,
                boolean conversationOnly) {
            this.sourceType = sourceType;
            this.sessionId = sessionId;
            this.role = role;
            this.eventType = eventType;
            this.search = search;
            this.conversationOnly = conversationOnly;
        }

        private static MessageFilter from(MessageQuery query) {
            return new MessageFilter(
                    normalize(query.getSourceType()),
                    normalize(query.getSessionId()),
                    normalize(query.getRole()),
                    normalize(query.getEventType()),
                    normalizedSearch(query.getSearch()),
                    query.isConversationOnly());
        }

        private boolean matches(DashboardMessage message) {
            return (sourceType == null || sourceType.equals(message.getSourceType()))
                    && (sessionId == null || sessionId.equals(message.getSessionId()))
                    && (role == null || role.equals(message.getRole()))
                    && (eventType == null || eventType.equals(message.getEventType()))
                    && (!conversationOnly
                            || MessageQuery.isConversationalMessage(
                                    message.getRole(), message.getEventType()));
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
            this.filter =
                    new MessageFilter(
                            this.sourceType, this.sessionId, null, null, null, false);
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
