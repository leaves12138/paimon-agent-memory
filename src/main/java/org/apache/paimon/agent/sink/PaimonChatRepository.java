package org.apache.paimon.agent.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.agent.config.AgentConfiguration;
import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Paimon implementation of the two-table chat storage design. */
public final class PaimonChatRepository implements ChatRepository {

    private static final String WRITER_ID_OPTION = "paimon-agent.writer-id";
    private static final String MORAX_BTREE_INDEX_ENABLED_OPTION =
            "morax.btree-index.enabled";
    private static final String MORAX_BTREE_INDEX_COLUMN_OPTION =
            "global-index.btree.index-column";
    private static final String MORAX_BTREE_INDEX_COLUMN = "session_id";
    private static final String SUBAGENT_SOURCE_JSON_COLUMN = "subagent_source_json";
    private static final String PROJECTLESS_COLUMN = "projectless";

    private final AgentConfiguration configuration;
    private final ProjectConfig project;
    private Catalog catalog;
    private Table sessionsTable;
    private Table messagesTable;
    private StreamWriteBuilder sessionsBuilder;
    private StreamTableCommit sessionsCommit;
    private StreamWriteBuilder messagesBuilder;
    private StreamTableCommit messagesCommit;
    public PaimonChatRepository(AgentConfiguration configuration) {
        this.configuration = configuration;
        this.project = configuration.project();
    }

    @Override
    public void initialize() throws Exception {
        initializeSafely(true);
    }

    /** Opens existing tables without claiming writer ownership or creating missing objects. */
    public void initializeForRestore() throws Exception {
        initializeSafely(false);
    }

    /** Returns the initialized sessions table for read-only services such as the dashboard. */
    public Table sessionsTableForRead() {
        ensureInitialized();
        return sessionsTable;
    }

    /** Returns the initialized messages table for read-only services such as the dashboard. */
    public Table messagesTableForRead() {
        ensureInitialized();
        return messagesTable;
    }

    private void initializeSafely(boolean writer) throws Exception {
        try {
            initializeInternal(writer);
        } catch (Exception failure) {
            try {
                close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void initializeInternal(boolean writer) throws Exception {
        Map<String, String> catalogOptions =
                new LinkedHashMap<>(configuration.catalogOptions());
        catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                Options.fromMap(catalogOptions), new Configuration()));
        Identifier sessionsIdentifier =
                Identifier.create(project.database(), project.sessionsTable());
        Identifier messagesIdentifier =
                Identifier.create(project.database(), project.messagesTable());
        if (writer) {
            catalog.createDatabase(project.database(), true);
            ensureWriterTables(sessionsIdentifier, messagesIdentifier);
        }

        sessionsTable = catalog.getTable(sessionsIdentifier);
        messagesTable = catalog.getTable(messagesIdentifier);
        if (writer) {
            // Validate both ownership and the exact legacy/current layouts before applying any
            // schema mutation. A differently configured collector must never upgrade this pair.
            validateSessionsTableForUpgrade(sessionsTable);
            validateMessagesTable(messagesTable);
            validateTableOwnership(true);
            ensureSessionsMetadataColumns(sessionsIdentifier);
        }
        validateSessionsTable(sessionsTable);
        validateMessagesTable(messagesTable);
        validateTableOwnership(writer);

        if (!writer) {
            return;
        }

        ensureMessagesMoraxBtreeOptions(messagesIdentifier);

        sessionsBuilder =
                sessionsTable
                        .newStreamWriteBuilder()
                        .withCommitUser(project.commitUser() + "-sessions");
        sessionsCommit = sessionsBuilder.newCommit();

        messagesBuilder =
                messagesTable
                        .newStreamWriteBuilder()
                        .withCommitUser(project.commitUser() + "-messages");
        messagesCommit = messagesBuilder.newCommit();
    }

    private void ensureWriterTables(
            Identifier sessionsIdentifier, Identifier messagesIdentifier) throws Exception {
        boolean sessionsExists = tableExists(sessionsIdentifier);
        boolean messagesExists = tableExists(messagesIdentifier);

        // When only the append table exists, validate its owner before creating anything. This
        // avoids completing a half-created table pair with a different collector identity.
        if (!sessionsExists && messagesExists) {
            validateExistingWriterTable(catalog.getTable(messagesIdentifier), false);
        }

        if (!sessionsExists) {
            catalog.createTable(sessionsIdentifier, sessionsSchema(), true);
        }
        validateExistingWriterTable(catalog.getTable(sessionsIdentifier), true);

        // Always establish and verify the primary-key table owner first. Two differently
        // configured creators racing on an empty database can then never claim one table each.
        if (!tableExists(messagesIdentifier)) {
            catalog.createTable(messagesIdentifier, messagesSchema(), true);
        }
        validateExistingWriterTable(catalog.getTable(messagesIdentifier), false);
    }

    private boolean tableExists(Identifier identifier) throws Exception {
        try {
            catalog.getTable(identifier);
            return true;
        } catch (Catalog.TableNotExistException ignored) {
            return false;
        }
    }

    private void validateExistingWriterTable(Table table, boolean sessions) {
        if (sessions) {
            validateSessionsTableForUpgrade(table);
        } else {
            validateMessagesTable(table);
        }
        String tableName = sessions ? project.sessionsTable() : project.messagesTable();
        String owner = table.options().get(WRITER_ID_OPTION);
        if (owner == null || owner.trim().isEmpty()) {
            throw new IllegalStateException(
                    tableName
                            + " is missing "
                            + WRITER_ID_OPTION
                            + "; create new version-1 tables before using this build");
        }
        if (!project.collectorId().equals(owner)) {
            throw new IllegalStateException(
                    tableName
                            + " is owned by collector.id="
                            + owner
                            + "; this collector uses "
                            + project.collectorId()
                            + ". Version 1 permits only one writer per table pair.");
        }
    }

    @Override
    public Map<SessionKey, ChatSession> loadSessions() throws Exception {
        ensureInitialized();
        Map<SessionKey, ChatSession> sessions = new HashMap<>();
        ReadBuilder builder = sessionsTable.newReadBuilder();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        ChatSession session = fromSessionRow(row);
                        sessions.put(session.key(), session);
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return sessions;
    }

    @Override
    public void forEachMessage(
            String sourceType, Set<String> sessionIds, ChatMessageConsumer consumer)
            throws Exception {
        ensureInitialized();
        PredicateBuilder predicates = new PredicateBuilder(messagesTable.rowType());
        Predicate filter =
                predicates.equal(
                        predicates.indexOf("source_type"),
                        BinaryString.fromString(sourceType));
        if (!sessionIds.isEmpty()) {
            List<Object> sessions = new java.util.ArrayList<>(sessionIds.size());
            for (String sessionId : sessionIds) {
                sessions.add(BinaryString.fromString(sessionId));
            }
            filter =
                    PredicateBuilder.and(
                            filter,
                            predicates.in(predicates.indexOf("session_id"), sessions));
        }
        ReadBuilder builder = messagesTable.newReadBuilder().withFilter(filter);
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        if (!sourceType.equals(row.getString(1).toString())) {
                            continue;
                        }
                        String sessionId = row.getString(2).toString();
                        if (!sessionIds.isEmpty() && !sessionIds.contains(sessionId)) {
                            continue;
                        }
                        consumer.accept(fromMessageRow(row));
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
    }

    @Override
    public void commit(long commitIdentifier, List<SessionBatch> batches) throws Exception {
        ensureInitialized();
        if (batches.isEmpty()) {
            return;
        }

        long pendingSessionCommitIdentifier = Math.multiplyExact(commitIdentifier, 2L);
        long finalSessionCommitIdentifier =
                Math.addExact(pendingSessionCommitIdentifier, 1L);
        Instant pendingAt = Instant.now();
        try (StreamTableWrite write = sessionsBuilder.newWrite()) {
            for (SessionBatch batch : batches) {
                ChatSession pendingSession =
                        batch.session()
                                .withPendingCommit(
                                        batch.startingCursor(),
                                        batch.startingCommitId(),
                                        commitIdentifier,
                                        batch.session().sourceCursor(),
                                        pendingAt);
                write.write(toSessionRow(pendingSession));
            }
            List<CommitMessage> pendingCommitMessages =
                    write.prepareCommit(false, pendingSessionCommitIdentifier);
            sessionsCommit.filterAndCommit(
                    Collections.singletonMap(
                            pendingSessionCommitIdentifier, pendingCommitMessages));
        }

        boolean hasMessages = false;
        for (SessionBatch batch : batches) {
            hasMessages |= !batch.messages().isEmpty();
        }
        if (hasMessages) {
            try (StreamTableWrite write = messagesBuilder.newWrite()) {
                for (SessionBatch batch : batches) {
                    for (ChatMessage message : batch.messages()) {
                        write.write(toMessageRow(message));
                    }
                }
                List<CommitMessage> commitMessages =
                        write.prepareCommit(false, commitIdentifier);
                messagesCommit.filterAndCommit(
                        Collections.singletonMap(commitIdentifier, commitMessages));
            }
        }

        Instant committedAt = Instant.now();
        try (StreamTableWrite write = sessionsBuilder.newWrite()) {
            for (SessionBatch batch : batches) {
                ChatSession session =
                        batch.session()
                                .withCheckpoint(
                                        batch.session().sourceCursor(),
                                        commitIdentifier,
                                        committedAt);
                write.write(toSessionRow(session));
            }
            List<CommitMessage> sessionCommitMessages =
                    write.prepareCommit(false, finalSessionCommitIdentifier);
            sessionsCommit.filterAndCommit(
                    Collections.singletonMap(
                            finalSessionCommitIdentifier, sessionCommitMessages));
        }
    }

    private Schema sessionsSchema() {
        return sessionsSchema(true, true);
    }

    private Schema sessionsSchemaWithSubagentSource() {
        return sessionsSchema(true, false);
    }

    private Schema legacySessionsSchema() {
        return sessionsSchema(false, false);
    }

    private Schema sessionsSchema(
            boolean includeSubagentSource, boolean includeProjectless) {
        Schema.Builder builder =
                Schema.newBuilder()
                .column("source_type", DataTypes.STRING())
                .column("session_id", DataTypes.STRING())
                .column("title", DataTypes.STRING())
                .column("cwd", DataTypes.STRING())
                .column("archived", DataTypes.BOOLEAN())
                .column("source_path", DataTypes.STRING())
                .column("source_cursor", DataTypes.STRING())
                .column("last_commit_id", DataTypes.BIGINT())
                .column("pending_commit_id", DataTypes.BIGINT())
                .column("pending_cursor", DataTypes.STRING())
                .column("created_at", DataTypes.TIMESTAMP(3))
                .column("updated_at", DataTypes.TIMESTAMP(3))
                .column("last_message_at", DataTypes.TIMESTAMP(3))
                .column("ingested_at", DataTypes.TIMESTAMP(3));
        if (includeSubagentSource) {
            builder.column(SUBAGENT_SOURCE_JSON_COLUMN, DataTypes.STRING());
        }
        if (includeProjectless) {
            builder.column(PROJECTLESS_COLUMN, DataTypes.BOOLEAN());
        }
        return builder
                .primaryKey("source_type", "session_id")
                .option(CoreOptions.BUCKET.key(), "1")
                .option(WRITER_ID_OPTION, project.collectorId())
                .build();
    }

    private Schema messagesSchema() {
        return Schema.newBuilder()
                .column("message_id", DataTypes.STRING().notNull())
                .column("source_type", DataTypes.STRING().notNull())
                .column("session_id", DataTypes.STRING().notNull())
                .column("sequence_no", DataTypes.BIGINT())
                .column("role", DataTypes.STRING())
                .column("event_type", DataTypes.STRING())
                .column("content_json", DataTypes.STRING().notNull())
                .column("attachments", DataTypes.ARRAY(DataTypes.BLOB()))
                .column("created_at", DataTypes.TIMESTAMP(3))
                .column("ingested_at", DataTypes.TIMESTAMP(3))
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(MORAX_BTREE_INDEX_ENABLED_OPTION, "true")
                .option(MORAX_BTREE_INDEX_COLUMN_OPTION, MORAX_BTREE_INDEX_COLUMN)
                .option(WRITER_ID_OPTION, project.collectorId())
                .build();
    }

    private void ensureSessionsMetadataColumns(Identifier sessionsIdentifier)
            throws Exception {
        Set<String> columns =
                sessionsTable.rowType().getFields().stream()
                        .map(DataField::name)
                        .collect(java.util.stream.Collectors.toSet());
        List<SchemaChange> changes = new ArrayList<>();
        if (!columns.contains(SUBAGENT_SOURCE_JSON_COLUMN)) {
            changes.add(
                    SchemaChange.addColumn(
                            SUBAGENT_SOURCE_JSON_COLUMN, DataTypes.STRING()));
        }
        if (!columns.contains(PROJECTLESS_COLUMN)) {
            changes.add(
                    SchemaChange.addColumn(
                            PROJECTLESS_COLUMN, DataTypes.BOOLEAN()));
        }
        if (!changes.isEmpty()) {
            catalog.alterTable(
                    sessionsIdentifier,
                    changes,
                    false);
            sessionsTable = catalog.getTable(sessionsIdentifier);

            // Reload and revalidate before any writer is constructed. This also catches a
            // concurrent or server-side alteration which did not produce the requested layout.
            validateSessionsTable(sessionsTable);
            validateTableOwnership(true);
        }
    }

    private void ensureMessagesMoraxBtreeOptions(Identifier messagesIdentifier)
            throws Exception {
        List<SchemaChange> changes = new ArrayList<>();
        if (!"true".equals(messagesTable.options().get(MORAX_BTREE_INDEX_ENABLED_OPTION))) {
            changes.add(SchemaChange.setOption(MORAX_BTREE_INDEX_ENABLED_OPTION, "true"));
        }
        if (!MORAX_BTREE_INDEX_COLUMN.equals(
                messagesTable.options().get(MORAX_BTREE_INDEX_COLUMN_OPTION))) {
            changes.add(
                    SchemaChange.setOption(
                            MORAX_BTREE_INDEX_COLUMN_OPTION, MORAX_BTREE_INDEX_COLUMN));
        }
        if (!changes.isEmpty()) {
            catalog.alterTable(messagesIdentifier, changes, false);
            messagesTable = catalog.getTable(messagesIdentifier);

            // Never continue with write builders unless the altered table still has the exact
            // version-1 structure and belongs to this collector.
            validateMessagesTable(messagesTable);
            validateTableOwnership(true);
        }
        if (!"true".equals(messagesTable.options().get(MORAX_BTREE_INDEX_ENABLED_OPTION))
                || !MORAX_BTREE_INDEX_COLUMN.equals(
                        messagesTable.options().get(MORAX_BTREE_INDEX_COLUMN_OPTION))) {
            throw new IllegalStateException(
                    "ai_chat_messages must enable the Morax BTree index for session_id");
        }
    }

    private void validateTableOwnership(boolean writer) {
        String sessionsOwner = sessionsTable.options().get(WRITER_ID_OPTION);
        String messagesOwner = messagesTable.options().get(WRITER_ID_OPTION);
        if (sessionsOwner == null || sessionsOwner.trim().isEmpty()) {
            throw new IllegalStateException(
                    project.sessionsTable()
                            + " is missing "
                            + WRITER_ID_OPTION
                            + "; create new version-1 tables before using this build");
        }
        if (!sessionsOwner.equals(messagesOwner)) {
            throw new IllegalStateException(
                    "Chat table writer ownership differs: sessions="
                            + sessionsOwner
                            + ", messages="
                            + messagesOwner);
        }
        if (writer && !project.collectorId().equals(sessionsOwner)) {
            throw new IllegalStateException(
                    "Chat tables are owned by collector.id="
                            + sessionsOwner
                            + "; this collector uses "
                            + project.collectorId()
                            + ". Version 1 permits only one writer per table pair.");
        }
    }

    private static GenericRow toSessionRow(ChatSession session) {
        return GenericRow.of(
                string(session.key().sourceType()),
                string(session.key().sessionId()),
                string(session.title()),
                string(session.cwd()),
                session.archived(),
                string(session.sourcePath()),
                string(session.sourceCursor()),
                session.lastCommitId(),
                session.pendingCommitId(),
                string(session.pendingCursor()),
                timestamp(session.createdAt()),
                timestamp(session.updatedAt()),
                timestamp(session.lastMessageAt()),
                timestamp(session.ingestedAt()),
                string(session.subagentSourceJson()),
                session.projectless());
    }

    private static GenericRow toMessageRow(ChatMessage message) {
        Object[] blobs = new Object[message.attachments().size()];
        for (int index = 0; index < blobs.length; index++) {
            AttachmentPayload payload = message.attachments().get(index);
            byte[] bytes = payload.bytes();
            blobs[index] = bytes == null ? null : new BlobData(bytes);
        }
        return GenericRow.of(
                string(message.messageId()),
                string(message.sessionKey().sourceType()),
                string(message.sessionKey().sessionId()),
                message.sequenceNumber(),
                string(message.role()),
                string(message.eventType()),
                string(message.contentJson()),
                new GenericArray(blobs),
                timestamp(message.createdAt()),
                timestamp(message.ingestedAt()));
    }

    private static ChatSession fromSessionRow(InternalRow row) {
        SessionKey key = new SessionKey(row.getString(0).toString(), row.getString(1).toString());
        return new ChatSession(
                key,
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
                row.isNullAt(15) ? null : row.getBoolean(15));
    }

    private static ChatMessage fromMessageRow(InternalRow row) {
        SessionKey key = new SessionKey(row.getString(1).toString(), row.getString(2).toString());
        List<AttachmentPayload> attachments = new java.util.ArrayList<>();
        if (!row.isNullAt(7)) {
            InternalArray values = row.getArray(7);
            for (int index = 0; index < values.size(); index++) {
                attachments.add(
                        values.isNullAt(index)
                                ? AttachmentPayload.missing()
                                : AttachmentPayload.of(values.getBlob(index).toData()));
            }
        }
        return new ChatMessage(
                row.getString(0).toString(),
                key,
                row.isNullAt(3) ? 0L : row.getLong(3),
                nullableString(row, 4),
                nullableString(row, 5),
                row.getString(6).toString(),
                attachments,
                nullableTimestamp(row, 8),
                row.isNullAt(9) ? Instant.EPOCH : row.getTimestamp(9, 3).toInstant());
    }

    private static BinaryString string(String value) {
        return value == null ? null : BinaryString.fromString(value);
    }

    private static Timestamp timestamp(Instant value) {
        return value == null ? null : Timestamp.fromInstant(value);
    }

    private static String nullableString(InternalRow row, int position) {
        return row.isNullAt(position) ? null : row.getString(position).toString();
    }

    private static Instant nullableTimestamp(InternalRow row, int position) {
        return row.isNullAt(position) ? null : row.getTimestamp(position, 3).toInstant();
    }

    private void validateSessionsTable(Table table) {
        validateSessionsTableStructure(table);
        requireRowType(table.rowType(), sessionsSchema().rowType(), "ai_chat_sessions");
    }

    private void validateSessionsTableForUpgrade(Table table) {
        validateSessionsTableStructure(table);
        RowType actual = table.rowType();
        if (!rowTypeMatches(actual, sessionsSchema().rowType())
                && !rowTypeMatches(actual, sessionsSchemaWithSubagentSource().rowType())
                && !rowTypeMatches(actual, legacySessionsSchema().rowType())) {
            throw new IllegalStateException(
                    "ai_chat_sessions row type must be the current schema or a supported legacy schema without "
                            + PROJECTLESS_COLUMN
                            + " and/or "
                            + SUBAGENT_SOURCE_JSON_COLUMN
                            + ", but found "
                            + actual);
        }
    }

    private void validateSessionsTableStructure(Table table) {
        if (!table.partitionKeys().isEmpty()) {
            throw new IllegalStateException("ai_chat_sessions must not be partitioned");
        }
        if (!table.primaryKeys().equals(java.util.Arrays.asList("source_type", "session_id"))) {
            throw new IllegalStateException(
                    "ai_chat_sessions primary key must be (source_type, session_id), but was "
                            + table.primaryKeys());
        }
        if (!"1".equals(table.options().get(CoreOptions.BUCKET.key()))) {
            throw new IllegalStateException("ai_chat_sessions must use bucket=1");
        }
    }

    private void validateMessagesTable(Table table) {
        if (!table.primaryKeys().isEmpty()) {
            throw new IllegalStateException("ai_chat_messages must be an append table without a primary key");
        }
        if (!table.partitionKeys().isEmpty()) {
            throw new IllegalStateException("ai_chat_messages must not be partitioned in version 1");
        }
        requireRowType(table.rowType(), messagesSchema().rowType(), "ai_chat_messages");
        DataField attachmentField = table.rowType().getFields().get(7);
        if (!(attachmentField.type() instanceof ArrayType)
                || !(((ArrayType) attachmentField.type()).getElementType() instanceof BlobType)) {
            throw new IllegalStateException("ai_chat_messages.attachments must be ARRAY<BLOB>");
        }
        if (!"true".equals(table.options().get(CoreOptions.ROW_TRACKING_ENABLED.key()))
                || !"true".equals(table.options().get(CoreOptions.DATA_EVOLUTION_ENABLED.key()))
                || !"-1".equals(table.options().get(CoreOptions.BUCKET.key()))) {
            throw new IllegalStateException(
                    "ai_chat_messages must enable row tracking, data evolution, and bucket=-1");
        }
    }

    private static void requireRowType(RowType actual, RowType expected, String tableName) {
        if (rowTypeMatches(actual, expected)) {
            return;
        }
        throw new IllegalStateException(
                tableName + " row type must be " + expected + " but found " + actual);
    }

    private static boolean rowTypeMatches(RowType actual, RowType expected) {
        List<DataField> actualFields = actual.getFields();
        List<DataField> expectedFields = expected.getFields();
        if (actualFields.size() != expectedFields.size()) {
            return false;
        }
        for (int index = 0; index < actualFields.size(); index++) {
            DataField actualField = actualFields.get(index);
            DataField expectedField = expectedFields.get(index);
            if (!actualField.name().equals(expectedField.name())
                    || !actualField.type().equals(expectedField.type())) {
                return false;
            }
        }
        return true;
    }

    private void ensureInitialized() {
        if (catalog == null) {
            throw new IllegalStateException("Repository has not been initialized");
        }
    }

    @Override
    public void close() throws Exception {
        AutoCloseable[] closeables =
                new AutoCloseable[] {messagesCommit, sessionsCommit, catalog};
        messagesCommit = null;
        sessionsCommit = null;
        catalog = null;
        messagesBuilder = null;
        sessionsBuilder = null;
        messagesTable = null;
        sessionsTable = null;
        Exception failure = null;
        for (AutoCloseable closeable : closeables) {
            if (closeable == null) {
                continue;
            }
            try {
                closeable.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
