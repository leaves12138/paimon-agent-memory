package org.apache.paimon.agent.sink;

import org.apache.paimon.agent.config.AgentConfiguration;
import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.config.SourceConfig;
import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonChatRepositoryTest {

    private static final Identifier SESSIONS_IDENTIFIER =
            Identifier.create("ai_memory", "ai_chat_sessions");
    private static final Identifier MESSAGES_IDENTIFIER =
            Identifier.create("ai_memory", "ai_chat_messages");
    private static final String SUBAGENT_SOURCE_JSON_COLUMN = "subagent_source_json";
    private static final String PROJECTLESS_COLUMN = "projectless";
    private static final String MORAX_BTREE_INDEX_ENABLED_OPTION =
            "morax.btree-index.enabled";
    private static final String MORAX_BTREE_INDEX_COLUMN_OPTION =
            "global-index.btree.index-column";

    @TempDir Path tempDir;

    @Test
    void createsSessionsTableWithNullableMetadataColumns() throws Exception {
        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
            assertThat(repository.sessionsTableForRead().rowType().getFields())
                    .extracting(field -> field.name())
                    .endsWith(SUBAGENT_SOURCE_JSON_COLUMN, PROJECTLESS_COLUMN);
            assertThat(
                            repository
                                    .sessionsTableForRead()
                                    .rowType()
                                    .getFields()
                                    .get(14)
                                    .type()
                                    .isNullable())
                    .isTrue();
            assertThat(
                            repository
                                    .sessionsTableForRead()
                                    .rowType()
                                    .getFields()
                                    .get(15)
                                    .type()
                                    .isNullable())
                    .isTrue();
        }
    }

    @Test
    void onlyTheOwningWriterAddsMetadataColumnsToLegacySessionsTable() throws Exception {
        SessionKey key = new SessionKey("codex", "legacy-root");
        Instant instant = Instant.parse("2026-01-01T00:00:00Z");
        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
            repository.commit(
                    0L,
                    Collections.singletonList(
                            new SessionBatch(
                                    session(key, instant, instant), Collections.emptyList())));
        }
        dropSessionMetadataColumns();
        assertSessionMetadataColumnsAbsent();

        try (PaimonChatRepository restoreReader =
                new PaimonChatRepository(configuration("restore-machine"))) {
            assertThatThrownBy(restoreReader::initializeForRestore)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(PROJECTLESS_COLUMN);
        }
        assertSessionMetadataColumnsAbsent();

        try (PaimonChatRepository foreignWriter =
                new PaimonChatRepository(configuration("another-writer"))) {
            assertThatThrownBy(foreignWriter::initialize)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("only one writer per table pair");
        }
        assertSessionMetadataColumnsAbsent();

        try (PaimonChatRepository owningWriter = new PaimonChatRepository(configuration())) {
            owningWriter.initialize();
            assertThat(owningWriter.sessionsTableForRead().rowType().getFields())
                    .extracting(field -> field.name())
                    .endsWith(SUBAGENT_SOURCE_JSON_COLUMN, PROJECTLESS_COLUMN);
            assertThat(owningWriter.loadSessions().get(key).subagentSourceJson()).isNull();
            assertThat(owningWriter.loadSessions().get(key).projectless()).isNull();
        }
    }

    @Test
    void owningWriterAddsProjectlessColumnToPreviousSessionsSchema() throws Exception {
        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
        }
        dropProjectlessColumn();
        assertProjectlessColumnAbsent();

        try (PaimonChatRepository restoreReader =
                new PaimonChatRepository(configuration("restore-machine"))) {
            assertThatThrownBy(restoreReader::initializeForRestore)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(PROJECTLESS_COLUMN);
        }
        assertProjectlessColumnAbsent();

        try (PaimonChatRepository owningWriter = new PaimonChatRepository(configuration())) {
            owningWriter.initialize();
            assertThat(owningWriter.sessionsTableForRead().rowType().getFields())
                    .extracting(field -> field.name())
                    .endsWith(SUBAGENT_SOURCE_JSON_COLUMN, PROJECTLESS_COLUMN);
        }
    }

    @Test
    void createsMessagesTableWithMoraxBtreeOptions() throws Exception {
        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
            assertMoraxBtreeOptions(repository.messagesTableForRead().options());
        }
    }

    @Test
    void onlyTheOwningWriterUpgradesLegacyMessagesTable() throws Exception {
        try (PaimonChatRepository repository = new PaimonChatRepository(configuration())) {
            repository.initialize();
        }
        removeMoraxBtreeOptions();

        try (PaimonChatRepository restoreReader =
                new PaimonChatRepository(configuration("restore-machine"))) {
            restoreReader.initializeForRestore();
            assertMoraxBtreeOptionsAbsent(restoreReader.messagesTableForRead().options());
        }
        assertMoraxBtreeOptionsAbsent(loadMessagesOptions());

        try (PaimonChatRepository foreignWriter =
                new PaimonChatRepository(configuration("another-writer"))) {
            assertThatThrownBy(foreignWriter::initialize)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("only one writer per table pair");
        }
        assertMoraxBtreeOptionsAbsent(loadMessagesOptions());

        try (PaimonChatRepository owningWriter = new PaimonChatRepository(configuration())) {
            owningWriter.initialize();
            assertMoraxBtreeOptions(owningWriter.messagesTableForRead().options());
        }
        assertMoraxBtreeOptions(loadMessagesOptions());
    }

    @Test
    void readsMessagesWithSourceAndSessionFiltersAndRoundTripsArrayBlob() throws Exception {
        AgentConfiguration configuration = configuration();
        SessionKey selectedKey = new SessionKey("codex", "s1");
        SessionKey otherCodexKey = new SessionKey("codex", "s2");
        SessionKey claudeKey = new SessionKey("claude", "s1");
        Instant createdAt = Instant.parse("2026-01-01T00:00:00.123Z");
        Instant ingestedAt = Instant.parse("2026-01-01T00:05:00.456Z");
        ChatSession selectedSession =
                new ChatSession(
                        selectedKey,
                        "title",
                        "/tmp",
                        false,
                        "/tmp/s1.jsonl",
                        "byte:100",
                        0,
                        createdAt,
                        createdAt,
                        createdAt,
                        ingestedAt,
                        "{\"thread_spawn\":{\"parent_thread_id\":\"root\",\"depth\":1}}")
                        .withProjectless(true);
        ChatMessage selectedMessage =
                new ChatMessage(
                        "m1",
                        selectedKey,
                        42,
                        "user",
                        "response_item",
                        "{\"text\":\"hello 世界\",\"nested\":{\"value\":7}}",
                        Arrays.asList(
                                AttachmentPayload.of("blob".getBytes(StandardCharsets.UTF_8)),
                                AttachmentPayload.missing(),
                                AttachmentPayload.of(new byte[0])),
                        createdAt,
                        ingestedAt);
        ChatSession otherCodexSession = session(otherCodexKey, createdAt, ingestedAt);
        ChatMessage otherCodexMessage =
                message("m2", otherCodexKey, 43, "assistant", createdAt, ingestedAt);
        ChatSession claudeSession = session(claudeKey, createdAt, ingestedAt);
        ChatMessage claudeMessage =
                message("m3", claudeKey, 44, "assistant", createdAt, ingestedAt);
        List<SessionBatch> batches =
                Arrays.asList(
                        new SessionBatch(
                                selectedSession, Collections.singletonList(selectedMessage)),
                        new SessionBatch(
                                otherCodexSession,
                                Collections.singletonList(otherCodexMessage)),
                        new SessionBatch(
                                claudeSession, Collections.singletonList(claudeMessage)));

        try (PaimonChatRepository repository = new PaimonChatRepository(configuration)) {
            repository.initialize();
            repository.commit(0L, batches);
            repository.commit(0L, batches);

            assertThat(repository.loadSessions()).containsKeys(selectedKey, otherCodexKey, claudeKey);
            assertThat(repository.loadSessions().get(selectedKey).lastCommitId()).isZero();
            assertThat(repository.loadSessions().get(selectedKey).pendingCommitId()).isNull();
            assertThat(repository.loadSessions().get(selectedKey).pendingCursor()).isNull();
            assertThat(repository.loadSessions().get(selectedKey).subagentSourceJson())
                    .isEqualTo(selectedSession.subagentSourceJson());
            assertThat(repository.loadSessions().get(selectedKey).projectless()).isTrue();

            List<ChatMessage> selected = new ArrayList<>();
            repository.forEachMessage(
                    "codex", Collections.singleton("s1"), selected::add);
            assertThat(selected).hasSize(1);
            assertRoundTrip(selected.get(0), selectedMessage);

            List<ChatMessage> allCodex = new ArrayList<>();
            repository.forEachMessage("codex", Collections.emptySet(), allCodex::add);
            assertThat(allCodex)
                    .extracting(message -> message.sessionKey().sessionId())
                    .containsExactlyInAnyOrder("s1", "s2");
            assertThat(allCodex)
                    .extracting(message -> message.sessionKey().sourceType())
                    .containsOnly("codex");

            List<ChatMessage> claude = new ArrayList<>();
            repository.forEachMessage(
                    "claude", Collections.singleton("s1"), claude::add);
            assertThat(claude).hasSize(1);
            assertRoundTrip(claude.get(0), claudeMessage);

            List<ChatMessage> missingSession = new ArrayList<>();
            repository.forEachMessage(
                    "codex", Collections.singleton("missing"), missingSession::add);
            assertThat(missingSession).isEmpty();
        }

        Map<String, String> options = new LinkedHashMap<>();
        options.put("metastore", "filesystem");
        options.put("warehouse", tempDir.toString());
        try (Catalog catalog =
                CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(options)))) {
            Table table = catalog.getTable(Identifier.create("ai_memory", "ai_chat_messages"));
            ReadBuilder builder = table.newReadBuilder();
            int count = 0;
            try (RecordReader<InternalRow> reader =
                    builder.newRead().createReader(builder.newScan().plan())) {
                RecordReader.RecordIterator<InternalRow> rows;
                while ((rows = reader.readBatch()) != null) {
                    try {
                        InternalRow row;
                        while ((row = rows.next()) != null) {
                            count++;
                            if ("m1".equals(row.getString(0).toString())) {
                                assertThat(row.getString(6).toString())
                                        .isEqualTo(selectedMessage.contentJson());
                                InternalArray attachments = row.getArray(7);
                                assertThat(attachments.size()).isEqualTo(3);
                                assertThat(attachments.getBlob(0).toData())
                                        .isEqualTo("blob".getBytes(StandardCharsets.UTF_8));
                                assertThat(attachments.isNullAt(1)).isTrue();
                                assertThat(attachments.getBlob(2).toData()).isEmpty();
                            }
                        }
                    } finally {
                        rows.releaseBatch();
                    }
                }
            }
            assertThat(count).isEqualTo(3);
        }

        try (PaimonChatRepository differentWriter =
                new PaimonChatRepository(configuration("another-writer"))) {
            assertThatThrownBy(differentWriter::initialize)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("only one writer per table pair");
        }
        try (PaimonChatRepository restoreReader =
                new PaimonChatRepository(configuration("restore-machine"))) {
            restoreReader.initializeForRestore();
            assertThat(restoreReader.loadSessions()).containsKey(selectedKey);
        }

        try (Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                Options.fromMap(
                                        Map.of(
                                                "metastore",
                                                "filesystem",
                                                "warehouse",
                                                tempDir.toString()))))) {
            catalog.dropTable(MESSAGES_IDENTIFIER, false);
        }
        try (PaimonChatRepository differentWriter =
                new PaimonChatRepository(configuration("half-pair-writer"))) {
            assertThatThrownBy(differentWriter::initialize)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("ai_chat_sessions is owned by collector.id=test-agent");
        }
        try (Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                Options.fromMap(
                                        Map.of(
                                                "metastore",
                                                "filesystem",
                                                "warehouse",
                                                tempDir.toString()))))) {
            assertThatThrownBy(() -> catalog.getTable(MESSAGES_IDENTIFIER))
                    .isInstanceOf(Catalog.TableNotExistException.class);
        }
    }

    private static ChatSession session(SessionKey key, Instant createdAt, Instant ingestedAt) {
        return new ChatSession(
                key,
                "title-" + key.sourceType() + "-" + key.sessionId(),
                "/tmp",
                false,
                "/tmp/" + key.sessionId() + ".jsonl",
                "byte:100",
                0,
                createdAt,
                createdAt,
                createdAt,
                ingestedAt);
    }

    private void removeMoraxBtreeOptions() throws Exception {
        try (Catalog catalog = localCatalog()) {
            catalog.alterTable(
                    MESSAGES_IDENTIFIER,
                    Arrays.asList(
                            SchemaChange.removeOption(MORAX_BTREE_INDEX_ENABLED_OPTION),
                            SchemaChange.removeOption(MORAX_BTREE_INDEX_COLUMN_OPTION)),
                    false);
        }
    }

    private void dropSessionMetadataColumns() throws Exception {
        try (Catalog catalog = localCatalog()) {
            catalog.alterTable(
                    SESSIONS_IDENTIFIER,
                    Arrays.asList(
                            SchemaChange.dropColumn(PROJECTLESS_COLUMN),
                            SchemaChange.dropColumn(SUBAGENT_SOURCE_JSON_COLUMN)),
                    false);
        }
    }

    private void assertSessionMetadataColumnsAbsent() throws Exception {
        try (Catalog catalog = localCatalog()) {
            assertThat(catalog.getTable(SESSIONS_IDENTIFIER).rowType().getFields())
                    .extracting(field -> field.name())
                    .doesNotContain(SUBAGENT_SOURCE_JSON_COLUMN, PROJECTLESS_COLUMN);
        }
    }

    private void dropProjectlessColumn() throws Exception {
        try (Catalog catalog = localCatalog()) {
            catalog.alterTable(
                    SESSIONS_IDENTIFIER,
                    Collections.singletonList(SchemaChange.dropColumn(PROJECTLESS_COLUMN)),
                    false);
        }
    }

    private void assertProjectlessColumnAbsent() throws Exception {
        try (Catalog catalog = localCatalog()) {
            assertThat(catalog.getTable(SESSIONS_IDENTIFIER).rowType().getFields())
                    .extracting(field -> field.name())
                    .doesNotContain(PROJECTLESS_COLUMN)
                    .contains(SUBAGENT_SOURCE_JSON_COLUMN);
        }
    }

    private Map<String, String> loadMessagesOptions() throws Exception {
        try (Catalog catalog = localCatalog()) {
            return new LinkedHashMap<>(catalog.getTable(MESSAGES_IDENTIFIER).options());
        }
    }

    private Catalog localCatalog() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("metastore", "filesystem");
        options.put("warehouse", tempDir.toString());
        return CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(options)));
    }

    private static void assertMoraxBtreeOptions(Map<String, String> options) {
        assertThat(options)
                .containsEntry(MORAX_BTREE_INDEX_ENABLED_OPTION, "true")
                .containsEntry(MORAX_BTREE_INDEX_COLUMN_OPTION, "session_id");
    }

    private static void assertMoraxBtreeOptionsAbsent(Map<String, String> options) {
        assertThat(options)
                .doesNotContainKeys(
                        MORAX_BTREE_INDEX_ENABLED_OPTION, MORAX_BTREE_INDEX_COLUMN_OPTION);
    }

    private static ChatMessage message(
            String id,
            SessionKey key,
            long sequenceNumber,
            String role,
            Instant createdAt,
            Instant ingestedAt) {
        return new ChatMessage(
                id,
                key,
                sequenceNumber,
                role,
                "message",
                "{\"id\":\"" + id + "\"}",
                Collections.emptyList(),
                createdAt,
                ingestedAt);
    }

    private static void assertRoundTrip(ChatMessage actual, ChatMessage expected) {
        assertThat(actual.messageId()).isEqualTo(expected.messageId());
        assertThat(actual.sessionKey()).isEqualTo(expected.sessionKey());
        assertThat(actual.sequenceNumber()).isEqualTo(expected.sequenceNumber());
        assertThat(actual.role()).isEqualTo(expected.role());
        assertThat(actual.eventType()).isEqualTo(expected.eventType());
        assertThat(actual.contentJson()).isEqualTo(expected.contentJson());
        assertThat(actual.createdAt()).isEqualTo(expected.createdAt());
        assertThat(actual.ingestedAt()).isEqualTo(expected.ingestedAt());
        assertThat(actual.attachments()).hasSameSizeAs(expected.attachments());
        for (int index = 0; index < expected.attachments().size(); index++) {
            AttachmentPayload actualAttachment = actual.attachments().get(index);
            AttachmentPayload expectedAttachment = expected.attachments().get(index);
            assertThat(actualAttachment.isMissing()).isEqualTo(expectedAttachment.isMissing());
            assertThat(actualAttachment.bytes()).isEqualTo(expectedAttachment.bytes());
        }
    }

    private AgentConfiguration configuration() {
        return configuration("test-agent");
    }

    private AgentConfiguration configuration(String collectorId) {
        Map<String, String> catalog = new LinkedHashMap<>();
        // Unit tests construct the repository directly with a local Catalog. The public
        // ConfigLoader intentionally accepts only metastore=rest.
        catalog.put("metastore", "filesystem");
        catalog.put("warehouse", tempDir.toString());
        ProjectConfig project =
                new ProjectConfig(
                        "ai_memory",
                        "ai_chat_sessions",
                        "ai_chat_messages",
                        Duration.ofMinutes(5),
                        Duration.ofMinutes(5),
                        false,
                        collectorId,
                        new SourceConfig(false, tempDir),
                        new SourceConfig(false, tempDir),
                        true,
                        false,
                        1024 * 1024,
                        100,
                        100,
                        0,
                        Duration.ofSeconds(1));
        return new AgentConfiguration(catalog, project);
    }
}
