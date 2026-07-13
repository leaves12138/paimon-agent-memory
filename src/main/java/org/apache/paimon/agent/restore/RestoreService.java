package org.apache.paimon.agent.restore;

import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionKey;
import org.apache.paimon.agent.sink.ChatRepository;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
        int pendingSessions = 0;
        List<ChatSession> stableCandidates = new ArrayList<>();
        for (ChatSession session : candidates) {
            if (session.hasPendingCommit()) {
                pendingSessions++;
                LOG.warn(
                        "Skipping {} session {} because its Paimon commit is still pending; retry after collection completes",
                        options.type(),
                        session.key().sessionId());
            } else {
                stableCandidates.add(session);
            }
        }
        if (stableCandidates.isEmpty()) {
            return new RestoreSummary(0, 0, pendingSessions);
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
            Map<String, ChatSession> selected = new HashMap<>();
            int skippedSessions = pendingSessions;
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
                    Comparator.comparing(
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
        List<ChatSession> sessions =
                repository.loadSessions().values().stream()
                        .filter(
                                session ->
                                        options.type()
                                                .sourceType()
                                                .equals(session.key().sourceType()))
                        .filter(
                                session ->
                                        options.sessionId() == null
                                                || options.sessionId()
                                                        .equals(session.key().sessionId()))
                        .collect(Collectors.toList());
        if (options.sessionId() != null && sessions.isEmpty()) {
            throw new IllegalArgumentException(
                    "No "
                            + options.type().sourceType()
                            + " session found with id "
                            + options.sessionId());
        }
        return sessions;
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
