package org.apache.paimon.agent.service;

import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.model.SessionBatch;
import org.apache.paimon.agent.model.SessionKey;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Owner-private write-ahead log for the exact batch submitted to Paimon. */
public final class PendingBatchStore {

    private static final int MAGIC = 0x5041494d;
    private static final int VERSION = 3;
    private static final int MIN_SUPPORTED_VERSION = 2;
    private static final int MAX_COLLECTION_ENTRIES = 10_000_000;
    private static final int CHECKSUM_BYTES = 32;

    private final Path directory;
    private final Path pendingFile;
    private final String collectorId;
    private final String tablePairIdentity;

    public PendingBatchStore(
            Path directory, String collectorId, String tablePairIdentity) {
        this.directory = directory.toAbsolutePath().normalize();
        this.pendingFile = this.directory.resolve("pending-commit.bin");
        this.collectorId = collectorId;
        this.tablePairIdentity = tablePairIdentity;
    }

    StoredCommit load() throws IOException {
        if (!Files.exists(pendingFile)) {
            return null;
        }
        if (Files.isSymbolicLink(pendingFile) || !Files.isRegularFile(pendingFile)) {
            throw new IOException("Invalid pending batch WAL: " + pendingFile);
        }
        long fileSize = Files.size(pendingFile);
        long payloadSize = verifyChecksum(pendingFile, fileSize);
        try (DataInputStream input =
                new DataInputStream(
                        new BufferedInputStream(Files.newInputStream(pendingFile)))) {
            if (input.readInt() != MAGIC) {
                throw new IOException("Invalid pending batch WAL magic: " + pendingFile);
            }
            int version = input.readInt();
            if (version < MIN_SUPPORTED_VERSION || version > VERSION) {
                throw new IOException(
                        "Unsupported pending batch WAL version " + version + ": " + pendingFile);
            }
            String storedCollector = readString(input, payloadSize);
            if (!collectorId.equals(storedCollector)) {
                throw new IOException(
                        "Pending batch WAL belongs to collector.id="
                                + storedCollector
                                + ", not "
                                + collectorId);
            }
            String storedTablePair = readString(input, payloadSize);
            if (!tablePairIdentity.equals(storedTablePair)) {
                throw new IOException(
                        "Pending batch WAL belongs to a different Catalog table pair");
            }
            long identifier = input.readLong();
            int batchCount = readCount(input, "batch", payloadSize);
            List<SessionBatch> batches = new ArrayList<>(batchCount);
            for (int index = 0; index < batchCount; index++) {
                batches.add(readBatch(input, payloadSize, version));
            }
            byte[] checksum = new byte[CHECKSUM_BYTES];
            input.readFully(checksum);
            if (input.read() >= 0) {
                throw new IOException("Pending batch WAL has trailing data: " + pendingFile);
            }
            return new StoredCommit(identifier, batches);
        } catch (EOFException e) {
            throw new IOException("Pending batch WAL is truncated: " + pendingFile, e);
        }
    }

    void save(long identifier, List<SessionBatch> batches) throws IOException {
        StoredCommit existing = load();
        if (existing != null) {
            if (existing.identifier() != identifier) {
                throw new IOException(
                        "Pending batch WAL contains commit "
                                + existing.identifier()
                                + " while trying to persist "
                                + identifier);
            }
            return;
        }

        Files.createDirectories(directory);
        setOwnerOnlyDirectoryPermissions(directory);
        if (Files.isSymbolicLink(directory)) {
            throw new IOException("Refusing symbolic-link WAL directory: " + directory);
        }
        Path temporary = Files.createTempFile(directory, ".pending-commit-", ".tmp");
        boolean installed = false;
        try {
            setOwnerOnlyFilePermissions(temporary);
            try (DataOutputStream output =
                    new DataOutputStream(
                            new BufferedOutputStream(
                                    Files.newOutputStream(
                                            temporary, StandardOpenOption.TRUNCATE_EXISTING)))) {
                output.writeInt(MAGIC);
                output.writeInt(VERSION);
                writeString(output, collectorId);
                writeString(output, tablePairIdentity);
                output.writeLong(identifier);
                output.writeInt(batches.size());
                for (SessionBatch batch : batches) {
                    writeBatch(output, batch);
                }
            }
            Files.write(
                    temporary,
                    sha256Prefix(temporary, Files.size(temporary)),
                    StandardOpenOption.APPEND);
            try (FileChannel channel =
                    FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            try {
                Files.move(temporary, pendingFile, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, pendingFile);
            }
            setOwnerOnlyFilePermissions(pendingFile);
            forceDirectory(directory);
            installed = true;
        } finally {
            Files.deleteIfExists(temporary);
            if (!installed && Files.isSymbolicLink(pendingFile)) {
                throw new IOException("Refusing symbolic-link WAL file: " + pendingFile);
            }
        }
    }

    void delete(long identifier) throws IOException {
        StoredCommit existing = load();
        if (existing == null) {
            return;
        }
        if (existing.identifier() != identifier) {
            throw new IOException(
                    "Refusing to delete pending commit "
                            + existing.identifier()
                            + " while completing "
                            + identifier);
        }
        Files.delete(pendingFile);
        forceDirectory(directory);
    }

    Path pendingFile() {
        return pendingFile;
    }

    private static void writeBatch(DataOutputStream output, SessionBatch batch)
            throws IOException {
        writeSession(output, batch.session());
        output.writeInt(batch.messages().size());
        for (ChatMessage message : batch.messages()) {
            writeMessage(output, message);
        }
        output.writeInt(batch.sourceRecordsRead());
        writeString(output, batch.startingCursor());
        output.writeLong(batch.startingCommitId());
    }

    private static SessionBatch readBatch(DataInputStream input, long fileSize, int version)
            throws IOException {
        ChatSession session = readSession(input, fileSize, version);
        int messageCount = readCount(input, "message", fileSize);
        List<ChatMessage> messages = new ArrayList<>(messageCount);
        for (int index = 0; index < messageCount; index++) {
            messages.add(readMessage(input, fileSize));
        }
        int sourceRecordsRead = input.readInt();
        String startingCursor = readString(input, fileSize);
        long startingCommitId = input.readLong();
        return new SessionBatch(
                session,
                messages,
                sourceRecordsRead,
                startingCursor,
                startingCommitId);
    }

    private static void writeSession(DataOutputStream output, ChatSession session)
            throws IOException {
        writeString(output, session.key().sourceType());
        writeString(output, session.key().sessionId());
        writeString(output, session.title());
        writeString(output, session.cwd());
        output.writeBoolean(session.archived());
        writeString(output, session.sourcePath());
        writeString(output, session.sourceCursor());
        output.writeLong(session.lastCommitId());
        output.writeBoolean(session.pendingCommitId() != null);
        if (session.pendingCommitId() != null) {
            output.writeLong(session.pendingCommitId());
        }
        writeString(output, session.pendingCursor());
        writeInstant(output, session.createdAt());
        writeInstant(output, session.updatedAt());
        writeInstant(output, session.lastMessageAt());
        writeInstant(output, session.ingestedAt());
        writeString(output, session.subagentSourceJson());
    }

    private static ChatSession readSession(DataInputStream input, long fileSize, int version)
            throws IOException {
        SessionKey key =
                new SessionKey(readString(input, fileSize), readString(input, fileSize));
        String title = readString(input, fileSize);
        String cwd = readString(input, fileSize);
        boolean archived = input.readBoolean();
        String sourcePath = readString(input, fileSize);
        String sourceCursor = readString(input, fileSize);
        long lastCommitId = input.readLong();
        Long pendingCommitId = input.readBoolean() ? input.readLong() : null;
        String pendingCursor = readString(input, fileSize);
        Instant createdAt = readInstant(input);
        Instant updatedAt = readInstant(input);
        Instant lastMessageAt = readInstant(input);
        Instant ingestedAt = readInstant(input);
        String subagentSourceJson =
                version >= 3 ? readString(input, fileSize) : null;
        return new ChatSession(
                key,
                title,
                cwd,
                archived,
                sourcePath,
                sourceCursor,
                lastCommitId,
                pendingCommitId,
                pendingCursor,
                createdAt,
                updatedAt,
                lastMessageAt,
                ingestedAt,
                subagentSourceJson);
    }

    private static void writeMessage(DataOutputStream output, ChatMessage message)
            throws IOException {
        writeString(output, message.messageId());
        writeString(output, message.sessionKey().sourceType());
        writeString(output, message.sessionKey().sessionId());
        output.writeLong(message.sequenceNumber());
        writeString(output, message.role());
        writeString(output, message.eventType());
        writeString(output, message.contentJson());
        output.writeInt(message.attachments().size());
        for (AttachmentPayload attachment : message.attachments()) {
            byte[] bytes = attachment.bytes();
            if (bytes == null) {
                output.writeInt(-1);
            } else {
                output.writeInt(bytes.length);
                output.write(bytes);
            }
        }
        writeInstant(output, message.createdAt());
        writeInstant(output, message.ingestedAt());
    }

    private static ChatMessage readMessage(DataInputStream input, long fileSize)
            throws IOException {
        String messageId = readString(input, fileSize);
        SessionKey key =
                new SessionKey(readString(input, fileSize), readString(input, fileSize));
        long sequenceNumber = input.readLong();
        String role = readString(input, fileSize);
        String eventType = readString(input, fileSize);
        String contentJson = readString(input, fileSize);
        int attachmentCount = readCount(input, "attachment", fileSize);
        List<AttachmentPayload> attachments = new ArrayList<>(attachmentCount);
        for (int index = 0; index < attachmentCount; index++) {
            int length = input.readInt();
            if (length < -1 || length > fileSize) {
                throw new IOException("Invalid attachment length in pending batch WAL: " + length);
            }
            if (length < 0) {
                attachments.add(AttachmentPayload.missing());
            } else {
                byte[] bytes = new byte[length];
                input.readFully(bytes);
                attachments.add(AttachmentPayload.of(bytes));
            }
        }
        Instant createdAt = readInstant(input);
        Instant ingestedAt = readInstant(input);
        if (ingestedAt == null) {
            throw new IOException("Pending batch WAL message is missing ingested_at");
        }
        return new ChatMessage(
                messageId,
                key,
                sequenceNumber,
                role,
                eventType,
                contentJson,
                attachments,
                createdAt,
                ingestedAt);
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        if (value == null) {
            output.writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(DataInputStream input, long fileSize) throws IOException {
        int length = input.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > fileSize) {
            throw new IOException("Invalid string length in pending batch WAL: " + length);
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeInstant(DataOutputStream output, Instant value) throws IOException {
        output.writeBoolean(value != null);
        if (value != null) {
            output.writeLong(value.toEpochMilli());
        }
    }

    private static Instant readInstant(DataInputStream input) throws IOException {
        return input.readBoolean() ? Instant.ofEpochMilli(input.readLong()) : null;
    }

    private static int readCount(
            DataInputStream input, String description, long fileSize) throws IOException {
        int count = input.readInt();
        long fileBound = Math.max(0L, fileSize / Integer.BYTES);
        if (count < 0 || count > MAX_COLLECTION_ENTRIES || count > fileBound) {
            throw new IOException("Invalid " + description + " count in pending batch WAL: " + count);
        }
        return count;
    }

    private static long verifyChecksum(Path file, long fileSize) throws IOException {
        if (fileSize <= CHECKSUM_BYTES) {
            throw new IOException("Pending batch WAL is too short to contain a checksum: " + file);
        }
        long payloadSize = fileSize - CHECKSUM_BYTES;
        byte[] actual = sha256Prefix(file, payloadSize);
        byte[] expected = new byte[CHECKSUM_BYTES];
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            channel.position(payloadSize);
            ByteBuffer buffer = ByteBuffer.wrap(expected);
            while (buffer.hasRemaining()) {
                if (channel.read(buffer) < 0) {
                    throw new IOException("Pending batch WAL checksum is truncated: " + file);
                }
            }
        }
        if (!MessageDigest.isEqual(actual, expected)) {
            throw new IOException("Pending batch WAL checksum mismatch: " + file);
        }
        return payloadSize;
    }

    private static byte[] sha256Prefix(Path file, long length) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
        try (InputStream input = new BufferedInputStream(Files.newInputStream(file))) {
            byte[] buffer = new byte[8192];
            long remaining = length;
            while (remaining > 0) {
                int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read < 0) {
                    throw new IOException("Pending batch WAL is truncated: " + file);
                }
                digest.update(buffer, 0, read);
                remaining -= read;
            }
        }
        return digest.digest();
    }

    private static void forceDirectory(Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Directory fsync is not exposed by every Java file-system provider.
        }
    }

    private static void setOwnerOnlyDirectoryPermissions(Path directory) {
        setPermissions(
                directory,
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE);
    }

    private static void setOwnerOnlyFilePermissions(Path file) {
        setPermissions(file, PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
    }

    private static void setPermissions(Path path, PosixFilePermission... permissions) {
        Set<PosixFilePermission> values = new HashSet<>(Arrays.asList(permissions));
        try {
            Files.setPosixFilePermissions(path, values);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Best effort on non-POSIX file systems.
        }
    }

    static final class StoredCommit {
        private final long identifier;
        private final List<SessionBatch> batches;

        private StoredCommit(long identifier, List<SessionBatch> batches) {
            this.identifier = identifier;
            this.batches = Collections.unmodifiableList(new ArrayList<>(batches));
        }

        long identifier() {
            return identifier;
        }

        List<SessionBatch> batches() {
            return batches;
        }
    }
}
