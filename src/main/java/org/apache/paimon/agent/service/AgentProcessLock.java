package org.apache.paimon.agent.service;

import org.apache.paimon.agent.config.AgentConfiguration;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Prevents two local processes from writing the same Catalog table pair. */
public final class AgentProcessLock implements AutoCloseable {

    private final FileChannel channel;
    private final FileLock lock;

    private AgentProcessLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    public static AgentProcessLock acquire(
            AgentConfiguration configuration, Path projectConfig) throws IOException {
        return acquire(defaultLockDirectory(), configuration, projectConfig);
    }

    static AgentProcessLock acquire(
            Path lockDirectory, AgentConfiguration configuration, Path projectConfig)
            throws IOException {
        Path directory = lockDirectory.toAbsolutePath().normalize();
        Files.createDirectories(directory);
        setOwnerOnlyDirectoryPermissions(directory);
        Path lockPath = directory.resolve(tablePairIdentity(configuration) + ".lock");
        Path absoluteConfig = projectConfig.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(lockPath)) {
            throw new IOException("Refusing symbolic-link collector lock: " + lockPath);
        }
        FileChannel channel =
                FileChannel.open(
                        lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        setOwnerOnlyFilePermissions(lockPath);
        try {
            FileLock lock = channel.tryLock();
            if (lock == null) {
                channel.close();
                throw new IllegalStateException(
                        "Another local collector is already writing the table pair configured by "
                                + absoluteConfig);
            }
            return new AgentProcessLock(channel, lock);
        } catch (OverlappingFileLockException e) {
            channel.close();
            throw new IllegalStateException(
                    "Another local collector is already writing the table pair configured by "
                            + absoluteConfig,
                    e);
        }
    }

    private static Path defaultLockDirectory() {
        return Paths.get(System.getProperty("user.home"), ".paimon-agent", "locks");
    }

    /** Stable, credential-free identity for one REST Catalog table pair. */
    public static String tablePairIdentity(AgentConfiguration configuration) {
        Map<String, String> catalog = configuration.catalogOptions();
        String identity =
                value(catalog, "metastore")
                        + '\n'
                        + value(catalog, "uri")
                        + '\n'
                        + value(catalog, "warehouse")
                        + '\n'
                        + configuration.project().database()
                        + '\n'
                        + configuration.project().sessionsTable()
                        + '\n'
                        + configuration.project().messagesTable();
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte item : digest) {
                result.append(String.format("%02x", item & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String value(Map<String, String> options, String key) {
        String value = options.get(key);
        return value == null ? "" : value.trim();
    }

    private static void setOwnerOnlyDirectoryPermissions(Path path) {
        setPermissions(
                path,
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE);
    }

    private static void setOwnerOnlyFilePermissions(Path path) {
        setPermissions(
                path, PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
    }

    private static void setPermissions(Path path, PosixFilePermission... permissions) {
        Set<PosixFilePermission> values = new HashSet<>(Arrays.asList(permissions));
        try {
            Files.setPosixFilePermissions(path, values);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Best effort on non-POSIX file systems.
        }
    }

    @Override
    public void close() throws IOException {
        try {
            lock.release();
        } finally {
            channel.close();
        }
    }
}
