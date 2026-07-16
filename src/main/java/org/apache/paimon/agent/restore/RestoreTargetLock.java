package org.apache.paimon.agent.restore;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

/** Cross-process lock preventing concurrent paimon-agent restores into one client home. */
final class RestoreTargetLock implements AutoCloseable {

    private static final String LOCK_DIRECTORY_PREFIX = ".paimon-agent-restore-lock-";

    private final FileChannel channel;
    private final FileLock lock;

    private RestoreTargetLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    static RestoreTargetLock acquire(RestoreOptions options) throws IOException {
        Path target = options.target().toAbsolutePath().normalize();
        if (Files.isSymbolicLink(target)) {
            throw new IOException("Restore target home must not be a symbolic link: " + target);
        }
        Path parent = target.getParent();
        if (parent == null) {
            throw new IOException("Restore target home has no parent directory: " + target);
        }
        if (!Files.isDirectory(parent)) {
            if (options.type() != RestoreType.CLAUDE) {
                throw new IOException("Restore target parent is not a directory: " + parent);
            }
            Files.createDirectories(parent);
        }
        Path canonicalParent = parent.toRealPath();
        Path canonicalTarget =
                Files.exists(target, LinkOption.NOFOLLOW_LINKS)
                        ? target.toRealPath()
                        : canonicalParent.resolve(target.getFileName()).normalize();
        // Serializing case variants is conservative on case-sensitive file systems and closes
        // the first-create race on the default case-insensitive macOS/Windows file systems.
        String targetIdentity = sha256(canonicalTarget.toString().toLowerCase(Locale.ROOT));
        Path lockDirectory = canonicalParent.resolve(LOCK_DIRECTORY_PREFIX + targetIdentity);
        if (Files.isSymbolicLink(lockDirectory)) {
            throw new IOException("Restore lock directory must not be a symbolic link: " + lockDirectory);
        }
        try {
            Files.createDirectory(lockDirectory);
        } catch (java.nio.file.FileAlreadyExistsException exists) {
            // Validate the existing entry below without following a symbolic link.
        }
        if (!Files.isDirectory(lockDirectory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Restore lock path is not a directory: " + lockDirectory);
        }
        lockDirectory = lockDirectory.toRealPath();
        if (!canonicalParent.equals(lockDirectory.getParent())) {
            throw new IOException("Restore lock directory escapes target parent: " + lockDirectory);
        }
        RestoreFiles.setOwnerOnlyDirectoryPermissions(lockDirectory);
        Path lockPath =
                RestoreFiles.resolveContainedFile(
                        lockDirectory, lockDirectory.getFileSystem().getPath("restore.lock"));
        FileChannel channel =
                FileChannel.open(
                        lockPath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        LinkOption.NOFOLLOW_LINKS);
        RestoreFiles.setOwnerOnlyFilePermissions(lockPath);
        try {
            FileLock lock = channel.tryLock();
            if (lock == null) {
                channel.close();
                throw new RestoreTargetBusyException(
                        "Another restore is already writing " + target, null);
            }
            return new RestoreTargetLock(channel, lock);
        } catch (OverlappingFileLockException busy) {
            channel.close();
            throw new RestoreTargetBusyException(
                    "Another restore is already writing " + target, busy);
        } catch (IOException | RuntimeException failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static String sha256(String value) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte item : digest) {
                result.append(String.format("%02x", item & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
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
