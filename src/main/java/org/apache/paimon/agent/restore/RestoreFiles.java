package org.apache.paimon.agent.restore;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Atomic output helpers shared by native format restorers. */
final class RestoreFiles {

    private RestoreFiles() {}

    static Path canonicalTargetDirectory(Path requested, boolean create, String description)
            throws IOException {
        Path absolute = requested.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(absolute)) {
            throw new IOException(description + " must not be a symbolic link: " + absolute);
        }
        if (create) {
            Files.createDirectories(absolute);
        }
        if (!Files.isDirectory(absolute, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException(description + " is not a directory: " + absolute);
        }
        Path canonical = absolute.toRealPath();
        setOwnerOnlyDirectoryPermissions(canonical);
        return canonical;
    }

    static Path ensureContainedDirectory(Path canonicalRoot, Path relative) throws IOException {
        if (relative.isAbsolute()) {
            throw new IOException("Expected a relative restore directory, but found " + relative);
        }
        Path root = canonicalRoot.toRealPath();
        Path current = root;
        for (Path component : relative.normalize()) {
            String value = component.toString();
            if (value.isEmpty() || ".".equals(value) || "..".equals(value)) {
                throw new IOException("Unsafe restore directory component " + relative);
            }
            current = current.resolve(component);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                if (Files.isSymbolicLink(current)) {
                    throw new IOException(
                            "Restore output directory must not be a symbolic link: " + current);
                }
                if (!Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Restore output path is not a directory: " + current);
                }
            } else {
                try {
                    Files.createDirectory(current);
                } catch (java.nio.file.FileAlreadyExistsException race) {
                    if (Files.isSymbolicLink(current)
                            || !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                        throw new IOException(
                                "Unsafe restore output directory appeared concurrently: "
                                        + current,
                                race);
                    }
                }
            }
            Path canonical = current.toRealPath();
            if (!canonical.startsWith(root)) {
                throw new IOException(
                        "Restore output directory escapes target " + root + ": " + canonical);
            }
            current = canonical;
            setOwnerOnlyDirectoryPermissions(current);
        }
        return current;
    }

    static Path resolveContainedFile(Path canonicalRoot, Path relative) throws IOException {
        if (relative.isAbsolute() || relative.getFileName() == null) {
            throw new IOException("Expected a relative restore file, but found " + relative);
        }
        Path normalized = relative.normalize();
        if (normalized.startsWith("..")) {
            throw new IOException("Unsafe restore file path " + relative);
        }
        Path parentRelative = normalized.getParent();
        Path parent =
                parentRelative == null
                        ? canonicalRoot.toRealPath()
                        : ensureContainedDirectory(canonicalRoot, parentRelative);
        Path output = parent.resolve(normalized.getFileName()).normalize();
        if (!output.getParent().equals(parent) || !output.startsWith(canonicalRoot.toRealPath())) {
            throw new IOException("Restore output file escapes target: " + output);
        }
        if (Files.isSymbolicLink(output)) {
            throw new IOException("Restore output file must not be a symbolic link: " + output);
        }
        return output;
    }

    static void writeLinesAtomically(Path output, List<String> lines) throws IOException {
        Path parent = output.getParent();
        if (parent == null
                || Files.isSymbolicLink(parent)
                || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Unsafe or missing restore output directory: " + parent);
        }
        Path temporary = Files.createTempFile(parent, ".restore-", ".tmp");
        try {
            try (BufferedWriter writer = Files.newBufferedWriter(temporary, StandardCharsets.UTF_8)) {
                for (String line : lines) {
                    writer.write(line);
                    writer.newLine();
                }
            }
            try (FileChannel channel =
                    FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            setOwnerOnlyFilePermissions(temporary);
            try {
                Files.move(
                        temporary,
                        output,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, output, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    static void setOwnerOnlyDirectoryPermissions(Path directory) {
        try {
            Set<PosixFilePermission> permissions =
                    new HashSet<>(
                            Arrays.asList(
                                    PosixFilePermission.OWNER_READ,
                                    PosixFilePermission.OWNER_WRITE,
                                    PosixFilePermission.OWNER_EXECUTE));
            Files.setPosixFilePermissions(directory, permissions);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Windows and some object-backed file systems do not expose POSIX permissions.
        }
    }

    static void setOwnerOnlyFilePermissions(Path file) {
        try {
            Set<PosixFilePermission> permissions =
                    new HashSet<>(
                            Arrays.asList(
                                    PosixFilePermission.OWNER_READ,
                                    PosixFilePermission.OWNER_WRITE));
            Files.setPosixFilePermissions(file, permissions);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Best effort only.
        }
    }

    static List<Path> installStagedAttachments(Path stagingDirectory, Path targetDirectory)
            throws IOException {
        if (!Files.isDirectory(stagingDirectory)) {
            return new ArrayList<>();
        }
        if (Files.isSymbolicLink(targetDirectory)) {
            throw new IOException(
                    "Restored attachment directory must not be a symbolic link: "
                            + targetDirectory);
        }
        Files.createDirectories(targetDirectory);
        if (!Files.isDirectory(targetDirectory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException(
                    "Restored attachment path is not a directory: " + targetDirectory);
        }
        setOwnerOnlyDirectoryPermissions(targetDirectory);
        List<Path> installed = new ArrayList<>();
        try {
            List<Path> stagedFiles;
            try (Stream<Path> files = Files.list(stagingDirectory)) {
                stagedFiles =
                        files.filter(Files::isRegularFile)
                                .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                                .collect(Collectors.toList());
            }
            for (Path staged : stagedFiles) {
                Path output = targetDirectory.resolve(staged.getFileName()).normalize();
                if (!output.startsWith(targetDirectory.normalize())) {
                    throw new IOException("Unsafe attachment installation path " + output);
                }
                if (Files.exists(output)) {
                    requireSameContents(staged, output);
                    continue;
                }

                Path temporary = Files.createTempFile(targetDirectory, ".attachment-", ".tmp");
                boolean reserved = false;
                try {
                    Files.copy(staged, temporary, StandardCopyOption.REPLACE_EXISTING);
                    try (FileChannel channel =
                            FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                        channel.force(true);
                    }
                    setOwnerOnlyFilePermissions(temporary);
                    try {
                        Files.createFile(output);
                        reserved = true;
                        setOwnerOnlyFilePermissions(output);
                    } catch (java.nio.file.FileAlreadyExistsException race) {
                        requireSameContents(staged, output);
                        continue;
                    }
                    try {
                        Files.move(
                                temporary,
                                output,
                                StandardCopyOption.REPLACE_EXISTING,
                                StandardCopyOption.ATOMIC_MOVE);
                    } catch (AtomicMoveNotSupportedException ignored) {
                        Files.move(temporary, output, StandardCopyOption.REPLACE_EXISTING);
                    }
                    setOwnerOnlyFilePermissions(output);
                    installed.add(output);
                    reserved = false;
                } finally {
                    Files.deleteIfExists(temporary);
                    if (reserved) {
                        Files.deleteIfExists(output);
                    }
                }
            }
            return installed;
        } catch (IOException failure) {
            deleteInstalledAttachments(installed, failure);
            throw failure;
        }
    }

    static void deleteInstalledAttachments(List<Path> installed, Exception failure) {
        for (int index = installed.size() - 1; index >= 0; index--) {
            try {
                Files.deleteIfExists(installed.get(index));
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    private static void requireSameContents(Path expected, Path actual) throws IOException {
        if (!Files.isRegularFile(actual)
                || Files.size(expected) != Files.size(actual)
                || !sameContents(expected, actual)) {
            throw new IOException(
                    "Refusing to replace an existing attachment with different content: "
                            + actual);
        }
    }

    private static boolean sameContents(Path first, Path second) throws IOException {
        try (BufferedInputStream left =
                        new BufferedInputStream(Files.newInputStream(first));
                BufferedInputStream right =
                        new BufferedInputStream(Files.newInputStream(second))) {
            byte[] leftBuffer = new byte[8192];
            byte[] rightBuffer = new byte[8192];
            int leftCount;
            while ((leftCount = left.read(leftBuffer)) >= 0) {
                int rightCount = 0;
                while (rightCount < leftCount) {
                    int read = right.read(rightBuffer, rightCount, leftCount - rightCount);
                    if (read < 0) {
                        return false;
                    }
                    rightCount += read;
                }
                for (int index = 0; index < leftCount; index++) {
                    if (leftBuffer[index] != rightBuffer[index]) {
                        return false;
                    }
                }
            }
            return right.read() < 0;
        }
    }
}
