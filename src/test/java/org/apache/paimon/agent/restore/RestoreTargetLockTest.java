package org.apache.paimon.agent.restore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestoreTargetLockTest {

    @TempDir Path tempDir;

    @Test
    void serializesTheSameTargetWithoutCreatingItAndReleasesAfterClose() throws Exception {
        Path target = tempDir.resolve("codex-home");
        Set<PosixFilePermission> originalPermissions =
                EnumSet.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE,
                        PosixFilePermission.GROUP_READ,
                        PosixFilePermission.GROUP_EXECUTE);
        Files.setPosixFilePermissions(tempDir, originalPermissions);
        RestoreOptions options =
                new RestoreOptions(
                        RestoreType.CODEX,
                        target,
                        tempDir.resolve("data"),
                        null,
                        "session",
                        false);

        try (RestoreTargetLock ignored = RestoreTargetLock.acquire(options)) {
            assertThat(target).doesNotExist();
            assertThatThrownBy(() -> RestoreTargetLock.acquire(options))
                    .isInstanceOf(RestoreTargetBusyException.class)
                    .hasMessageContaining(target.toAbsolutePath().toString());
        }

        assertThat(Files.getPosixFilePermissions(tempDir)).isEqualTo(originalPermissions);
        assertThatCode(
                        () -> {
                            try (RestoreTargetLock ignored =
                                    RestoreTargetLock.acquire(options)) {
                                // The previous owner released the cross-process lock.
                            }
                        })
                .doesNotThrowAnyException();
    }

    @Test
    void canonicalizesAParentDirectoryAliasToTheSamePhysicalLock() throws Exception {
        Path realParent = Files.createDirectory(tempDir.resolve("real-parent"));
        Path aliasParent = tempDir.resolve("alias-parent");
        Files.createSymbolicLink(aliasParent, realParent);
        RestoreOptions real = options(realParent.resolve("codex-home"));
        RestoreOptions alias = options(aliasParent.resolve("codex-home"));

        try (RestoreTargetLock ignored = RestoreTargetLock.acquire(real)) {
            assertThatThrownBy(() -> RestoreTargetLock.acquire(alias))
                    .isInstanceOf(RestoreTargetBusyException.class);
        }
    }

    @Test
    void serializesCaseVariantsBeforeTheTargetExists() throws Exception {
        RestoreOptions lower = options(tempDir.resolve(".claude"));
        RestoreOptions upper = options(tempDir.resolve(".CLAUDE"));

        try (RestoreTargetLock ignored = RestoreTargetLock.acquire(lower)) {
            assertThatThrownBy(() -> RestoreTargetLock.acquire(upper))
                    .isInstanceOf(RestoreTargetBusyException.class);
        }
    }

    private RestoreOptions options(Path target) {
        return new RestoreOptions(
                RestoreType.CODEX,
                target,
                tempDir.resolve("data"),
                null,
                "session",
                false);
    }
}
