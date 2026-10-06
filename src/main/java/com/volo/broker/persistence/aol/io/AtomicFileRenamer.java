package com.volo.broker.persistence.aol.io;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Utility for performing atomic file rename operations.
 */
public final class AtomicFileRenamer {

    private AtomicFileRenamer() {}

    /**
     * Atomically renames {@code source} to {@code target}.
     * If {@code target} already exists it is replaced atomically.
     *
     * @param source the file to rename; must exist
     * @param target the desired name; parent directory must exist
     * @throws IOException                      if the rename fails for any reason
     * @throws AtomicMoveNotSupportedException  if the filesystem does not
     *                                          support atomic moves between these paths
     */
    public static void rename(Path source, Path target) throws IOException {
        Files.move(source, target,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Convenience overload that accepts string paths.
     *
     * @see #rename(Path, Path)
     */
    public static void rename(String source, String target) throws IOException {
        rename(Path.of(source), Path.of(target));
    }
}
