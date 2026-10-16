package com.volo.broker.persistence.aol.engine;

import com.volo.broker.persistence.aol.io.AolFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

/**
 * Strategy interface for executing {@code force()} on all dirty files accumulated during a batch.
 */
public interface FsyncStrategy extends Closeable {

    /**
     * Forces all modified files to underlying storage.
     *
     * @param dirtyFiles the set of files modified during the current batch
     * @throws IOException if any file fails to force
     */
    void force(Set<AolFile> dirtyFiles) throws IOException;

    /**
     * Returns total number of physical file force() calls performed by this strategy.
     */
    default long getForceCount() {
        return 0L;
    }

    @Override
    default void close() throws IOException {}
}

