package com.volo.broker.persistence.aol.engine;

/**
 * Defines the fsync strategy used when committing a batch of files in the AOL storage engine.
 */
public enum FsyncMode {
    /**
     * Executes {@code force()} sequentially on the single writer thread, one file after another.
     */
    SEQUENTIAL,

    /**
     * Executes {@code force()} concurrently across all dirty files in the batch using virtual threads.
     */
    PARALLEL
}

