package com.volo.broker.persistence.aol.engine;

import com.volo.broker.persistence.aol.io.AolFile;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes {@code file.force()} sequentially on the calling thread for all dirty files.
 */
public final class SequentialFsyncStrategy implements FsyncStrategy {
    private final Logger logger = LoggerFactory.getLogger(SequentialFsyncStrategy.class);
    private final AtomicLong forceCount = new AtomicLong(0);

    @Override
    public void force(Set<AolFile> dirtyFiles) throws IOException {
        if (dirtyFiles == null || dirtyFiles.isEmpty()) {
            return;
        }
        for (AolFile file : dirtyFiles) {
            file.force();
            forceCount.incrementAndGet();
        }
        logger.info("Forced sync on {} files (total forces: {})", dirtyFiles.size(), forceCount.get());
    }

    @Override
    public long getForceCount() {
        return forceCount.get();
    }
}

