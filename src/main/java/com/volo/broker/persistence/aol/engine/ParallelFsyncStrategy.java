package com.volo.broker.persistence.aol.engine;

import com.volo.broker.persistence.aol.io.AolFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Executes {@code file.force()} concurrently across all dirty files using virtual threads or an executor.
 */
public final class ParallelFsyncStrategy implements FsyncStrategy {
    private final AtomicLong forceCount = new AtomicLong(0);

    private final ExecutorService executor;
    private final boolean ownsExecutor;

    public ParallelFsyncStrategy() {
        this(Executors.newVirtualThreadPerTaskExecutor(), true);
    }

    public ParallelFsyncStrategy(ExecutorService executor) {
        this(executor, false);
    }

    private ParallelFsyncStrategy(ExecutorService executor, boolean ownsExecutor) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.ownsExecutor = ownsExecutor;
    }

    @Override
    public void force(Set<AolFile> dirtyFiles) throws IOException {
        if (dirtyFiles == null || dirtyFiles.isEmpty()) {
            return;
        }
        if (dirtyFiles.size() == 1) {
            dirtyFiles.iterator().next().force();
            forceCount.incrementAndGet();
            return;
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>(dirtyFiles.size());
        for (AolFile file : dirtyFiles) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    file.force();
                    forceCount.incrementAndGet();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, executor));
        }

        try {
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        } catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            if (cause instanceof UncheckedIOException uio) {
                throw uio.getCause();
            }
            if (cause instanceof IOException ioe) {
                throw ioe;
            }
            throw new IOException(cause);
        }
    }

    @Override
    public long getForceCount() {
        return forceCount.get();
    }

    @Override
    public void close() {
        if (ownsExecutor) {
            executor.close();
        }
    }
}

