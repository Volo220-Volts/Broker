package com.volo.broker.persistence.aol.engine;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.WriteResult;
import com.volo.broker.persistence.aol.codec.DataRecordCodec;
import com.volo.broker.persistence.aol.io.AolFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-writer AOL engine coordinating all append and acknowledge operations across all queues
 * using a lock-free {@link MpscQueue} (Multi-Producer Single-Consumer) without Disruptor.
 *
 * <p>Producers enqueue tasks in a wait-free manner without lock contention or buffer-full blocking.
 * A single background writer thread drains micro-batches of tasks, writes them into their respective
 * queue files, and performs coordinated {@code force()} calls across all modified files.
 *
 * <p>Supports runtime switching between {@link FsyncMode#SEQUENTIAL} and {@link FsyncMode#PARALLEL}.
 */
public final class AolMpscEngine implements AolStorageEngine {

    public static final int DEFAULT_MAX_BATCH_SIZE = 256;
    public static final long DEFAULT_LINGER_MICROS = 1500L;
    public static final int DEFAULT_MAX_ACK_BATCH_SIZE = 256;
    public static final long DEFAULT_ACK_LINGER_MICROS = 5_000L;

    public static final class QueueState {
        private final AolFile dataFile;
        private final AolFile ackedFile;
        private long nextOffset;

        public QueueState(AolFile dataFile, AolFile ackedFile, long nextOffset) {
            this.dataFile = Objects.requireNonNull(dataFile, "dataFile");
            this.ackedFile = Objects.requireNonNull(ackedFile, "ackedFile");
            this.nextOffset = nextOffset;
        }

        public AolFile dataFile() { return dataFile; }
        public AolFile ackedFile() { return ackedFile; }
        public long nextOffset() { return nextOffset; }
    }

    public sealed interface AolTask permits AolTask.DataTask, AolTask.AckTask {
        String queuePath();

        record DataTask(String queuePath, QueuedMessage message, CompletableFuture<WriteResult> future) implements AolTask {}
        record AckTask(String queuePath, long ackOffset, CompletableFuture<Void> future) implements AolTask {}
    }

    private record PendingWrite(CompletableFuture<WriteResult> future, long offset, long physPos) {}

    private final ConcurrentHashMap<String, QueueState> queues = new ConcurrentHashMap<>();
    private final MpscQueue<AolTask> queue;
    private final DataRecordCodec codec;
    private final Thread writerThread;

    private volatile FsyncStrategy fsyncStrategy;
    private volatile FsyncMode fsyncMode;
    private volatile Throwable failure = null;
    private volatile boolean running = true;

    private volatile int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    private volatile long lingerMicros = DEFAULT_LINGER_MICROS;
    private volatile int maxAckBatchSize = DEFAULT_MAX_ACK_BATCH_SIZE;
    private volatile long ackLingerMicros = DEFAULT_ACK_LINGER_MICROS;

    private final AtomicLong dataForceCount = new AtomicLong();
    private final AtomicLong ackForceCount = new AtomicLong();

    public AolMpscEngine(DataRecordCodec codec) {
        this(codec, FsyncMode.PARALLEL);
    }

    public AolMpscEngine(DataRecordCodec codec, FsyncMode fsyncMode) {
        this.codec = Objects.requireNonNull(codec, "codec");
        this.fsyncMode = Objects.requireNonNull(fsyncMode, "fsyncMode");
        this.fsyncStrategy = createStrategy(fsyncMode);
        this.queue = new MpscQueue<>();

        this.writerThread = new Thread(this::writerLoop, "aol-mpsc-writer");
        this.writerThread.setDaemon(true);
        this.queue.setConsumerThread(this.writerThread);
        this.writerThread.start();
    }

    @Override
    public void registerQueue(String queuePath, AolFile dataFile, AolFile ackedFile, long nextOffset) {
        Objects.requireNonNull(queuePath, "queuePath");
        queues.put(queuePath, new QueueState(dataFile, ackedFile, nextOffset));
    }

    public QueueState getQueueState(String queuePath) {
        return queues.get(queuePath);
    }

    @Override
    public synchronized void setFsyncMode(FsyncMode mode) {
        Objects.requireNonNull(mode, "mode");
        if (this.fsyncMode == mode) {
            return;
        }

        FsyncStrategy old = this.fsyncStrategy;
        FsyncStrategy next = createStrategy(mode);

        this.fsyncStrategy = next;
        this.fsyncMode = mode;

        if (old != null) {
            try {
                old.close();
            } catch (Exception ignored) {}
        }
    }

    @Override
    public FsyncMode getFsyncMode() {
        return this.fsyncMode;
    }

    @Override
    public void setBatchConfig(int maxBatchSize, long lingerMicros) {
        if (maxBatchSize <= 0) throw new IllegalArgumentException("maxBatchSize must be > 0");
        if (lingerMicros < 0) throw new IllegalArgumentException("lingerMicros must be >= 0");
        this.maxBatchSize = maxBatchSize;
        this.lingerMicros = lingerMicros;
    }

    @Override
    public void setAckBatchConfig(int maxAckBatchSize, long ackLingerMicros) {
        if (maxAckBatchSize <= 0) throw new IllegalArgumentException("maxAckBatchSize must be > 0");
        if (ackLingerMicros < 0) throw new IllegalArgumentException("ackLingerMicros must be >= 0");
        this.maxAckBatchSize = maxAckBatchSize;
        this.ackLingerMicros = ackLingerMicros;
    }

    public int getMaxBatchSize() { return maxBatchSize; }
    public long getLingerMicros() { return lingerMicros; }
    public int getMaxAckBatchSize() { return maxAckBatchSize; }
    public long getAckLingerMicros() { return ackLingerMicros; }

    @Override
    public long getForceCount() {
        return dataForceCount.get() + ackForceCount.get();
    }

    @Override
    public long getDataForceCount() {
        return dataForceCount.get();
    }

    @Override
    public long getAckForceCount() {
        return ackForceCount.get();
    }

    @Override
    public CompletableFuture<WriteResult> append(String queuePath, QueuedMessage message) {
        Objects.requireNonNull(queuePath, "queuePath");
        Objects.requireNonNull(message, "message");
        checkNotFailed();

        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        queue.offer(new AolTask.DataTask(queuePath, message, future));
        return future;
    }

    @Override
    public CompletableFuture<Void> acknowledge(String queuePath, long queueOffset) {
        Objects.requireNonNull(queuePath, "queuePath");
        checkNotFailed();

        CompletableFuture<Void> future = new CompletableFuture<>();
        queue.offer(new AolTask.AckTask(queuePath, queueOffset, future));
        return future;
    }

    private static final class BatchState {
        final Set<AolFile> dirtyFiles = Collections.newSetFromMap(new IdentityHashMap<>());
        final List<PendingWrite> pendingWrites = new ArrayList<>();
        final List<CompletableFuture<Void>> pendingAcks = new ArrayList<>();
        long firstPendingWriteNanos = 0L;
        long firstPendingAckNanos = 0L;

        boolean isEmpty() {
            return dirtyFiles.isEmpty() && pendingWrites.isEmpty() && pendingAcks.isEmpty();
        }

        void resetTimestamps() {
            firstPendingWriteNanos = 0L;
            firstPendingAckNanos = 0L;
        }

        void updateTimestamps(long now) {
            if (!pendingWrites.isEmpty() && firstPendingWriteNanos == 0L) {
                firstPendingWriteNanos = now;
            }
            if (!pendingAcks.isEmpty() && firstPendingAckNanos == 0L) {
                firstPendingAckNanos = now;
            }
        }
    }

    private void writerLoop() {
        BatchState batch = new BatchState();

        while (running || queue.hasPending()) {
            try {
                long waitNanos = prepareDrainWaitNanos(batch);

                int drained = queue.drainBatch(task -> processTask(task, batch), maxBatchSize, waitNanos);

                long now = System.nanoTime();
                batch.updateTimestamps(now);

                if (shouldCommitAfterDrain(batch, drained, now)) {
                    commitBatch(batch);
                }

                if (drained == 0 && !running && batch.isEmpty()) {
                    break;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                recordFailure(t);
            }
        }

        commitBatch(batch);
    }

    private long prepareDrainWaitNanos(BatchState batch) {
        long now = System.nanoTime();
        if (!batch.pendingWrites.isEmpty()) {
            long limit = TimeUnit.MICROSECONDS.toNanos(lingerMicros);
            long elapsed = now - batch.firstPendingWriteNanos;
            if (batch.pendingWrites.size() >= maxBatchSize || elapsed >= limit || lingerMicros <= 0) {
                commitBatch(batch);
                return 0L;
            }
            return limit - elapsed;
        }

        if (!batch.pendingAcks.isEmpty()) {
            long limit = TimeUnit.MICROSECONDS.toNanos(ackLingerMicros);
            long elapsed = now - batch.firstPendingAckNanos;
            if (batch.pendingAcks.size() >= maxAckBatchSize || elapsed >= limit) {
                commitBatch(batch);
                return 0L;
            }
            return limit - elapsed;
        }

        return 0L;
    }

    private void processTask(AolTask task, BatchState batch) {
        try {
            if (failure != null) {
                failTask(task, failure);
                return;
            }

            QueueState state = queues.get(task.queuePath());
            if (state == null) {
                throw new IllegalStateException("Queue not registered in MPSC engine: " + task.queuePath());
            }

            switch (task) {
                case AolTask.DataTask dt -> processDataTask(dt, state, batch);
                case AolTask.AckTask at -> processAckTask(at, state, batch);
            }
        } catch (Throwable t) {
            recordFailure(t);
            failTask(task, t);
        }
    }

    private void processDataTask(AolTask.DataTask dt, QueueState state, BatchState batch) throws Exception {
        long offset = state.nextOffset++;
        ByteBuffer encoded = codec.encode(dt.message(), offset);
        long physPos = state.dataFile.write(encoded);
        batch.dirtyFiles.add(state.dataFile);
        batch.pendingWrites.add(new PendingWrite(dt.future(), offset, physPos));
    }

    private void processAckTask(AolTask.AckTask at, QueueState state, BatchState batch) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).putLong(at.ackOffset());
        buf.flip();
        state.ackedFile.write(buf);
        batch.dirtyFiles.add(state.ackedFile);
        if (at.future() != null) {
            batch.pendingAcks.add(at.future());
        }
    }

    private boolean shouldCommitAfterDrain(BatchState batch, int drained, long now) {
        if (!running && (drained == 0 || !queue.hasPending())) {
            return true;
        }

        if (!batch.pendingWrites.isEmpty()) {
            long writeElapsed = now - batch.firstPendingWriteNanos;
            long writeLimit = TimeUnit.MICROSECONDS.toNanos(lingerMicros);
            return batch.pendingWrites.size() >= maxBatchSize
                    || writeElapsed >= writeLimit
                    || lingerMicros <= 0
                    || drained == 0;
        }

        if (!batch.pendingAcks.isEmpty()) {
            long ackElapsed = now - batch.firstPendingAckNanos;
            long ackLimit = TimeUnit.MICROSECONDS.toNanos(ackLingerMicros);
            return batch.pendingAcks.size() >= maxAckBatchSize
                    || ackElapsed >= ackLimit
                    || drained == 0;
        }

        return false;
    }

    private void commitBatch(BatchState batch) {
        commitBatch(batch.dirtyFiles, batch.pendingWrites, batch.pendingAcks);
        batch.resetTimestamps();
    }

    private void commitBatch(Set<AolFile> dirtyFiles,
                             List<PendingWrite> pendingWrites,
                             List<CompletableFuture<Void>> pendingAcks) {
        if (dirtyFiles.isEmpty() && pendingWrites.isEmpty() && pendingAcks.isEmpty()) {
            return;
        }

        try {
            if (failure == null) {
                int dataFilesCount = 0;
                int ackFilesCount = 0;
                for (AolFile file : dirtyFiles) {
                    if (isAckFile(file)) {
                        ackFilesCount++;
                    } else {
                        dataFilesCount++;
                    }
                }

                fsyncStrategy.force(dirtyFiles);
                dataForceCount.addAndGet(dataFilesCount);
                ackForceCount.addAndGet(ackFilesCount);

                for (PendingWrite pw : pendingWrites) {
                    pw.future().complete(new WriteResult(pw.offset(), pw.physPos()));
                }
                for (CompletableFuture<Void> af : pendingAcks) {
                    af.complete(null);
                }
            } else {
                failAll(pendingWrites, pendingAcks, failure);
            }
        } catch (Throwable t) {
            recordFailure(t);
            failAll(pendingWrites, pendingAcks, t);
        } finally {
            dirtyFiles.clear();
            pendingWrites.clear();
            pendingAcks.clear();
        }
    }

    private boolean isAckFile(AolFile file) {
        for (QueueState qs : queues.values()) {
            if (qs.ackedFile == file) return true;
        }
        return false;
    }

    private void failTask(AolTask task, Throwable cause) {
        switch (task) {
            case AolTask.DataTask dt -> {
                if (dt.future() != null) dt.future().completeExceptionally(cause);
            }
            case AolTask.AckTask at -> {
                if (at.future() != null) at.future().completeExceptionally(cause);
            }
        }
    }

    private void failAll(List<PendingWrite> pendingWrites, List<CompletableFuture<Void>> pendingAcks, Throwable cause) {
        for (PendingWrite pw : pendingWrites) {
            pw.future().completeExceptionally(cause);
        }
        for (CompletableFuture<Void> af : pendingAcks) {
            af.completeExceptionally(cause);
        }
    }

    private void recordFailure(Throwable t) {
        if (this.failure == null) {
            this.failure = t;
        }
    }

    private void checkNotFailed() {
        Throwable f = failure;
        if (f != null) {
            throw new IllegalStateException("AOL MPSC engine is in FAILED state", f);
        }
    }

    private static FsyncStrategy createStrategy(FsyncMode mode) {
        return (mode == FsyncMode.PARALLEL)
                ? new ParallelFsyncStrategy()
                : new SequentialFsyncStrategy();
    }

    @Override
    public void close() throws IOException {
        running = false;
        queue.stop();

        boolean interrupted = false;
        while (true) {
            try {
                writerThread.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        fsyncStrategy.close();

        Throwable f = failure;
        if (f instanceof IOException ioe) {
            throw ioe;
        } else if (f != null) {
            throw new IOException("MPSC engine encountered fatal error", f);
        }
    }
}

