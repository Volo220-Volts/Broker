package com.volo.broker.persistence.aol.store;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.MessageLogStorage;
import com.volo.broker.persistence.aol.api.RecoveryResult;
import com.volo.broker.persistence.aol.api.WriteResult;
import com.volo.broker.persistence.aol.codec.DataRecordCodec;
import com.volo.broker.persistence.aol.codec.DecodedRecord;
import com.volo.broker.persistence.aol.engine.AolMpscEngine;
import com.volo.broker.persistence.aol.engine.AolStorageEngine;
import com.volo.broker.persistence.aol.engine.FsyncMode;
import com.volo.broker.persistence.aol.io.AolFileChannel;
import com.volo.broker.persistence.aol.io.SegmentTailValidator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Append-Only Log (AOL) implementation of {@link MessageLogStorage} powered by
 * a lock-free {@link AolMpscEngine} and parallel disk synchronization.
 */
public final class AolMessageLogStorage implements MessageLogStorage {

    private static final Logger log = LoggerFactory.getLogger(AolMessageLogStorage.class);

    private static final String DATA_FILE_NAME   = "data.log";
    private static final String ACKED_FILE_NAME  = "acked.bin";
    private static final int    ACKED_ENTRY_SIZE = Long.BYTES; // 8 bytes per offset
    private static final int    RECORD_LEN_FIELD = Integer.BYTES; // 4 bytes

    public enum StorageEngineType {
        MPSC
    }

    private record QueueFiles(AolFileChannel dataFile, AolFileChannel ackedFile) implements Closeable {
        @Override
        public void close() throws IOException {
            IOException first = null;
            try {
                dataFile.close();
            } catch (IOException e) {
                first = e;
            }
            try {
                ackedFile.close();
            } catch (IOException e) {
                if (first == null) first = e;
            }
            if (first != null) throw first;
        }
    }

    private final Path                               baseDir;
    private final DataRecordCodec                    codec;
    private final SegmentTailValidator               tailValidator;
    private final StorageEngineType                  engineType;
    private final AolMpscEngine                      mpscEngine;
    private final ConcurrentHashMap<String, QueueFiles> openFiles = new ConcurrentHashMap<>();
    private final AtomicBoolean                      closed = new AtomicBoolean(false);
    private final Thread                             shutdownHook;
    private volatile boolean                         syncAck = true;

    // ------------------------------------------------------------------ construction

    /**
     * Creates a new {@code AolMessageLogStorage} rooted at {@code baseDir} using the lock-free MPSC engine
     * with {@link FsyncMode#PARALLEL} by default.
     *
     * @param baseDir root directory for all queue message logs
     * @throws IOException if the root directory cannot be created
     */
    public AolMessageLogStorage(Path baseDir) throws IOException {
        this(baseDir, StorageEngineType.MPSC, FsyncMode.PARALLEL);
    }

    /**
     * Creates a new {@code AolMessageLogStorage} rooted at {@code baseDir} using the lock-free MPSC engine
     * with the specified {@link FsyncMode}.
     */
    public AolMessageLogStorage(Path baseDir, FsyncMode fsyncMode) throws IOException {
        this(baseDir, StorageEngineType.MPSC, fsyncMode);
    }

    /**
     * Creates a new {@code AolMessageLogStorage} rooted at {@code baseDir} with specified engine type and fsync mode.
     */
    public AolMessageLogStorage(Path baseDir, StorageEngineType engineType, FsyncMode fsyncMode) throws IOException {
        this.baseDir       = Objects.requireNonNull(baseDir, "baseDir");
        this.codec         = new DataRecordCodec();
        this.tailValidator = new SegmentTailValidator(codec);
        this.engineType    = engineType != null ? engineType : StorageEngineType.MPSC;
        FsyncMode mode     = fsyncMode != null ? fsyncMode : FsyncMode.PARALLEL;
        this.mpscEngine    = new AolMpscEngine(codec, mode);
        Files.createDirectories(baseDir);

        this.shutdownHook = new Thread(() -> {
            try {
                close();
            } catch (IOException e) {
                log.error("Error during AOL storage shutdown hook: {}", e.getMessage(), e);
            }
        }, "aol-storage-shutdown-hook");
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // JVM is shutting down
        }
    }

    /**
     * Dynamically switches the fsync mode on the MPSC engine (SEQUENTIAL or PARALLEL).
     */
    public void setFsyncMode(FsyncMode mode) {
        mpscEngine.setFsyncMode(mode);
    }

    public FsyncMode getFsyncMode() {
        return mpscEngine.getFsyncMode();
    }

    public StorageEngineType getEngineType() {
        return engineType;
    }

    public AolStorageEngine getBatchEngine() {
        return mpscEngine;
    }

    public AolMpscEngine getMpscEngine() {
        return mpscEngine;
    }

    /**
     * Configures the micro-batching parameters for data writes in the MPSC engine.
     */
    public void setBatchConfig(int maxBatchSize, long lingerMicros) {
        mpscEngine.setBatchConfig(maxBatchSize, lingerMicros);
    }

    /**
     * Configures batching parameters for standalone ACK persistence in the MPSC engine.
     */
    public void setAckBatchConfig(int maxAckBatchSize, long ackLingerMicros) {
        mpscEngine.setAckBatchConfig(maxAckBatchSize, ackLingerMicros);
    }

    /**
     * Sets whether consumer acknowledgments synchronously await disk fsync before returning.
     * When false, acks are queued lock-free and persisted in background batches without blocking consumers.
     */
    public void setSyncAck(boolean syncAck) {
        this.syncAck = syncAck;
    }

    public boolean isSyncAck() {
        return syncAck;
    }

    /**
     * Returns total number of physical file force() calls performed by the underlying MPSC engine.
     */
    public long getForceCount() {
        return mpscEngine.getForceCount();
    }

    /**
     * Returns number of physical file force() calls performed for message data files.
     */
    public long getDataForceCount() {
        return mpscEngine.getDataForceCount();
    }

    /**
     * Returns number of physical file force() calls performed for ack files.
     */
    public long getAckForceCount() {
        return mpscEngine.getAckForceCount();
    }

    @Override
    public WriteResult append(String queuePath, QueuedMessage message) throws IOException {
        validateQueuePath(queuePath);
        Objects.requireNonNull(message, "message");

        ensureQueueRegistered(queuePath);
        try {
            return mpscEngine.append(queuePath, message).join();
        } catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            if (cause instanceof IOException ioe) throw ioe;
            throw new IOException("MPSC engine append failed", cause);
        }
    }

    @Override
    public void acknowledge(String queuePath, long queueOffset) throws IOException {
        validateQueuePath(queuePath);

        ensureQueueRegistered(queuePath);
        try {
            CompletableFuture<Void> fut = mpscEngine.acknowledge(queuePath, queueOffset);
            if (syncAck) {
                fut.join();
            }
        } catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            if (cause instanceof IOException ioe) throw ioe;
            throw new IOException("MPSC engine acknowledge failed", cause);
        }
    }

    @Override
    public RecoveryResult recover(String queuePath) throws IOException {
        validateQueuePath(queuePath);

        Path queueDir  = queueDir(queuePath);
        Path dataPath  = queueDir.resolve(DATA_FILE_NAME);
        Path ackedPath = queueDir.resolve(ACKED_FILE_NAME);

        if (!Files.exists(dataPath)) {
            log.info("No data.log for queue '" + queuePath + "' — starting fresh.");
            return RecoveryResult.empty();
        }

        Files.createDirectories(queueDir);

        AolFileChannel dataFile = AolFileChannel.openExisting(dataPath);
        try {
            SegmentTailValidator.ValidationResult tailResult = tailValidator.validate(dataFile);
            if (tailResult.truncationNeeded()) {
                log.warn("Queue '{}': truncating corrupt tail at {} (was {})",
                        queuePath, tailResult.lastValidPosition(), dataFile.size());
                dataFile.truncate(tailResult.lastValidPosition());
            }

            Set<Long> ackedOffsets = loadAckedOffsets(ackedPath);

            ScanResult scan = scanDataFile(dataFile, ackedOffsets);

            AolFileChannel ackedFile = Files.exists(ackedPath)
                    ? AolFileChannel.openExisting(ackedPath)
                    : AolFileChannel.createNew(ackedPath);

            QueueFiles qf = new QueueFiles(dataFile, ackedFile);
            QueueFiles previous = openFiles.put(queuePath, qf);
            mpscEngine.registerQueue(queuePath, dataFile, ackedFile, scan.nextOffset());
            if (previous != null) {
                log.warn("Queue '{}' was already recovered — closing old files.", queuePath);
                previous.close();
            }

            log.info("Queue '" + queuePath + "' recovered: "
                    + scan.liveMessages().size() + " live messages, nextOffset=" + scan.nextOffset());

            return new RecoveryResult(scan.liveMessages(), scan.nextOffset());

        } catch (Exception e) {
            dataFile.close();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // JVM is already shutting down
        }

        IOException firstError = null;
        try {
            mpscEngine.close();
        } catch (IOException e) {
            log.warn("Error closing MPSC engine: {}", e.getMessage());
            firstError = e;
        }

        for (var entry : openFiles.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.warn("Error closing files for queue '{}': {}", entry.getKey(), e.getMessage());
                if (firstError == null) firstError = e;
            }
        }

        long dataForces = getDataForceCount();
        long ackForces = getAckForceCount();
        long totalForces = dataForces + ackForces;

        String statsMessage = String.format(
                """

                =======================================================
                [AOL Storage Shutdown] Fsync Statistics:
                  - Message data forces (data.log) : %d
                  - Ack forces (acked.bin)          : %d
                  - Total fsync force() calls       : %d
                =======================================================""",
                dataForces, ackForces, totalForces
        );

        log.info(statsMessage);

        openFiles.clear();
        if (firstError != null) throw firstError;
    }

    // ------------------------------------------------------------------ private: file lifecycle

    private QueueFiles ensureQueueRegistered(String queuePath) throws IOException {
        QueueFiles existing = openFiles.get(queuePath);
        if (existing != null) return existing;

        return createQueueFiles(queuePath);
    }

    private synchronized QueueFiles createQueueFiles(String queuePath) throws IOException {
        QueueFiles existing = openFiles.get(queuePath);
        if (existing != null) return existing;

        Path queueDir  = queueDir(queuePath);
        Path dataPath  = queueDir.resolve(DATA_FILE_NAME);
        Path ackedPath = queueDir.resolve(ACKED_FILE_NAME);
        Files.createDirectories(queueDir);

        AolFileChannel dataFile = Files.exists(dataPath)
                ? AolFileChannel.openExisting(dataPath)
                : AolFileChannel.createNew(dataPath);

        AolFileChannel ackedFile = Files.exists(ackedPath)
                ? AolFileChannel.openExisting(ackedPath)
                : AolFileChannel.createNew(ackedPath);

        QueueFiles qf = new QueueFiles(dataFile, ackedFile);
        openFiles.put(queuePath, qf);
        mpscEngine.registerQueue(queuePath, dataFile, ackedFile, 0L);
        log.info("Registered new queue '{}' in MPSC engine.", queuePath);
        return qf;
    }

    private static Set<Long> loadAckedOffsets(Path ackedPath) throws IOException {
        if (!Files.exists(ackedPath)) {
            return Set.of();
        }
        byte[] bytes = Files.readAllBytes(ackedPath);
        int count = bytes.length / ACKED_ENTRY_SIZE;
        Set<Long> offsets = new HashSet<>(count * 2);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        for (int i = 0; i < count; i++) {
            offsets.add(buf.getLong());
        }
        return offsets;
    }

    /**
     * Sequentially scans {@code dataFile} from position 0, decoding every record.
     * Skips acked messages.
     */
    private ScanResult scanDataFile(AolFileChannel dataFile,
                                    Set<Long> ackedOffsets) throws IOException {
        List<QueuedMessage> live      = new ArrayList<>();
        long                maxOffset = -1;
        long                pos       = 0;
        long                fileSize  = dataFile.size();

        while (pos + RECORD_LEN_FIELD <= fileSize) {

            ByteBuffer lenBuf      = dataFile.read(pos, RECORD_LEN_FIELD);
            int        recordLength = lenBuf.getInt();

            int  minBody   = DataRecordCodec.FIXED_BODY_SIZE + RECORD_LEN_FIELD; // 43
            long recordEnd = pos + RECORD_LEN_FIELD + recordLength;

            if (recordLength < minBody || recordEnd > fileSize) {
                break;
            }

            ByteBuffer  recordBuf = dataFile.read(pos, RECORD_LEN_FIELD + recordLength);
            DecodedRecord decoded = codec.decode(recordBuf);

            maxOffset = Math.max(maxOffset, decoded.queueOffset());

            if (!ackedOffsets.contains(decoded.queueOffset())) {
                live.add(decoded.message().asRedelivered());
            }

            pos = recordEnd;
        }

        long nextOffset = maxOffset < 0 ? 0L : maxOffset + 1;
        return new ScanResult(live, nextOffset);
    }

    private Path queueDir(String queuePath) {
        return baseDir.resolve("queues").resolve(encodeQueueName(queuePath));
    }

    private static String encodeQueueName(String queuePath) {
        return queuePath
                .replace('/', '_')
                .replace('\\', '_')
                .replace('\0', '_');
    }

    private static void validateQueuePath(String queuePath) {
        if (queuePath == null || queuePath.isBlank()) {
            throw new IllegalArgumentException("queuePath must not be null or blank");
        }
    }

    private record ScanResult(List<QueuedMessage> liveMessages, long nextOffset) {}
}

