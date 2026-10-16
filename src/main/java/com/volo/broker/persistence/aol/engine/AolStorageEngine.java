package com.volo.broker.persistence.aol.engine;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.WriteResult;
import com.volo.broker.persistence.aol.io.AolFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Common abstraction for batched single-writer AOL storage engines (e.g. MPSC).
 */
public interface AolStorageEngine extends Closeable {

    /**
     * Registers or updates a queue's storage files and initial offset in the engine.
     */
    void registerQueue(String queuePath, AolFile dataFile, AolFile ackedFile, long nextOffset);

    /**
     * Dynamically switches the fsync mode between SEQUENTIAL and PARALLEL.
     */
    void setFsyncMode(FsyncMode mode);

    /**
     * Returns the active fsync mode.
     */
    FsyncMode getFsyncMode();

    /**
     * Configures the micro-batching parameters for data writes.
     */
    void setBatchConfig(int maxBatchSize, long lingerMicros);

    /**
     * Configures batching parameters for standalone ACK persistence.
     */
    void setAckBatchConfig(int maxAckBatchSize, long ackLingerMicros);

    /**
     * Submits a message to be written and persisted. Returns a future completed once durable.
     */
    CompletableFuture<WriteResult> append(String queuePath, QueuedMessage message);

    /**
     * Submits an acknowledgment to be persisted. Returns a future completed once durable.
     */
    CompletableFuture<Void> acknowledge(String queuePath, long queueOffset);

    /**
     * Returns total number of physical file force() calls performed by this engine.
     */
    long getForceCount();

    /**
     * Returns number of physical file force() calls performed for message data files.
     */
    default long getDataForceCount() {
        return 0L;
    }

    /**
     * Returns number of physical file force() calls performed for ack files.
     */
    default long getAckForceCount() {
        return 0L;
    }

    @Override
    void close() throws IOException;
}

