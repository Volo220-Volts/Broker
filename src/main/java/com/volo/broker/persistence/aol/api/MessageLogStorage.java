package com.volo.broker.persistence.aol.api;

import com.volo.broker.broker.message.QueuedMessage;

import java.io.Closeable;
import java.io.IOException;

/**
 * Low-level storage interface for appending, acknowledging, and recovering
 * message streams in append-only log files.
 */
public interface MessageLogStorage extends Closeable {

    /**
     * Appends a message to the queue's data log and returns the assigned offset and physical position.
     *
     * @param queuePath logical queue path
     * @param message   the message to persist
     * @return offset and file position of the persisted message
     * @throws IOException if a physical I/O error occurs
     */
    WriteResult append(String queuePath, QueuedMessage message) throws IOException;

    /**
     * Records a consumer acknowledgment for the specified message offset.
     *
     * @param queuePath   logical queue path
     * @param queueOffset monotonic queue offset being acknowledged
     * @throws IOException if a physical I/O error occurs
     */
    void acknowledge(String queuePath, long queueOffset) throws IOException;

    /**
     * Recovers all unacknowledged (live) messages for the specified queue.
     *
     * @param queuePath logical queue path
     * @return live messages and the next offset to assign
     * @throws IOException if a physical I/O error occurs
     */
    RecoveryResult recover(String queuePath) throws IOException;
}

