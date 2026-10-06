package com.volo.broker.persistence.aol.api;

import com.volo.broker.broker.message.QueuedMessage;

import java.io.Closeable;
import java.io.IOException;

/**
 * Durable storage contract for a single broker queue backed by an Append-Only Log.
 *
 */
public interface QueueStorage extends Closeable {

    /**
     * Durably appends {@code message} to the log for {@code queuePath}.
     *
     * @param queuePath logical queue path
     * @param message   the message to persist
     * @return result containing the assigned {@code queueOffset} and the
     *         physical file position of the written record
     * @throws IOException if the write or fsync fails
     */
    WriteResult append(String queuePath, QueuedMessage message) throws IOException;

    /**
     * Durably records that the message at {@code queueOffset} has been
     * acknowledged and should not be re-delivered after a restart.
     *
     * @param queuePath   logical queue path
     * @param queueOffset the offset of the message
     * @throws IOException if persisting the acknowledgement fails
     */
    void acknowledge(String queuePath, long queueOffset) throws IOException;

    /**
     * Recovers all unacknowledged messages for {@code queuePath} from disk.
     *
     * @param queuePath logical queue path
     * @return the recovery result; returns empty {@link RecoveryResult} if no data exists for this queue
     * @throws IOException if reading or repairing the segment file fails
     */
    RecoveryResult recover(String queuePath) throws IOException;


    @Override
    void close() throws IOException;
}
