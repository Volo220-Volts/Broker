package com.volo.broker.persistence.aol.api;

/**
 * The result of a successful {@link QueueStorage#append} call.
 *
 * @param queueOffset      offset assigned to the message within its queue
 * @param physicalPosition byte offset in the file where the record begins.
 */
public record WriteResult(long queueOffset, long physicalPosition) {

    public WriteResult {
        if (queueOffset < 0) {
            throw new IllegalArgumentException("queueOffset must be >= 0, got: " + queueOffset);
        }
        if (physicalPosition < 0) {
            throw new IllegalArgumentException("physicalPosition must be >= 0, got: " + physicalPosition);
        }
    }
}
