package com.volo.broker.persistence.aol.codec;

import com.volo.broker.broker.message.QueuedMessage;

/**
 * The result of successfully decoding a single AOL data record.
 *
 * @param queueOffset position of this message within its queue
 * @param message     the fully reconstructed {@link QueuedMessage}
 */
public record DecodedRecord(long queueOffset, QueuedMessage message) {

    public DecodedRecord {
        if (queueOffset < 0) {
            throw new IllegalArgumentException("queueOffset must be >= 0, got: " + queueOffset);
        }
        if (message == null) {
            throw new NullPointerException("message must not be null");
        }
    }
}
