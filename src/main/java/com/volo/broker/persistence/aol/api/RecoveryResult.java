package com.volo.broker.persistence.aol.api;

import com.volo.broker.broker.message.QueuedMessage;

import java.util.List;

/**
 * The result of {@link MessageLogStorage#recover} for a single queue.
 */
public record RecoveryResult(List<QueuedMessage> liveMessages, long nextQueueOffset) {

    public RecoveryResult {
        if (liveMessages == null) throw new NullPointerException("liveMessages must not be null");
        liveMessages = List.copyOf(liveMessages);
        if (nextQueueOffset < 0) throw new IllegalArgumentException(
                "nextQueueOffset must be >= 0, got: " + nextQueueOffset);

        
    }

    public static RecoveryResult empty() {
        return new RecoveryResult(List.of(), 0L);
    }

    public boolean isEmpty() {
        return liveMessages.isEmpty();
    }
}
