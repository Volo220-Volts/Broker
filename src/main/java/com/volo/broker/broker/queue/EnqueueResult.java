package com.volo.broker.broker.queue;

import java.util.List;

import com.volo.broker.broker.message.QueuedMessage;

public record EnqueueResult(
        boolean accepted,
        List<QueuedMessage> droppedMessages
) {
    public static EnqueueResult ofAccepted() {
        return new EnqueueResult(true, List.of());
    }

    public static EnqueueResult ofAcceptedWithDropped(List<QueuedMessage> droppedMessages) {
        return new EnqueueResult(true, List.copyOf(droppedMessages));
    }

    public static EnqueueResult ofRejected() {
        return new EnqueueResult(false, List.of());
    }
}