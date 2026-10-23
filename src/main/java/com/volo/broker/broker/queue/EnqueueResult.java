package com.volo.broker.broker.queue;

import java.util.List;
import java.util.Objects;

import com.volo.broker.broker.flow.FlowTransition;
import com.volo.broker.broker.message.QueuedMessage;

public record EnqueueResult(
        boolean accepted,
        List<QueuedMessage> droppedMessages,
        FlowTransition transition
) {
    public EnqueueResult {
        droppedMessages = Objects.requireNonNullElse(droppedMessages, List.of());
        transition = Objects.requireNonNullElse(transition, FlowTransition.NONE);
    }

    public EnqueueResult(boolean accepted, List<QueuedMessage> droppedMessages) {
        this(accepted, droppedMessages, FlowTransition.NONE);
    }

    public static EnqueueResult ofAccepted() {
        return new EnqueueResult(true, List.of(), FlowTransition.NONE);
    }

    public static EnqueueResult ofAccepted(FlowTransition transition) {
        return new EnqueueResult(true, List.of(), transition);
    }

    public static EnqueueResult ofAcceptedWithDropped(List<QueuedMessage> droppedMessages) {
        return new EnqueueResult(true, List.copyOf(droppedMessages), FlowTransition.NONE);
    }

    public static EnqueueResult ofAcceptedWithDropped(List<QueuedMessage> droppedMessages, FlowTransition transition) {
        return new EnqueueResult(true, List.copyOf(droppedMessages), transition);
    }

    public static EnqueueResult ofRejected() {
        return new EnqueueResult(false, List.of(), FlowTransition.NONE);
    }

    public static EnqueueResult ofRejected(FlowTransition transition) {
        return new EnqueueResult(false, List.of(), transition);
    }
}