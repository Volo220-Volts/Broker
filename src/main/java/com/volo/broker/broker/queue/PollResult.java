package com.volo.broker.broker.queue;

import com.volo.broker.broker.flow.FlowTransition;
import com.volo.broker.broker.message.QueuedMessage;

import java.util.Objects;
import java.util.Optional;

public record PollResult(
        Optional<QueuedMessage> message,
        FlowTransition transition
) {
    public PollResult {
        Objects.requireNonNull(message, "message must not be null");
        Objects.requireNonNull(transition, "transition must not be null");
    }

    public static PollResult empty() {
        return new PollResult(Optional.empty(), FlowTransition.NONE);
    }

    public static PollResult empty(FlowTransition transition) {
        return new PollResult(Optional.empty(), transition);
    }

    public static PollResult of(QueuedMessage message, FlowTransition transition) {
        return new PollResult(Optional.of(message), transition);
    }

    public static PollResult of(QueuedMessage message) {
        return new PollResult(Optional.of(message), FlowTransition.NONE);
    }
}

