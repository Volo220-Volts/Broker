package com.volo.broker.broker.queue.settings;

import java.util.Objects;

public record DispatchPolicy(
        int prefetchCount,
        ConsumerSelectionStrategy consumerSelectionStrategy
) {
    private static final int DEFAULT_PREFETCH_COUNT = 10;

    public DispatchPolicy {
        if (prefetchCount <= 0) {
            prefetchCount = DEFAULT_PREFETCH_COUNT;
        }

        consumerSelectionStrategy = Objects.requireNonNullElse(
                consumerSelectionStrategy,
                ConsumerSelectionStrategy.ROUND_ROBIN
        );
    }

    public static DispatchPolicy defaults() {
        return new DispatchPolicy(DEFAULT_PREFETCH_COUNT, ConsumerSelectionStrategy.ROUND_ROBIN);
    }
}
