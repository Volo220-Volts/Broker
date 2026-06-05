package com.volo.broker.broker.queue.settings;

import java.util.Objects;

public record QueueCapacityPolicy(
        boolean limited,
        int maxMessages,
        long maxBytes,
        OverflowPolicy overflowPolicy
) {
    public QueueCapacityPolicy {
        overflowPolicy = Objects.requireNonNullElse(overflowPolicy, OverflowPolicy.REJECT_PUBLISH);

        if (limited && maxMessages <= 0 && maxBytes <= 0) {
            throw new IllegalArgumentException("maxMessages or maxBytes must be configured when capacity is limited");
        }

        if (maxMessages < 0) {
            throw new IllegalArgumentException("maxMessages must not be negative");
        }

        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must not be negative");
        }
    }

    public static QueueCapacityPolicy unlimited() {
        return new QueueCapacityPolicy(false, 0, 0, OverflowPolicy.REJECT_PUBLISH);
    }
}
