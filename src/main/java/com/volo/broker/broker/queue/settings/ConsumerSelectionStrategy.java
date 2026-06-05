package com.volo.broker.broker.queue.settings;

public enum ConsumerSelectionStrategy {
    ROUND_ROBIN,
    FIRST_AVAILABLE,
    LEAST_INFLIGHT
}
