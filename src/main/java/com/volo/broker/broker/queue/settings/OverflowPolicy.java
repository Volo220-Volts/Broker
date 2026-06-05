package com.volo.broker.broker.queue.settings;

public enum OverflowPolicy {
    REJECT_PUBLISH,
    DROP_OLDEST,
    DROP_NEWEST
}
