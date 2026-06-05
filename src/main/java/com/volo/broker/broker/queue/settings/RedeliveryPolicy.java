package com.volo.broker.broker.queue.settings;

public record RedeliveryPolicy(
        boolean enabled,
        int maxAttempts,
        boolean requeueOnDisconnect,
        boolean requeueOnSendFailure
) {
    public RedeliveryPolicy {
        if (enabled && maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be greater than zero when redelivery is enabled");
        }
    }

    public static RedeliveryPolicy defaults() {
        return new RedeliveryPolicy(true, 5, true, true);
    }

    public static RedeliveryPolicy disabled() {
        return new RedeliveryPolicy(false, 1, false, false);
    }
}
