package com.volo.broker.broker.queue.settings;

public record QueueLifecyclePolicy(
        boolean autoDelete,
        boolean idleTimeoutEnabled,
        long idleTimeoutMs
) {
    public QueueLifecyclePolicy {
        if (idleTimeoutEnabled && idleTimeoutMs <= 0) {
            throw new IllegalArgumentException("idleTimeoutMs must be greater than zero when idle timeout is enabled");
        }

        if (!idleTimeoutEnabled && idleTimeoutMs < 0) {
            throw new IllegalArgumentException("idleTimeoutMs must not be negative");
        }
    }

    public static QueueLifecyclePolicy defaults() {
        return new QueueLifecyclePolicy(false, false, 0);
    }
}
