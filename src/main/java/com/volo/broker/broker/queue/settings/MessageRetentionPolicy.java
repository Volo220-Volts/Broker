package com.volo.broker.broker.queue.settings;

public record MessageRetentionPolicy(
        boolean ttlEnabled,
        long ttlMs
) {
    public MessageRetentionPolicy {
        if (ttlEnabled && ttlMs <= 0) {
            throw new IllegalArgumentException("ttlMs must be greater than zero when TTL is enabled");
        }

        if (!ttlEnabled && ttlMs < 0) {
            throw new IllegalArgumentException("ttlMs must not be negative");
        }
    }

    public static MessageRetentionPolicy disabled() {
        return new MessageRetentionPolicy(false, 0);
    }

    public boolean expired(long createdAt, long now) {
        return ttlEnabled && now - createdAt >= ttlMs;
    }
}
