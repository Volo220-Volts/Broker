package com.volo.broker;

import com.volo.broker.broker.queue.settings.*;

public final class TestQueueSettings {
    private TestQueueSettings() {
    }

    public static QueueSettings reliable() {
        return QueueSettings.defaults();
    }

    public static QueueSettings memoryOnlyNoConsumerAck() {
        return new QueueSettings(
            PersistenceMode.MEMORY_ONLY,
            PublishAckMode.ENABLED,
            ConsumerAckMode.NONE,
            RedeliveryPolicy.disabled(),
            MessageRetentionPolicy.disabled(),
            QueueLifecyclePolicy.defaults(),
            QueueCapacityPolicy.unlimited(),
            DispatchPolicy.defaults()
        );
    }

    public static QueueSettings withPrefetch(int prefetch) {
        return new QueueSettings(
            PersistenceMode.PERSISTENT,
            PublishAckMode.ENABLED,
            ConsumerAckMode.ENABLED,
            RedeliveryPolicy.defaults(),
            MessageRetentionPolicy.disabled(),
            QueueLifecyclePolicy.defaults(),
            QueueCapacityPolicy.unlimited(),
            new DispatchPolicy(prefetch, ConsumerSelectionStrategy.ROUND_ROBIN)
        );
    }

    public static QueueSettings withTtl(long ttlMs) {
        return new QueueSettings(
            PersistenceMode.PERSISTENT,
            PublishAckMode.ENABLED,
            ConsumerAckMode.ENABLED,
            RedeliveryPolicy.defaults(),
            new MessageRetentionPolicy(true, ttlMs),
            QueueLifecyclePolicy.defaults(),
            QueueCapacityPolicy.unlimited(),
            DispatchPolicy.defaults()
        );
    }

    public static QueueSettings withCapacity(
            int maxMessages,
            OverflowPolicy overflowPolicy
    ) {
        return new QueueSettings(
            PersistenceMode.MEMORY_ONLY,
            PublishAckMode.ENABLED,
            ConsumerAckMode.NONE,
            RedeliveryPolicy.disabled(),
            MessageRetentionPolicy.disabled(),
            QueueLifecyclePolicy.defaults(),
            new QueueCapacityPolicy(true, maxMessages, 0, overflowPolicy),
            DispatchPolicy.defaults()
        );
    }

    public static QueueSettings reliableWithRedeliveryDisabled() {
        return new QueueSettings(
            PersistenceMode.PERSISTENT,
            PublishAckMode.ENABLED,
            ConsumerAckMode.ENABLED,
            RedeliveryPolicy.disabled(),
            MessageRetentionPolicy.disabled(),
            QueueLifecyclePolicy.defaults(),
            QueueCapacityPolicy.unlimited(),
            DispatchPolicy.defaults()
        );
    }

    public static QueueSettings memoryOnlyWithConsumerAckAndRedelivery() {
        return new QueueSettings(
            PersistenceMode.MEMORY_ONLY,
            PublishAckMode.ENABLED,
            ConsumerAckMode.ENABLED,
            RedeliveryPolicy.defaults(),
            MessageRetentionPolicy.disabled(),
            QueueLifecyclePolicy.defaults(),
            QueueCapacityPolicy.unlimited(),
            DispatchPolicy.defaults()
        );
    }

    public static QueueSettings withIdleTimeout(long idleTimeoutMs) {
        return new QueueSettings(
            PersistenceMode.MEMORY_ONLY,
            PublishAckMode.ENABLED,
            ConsumerAckMode.NONE,
            RedeliveryPolicy.disabled(),
            MessageRetentionPolicy.disabled(),
            new QueueLifecyclePolicy(false, true, idleTimeoutMs, false),
            QueueCapacityPolicy.unlimited(),
            DispatchPolicy.defaults()
        );
    }

    public static QueueSettings autoDeleteMemoryOnly() {
        return new QueueSettings(
            PersistenceMode.MEMORY_ONLY,
            PublishAckMode.ENABLED,
            ConsumerAckMode.NONE,
            RedeliveryPolicy.disabled(),
            MessageRetentionPolicy.disabled(),
            new QueueLifecyclePolicy(true, false, 0, false),
            QueueCapacityPolicy.unlimited(),
            DispatchPolicy.defaults()
        );
    }

    public static QueueSettings withFlowControl(FlowControlPolicy flowControlPolicy) {
        return new QueueSettings(
            PersistenceMode.PERSISTENT,
            PublishAckMode.ENABLED,
            ConsumerAckMode.ENABLED,
            RedeliveryPolicy.defaults(),
            MessageRetentionPolicy.disabled(),
            QueueLifecyclePolicy.defaults(),
            QueueCapacityPolicy.unlimited(),
            DispatchPolicy.defaults(),
            flowControlPolicy
        );
    }
}