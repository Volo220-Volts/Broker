package com.volo.broker.broker.queue.settings;

import java.util.Objects;

public record QueueSettings(
        PersistenceMode persistenceMode,
        PublishAckMode publishAckMode,
        ConsumerAckMode consumerAckMode,
        RedeliveryPolicy redeliveryPolicy,
        MessageRetentionPolicy retentionPolicy,
        QueueLifecyclePolicy lifecyclePolicy,
        QueueCapacityPolicy capacityPolicy,
        DispatchPolicy dispatchPolicy
) {
    public QueueSettings {
        persistenceMode = Objects.requireNonNullElse(persistenceMode, PersistenceMode.PERSISTENT);
        publishAckMode = Objects.requireNonNullElse(publishAckMode, PublishAckMode.ENABLED);
        consumerAckMode = Objects.requireNonNullElse(consumerAckMode, ConsumerAckMode.ENABLED);
        redeliveryPolicy = Objects.requireNonNullElse(redeliveryPolicy, RedeliveryPolicy.defaults());
        retentionPolicy = Objects.requireNonNullElse(retentionPolicy, MessageRetentionPolicy.disabled());
        lifecyclePolicy = Objects.requireNonNullElse(lifecyclePolicy, QueueLifecyclePolicy.defaults());
        capacityPolicy = Objects.requireNonNullElse(capacityPolicy, QueueCapacityPolicy.unlimited());
        dispatchPolicy = Objects.requireNonNullElse(dispatchPolicy, DispatchPolicy.defaults());

        validateCompatibility(persistenceMode, consumerAckMode, redeliveryPolicy);
    }

    public static QueueSettings defaults() {
        return new QueueSettings(
                PersistenceMode.PERSISTENT,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                RedeliveryPolicy.defaults(),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                QueueCapacityPolicy.unlimited(),
                DispatchPolicy.defaults()
        );
    }

    public boolean persistent() {
        return persistenceMode == PersistenceMode.PERSISTENT;
    }

    public boolean publishAckEnabled() {
        return publishAckMode == PublishAckMode.ENABLED;
    }

    public boolean consumerAckEnabled() {
        return consumerAckMode == ConsumerAckMode.ENABLED;
    }

    private static void validateCompatibility(
            PersistenceMode persistenceMode,
            ConsumerAckMode consumerAckMode,
            RedeliveryPolicy redeliveryPolicy
    ) {
        if (persistenceMode == PersistenceMode.PERSISTENT && consumerAckMode == ConsumerAckMode.NONE) {
            throw new IllegalArgumentException("PERSISTENT queue requires consumer acknowledgements");
        }

        if (consumerAckMode == ConsumerAckMode.NONE && redeliveryPolicy.enabled()) {
            throw new IllegalArgumentException("Redelivery requires consumer acknowledgements");
        }
    }
}
