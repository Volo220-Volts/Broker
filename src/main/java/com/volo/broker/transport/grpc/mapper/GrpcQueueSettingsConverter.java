package com.volo.broker.transport.grpc.mapper;

import com.custombroker.grpc.QueueSettings;
import com.volo.broker.broker.queue.settings.DispatchPolicy;
import com.volo.broker.broker.queue.settings.MessageRetentionPolicy;
import com.volo.broker.broker.queue.settings.QueueCapacityPolicy;
import com.volo.broker.broker.queue.settings.QueueLifecyclePolicy;
import com.volo.broker.broker.queue.settings.RedeliveryPolicy;
import com.volo.broker.broker.queue.settings.PersistenceMode;
import com.volo.broker.broker.queue.settings.PublishAckMode;
import com.volo.broker.broker.queue.settings.ConsumerAckMode;
import com.volo.broker.broker.queue.settings.OverflowPolicy;
import com.volo.broker.broker.queue.settings.ConsumerSelectionStrategy;

public final class GrpcQueueSettingsConverter {
    private GrpcQueueSettingsConverter() {}

    public static com.volo.broker.broker.queue.settings.QueueSettings toDomain(QueueSettings proto) {
        if (proto == null) return com.volo.broker.broker.queue.settings.QueueSettings.defaults();

        return new com.volo.broker.broker.queue.settings.QueueSettings(
                toPersistenceMode(proto.getPersistenceMode()),
                toPublishAckMode(proto.getPublishAckMode()),
                toConsumerAckMode(proto.getConsumerAckMode()),
                toRedeliveryPolicy(proto.hasRedeliveryPolicy() ? proto.getRedeliveryPolicy() : null),
                toRetentionPolicy(proto.hasRetentionPolicy() ? proto.getRetentionPolicy() : null),
                toLifecyclePolicy(proto.hasLifecyclePolicy() ? proto.getLifecyclePolicy() : null),
                toCapacityPolicy(proto.hasCapacityPolicy() ? proto.getCapacityPolicy() : null),
                toDispatchPolicy(proto.hasDispatchPolicy() ? proto.getDispatchPolicy() : null)
        );
    }

    private static PersistenceMode toPersistenceMode(com.custombroker.grpc.PersistenceMode mode) {
        return switch (mode) {
            case MEMORY_ONLY -> PersistenceMode.MEMORY_ONLY;
            case PERSISTENT, PERSISTENCE_MODE_UNSPECIFIED, UNRECOGNIZED -> PersistenceMode.PERSISTENT;
        };
    }

    private static PublishAckMode toPublishAckMode(com.custombroker.grpc.PublishAckMode mode) {
        return switch (mode) {
            case PUBLISH_ACK_NONE -> PublishAckMode.NONE;
            case PUBLISH_ACK_ENABLED, PUBLISH_ACK_MODE_UNSPECIFIED, UNRECOGNIZED -> PublishAckMode.ENABLED;
        };
    }

    private static ConsumerAckMode toConsumerAckMode(com.custombroker.grpc.ConsumerAckMode mode) {
        return switch (mode) {
            case CONSUMER_ACK_NONE -> ConsumerAckMode.NONE;
            case CONSUMER_ACK_ENABLED, CONSUMER_ACK_MODE_UNSPECIFIED, UNRECOGNIZED -> ConsumerAckMode.ENABLED;
        };
    }

    private static RedeliveryPolicy toRedeliveryPolicy(com.custombroker.grpc.RedeliveryPolicy proto) {
        if (proto == null) return RedeliveryPolicy.defaults();
        return new RedeliveryPolicy(proto.getEnabled(), proto.getMaxAttempts(), proto.getRequeueOnDisconnect(), proto.getRequeueOnSendFailure());
    }

    private static MessageRetentionPolicy toRetentionPolicy(com.custombroker.grpc.MessageRetentionPolicy proto) {
        if (proto == null) return MessageRetentionPolicy.disabled();
        return new MessageRetentionPolicy(proto.getTtlEnabled(), proto.getTtlMs());
    }

    private static QueueLifecyclePolicy toLifecyclePolicy(com.custombroker.grpc.QueueLifecyclePolicy proto) {
        if (proto == null) return QueueLifecyclePolicy.defaults();
        return new QueueLifecyclePolicy(proto.getAutoDelete(), proto.getIdleTimeoutEnabled(), proto.getIdleTimeoutMs());
    }

    private static QueueCapacityPolicy toCapacityPolicy(com.custombroker.grpc.QueueCapacityPolicy proto) {
        if (proto == null) return QueueCapacityPolicy.unlimited();
        return new QueueCapacityPolicy(proto.getLimited(), proto.getMaxMessages(), proto.getMaxBytes(), toOverflowPolicy(proto.getOverflowPolicy()));
    }

    private static DispatchPolicy toDispatchPolicy(com.custombroker.grpc.DispatchPolicy proto) {
        if (proto == null) return DispatchPolicy.defaults();
        return new DispatchPolicy(proto.getPrefetchCount(), toConsumerSelectionStrategy(proto.getConsumerSelectionStrategy()));
    }

    private static OverflowPolicy toOverflowPolicy(com.custombroker.grpc.OverflowPolicy policy) {
        return switch (policy) {
            case DROP_OLDEST -> OverflowPolicy.DROP_OLDEST;
            case DROP_NEWEST -> OverflowPolicy.DROP_NEWEST;
            case REJECT_PUBLISH, OVERFLOW_POLICY_UNSPECIFIED, UNRECOGNIZED -> OverflowPolicy.REJECT_PUBLISH;
        };
    }

    private static ConsumerSelectionStrategy toConsumerSelectionStrategy(com.custombroker.grpc.ConsumerSelectionStrategy strategy) {
        return switch (strategy) {
            case FIRST_AVAILABLE -> ConsumerSelectionStrategy.FIRST_AVAILABLE;
            case LEAST_INFLIGHT -> ConsumerSelectionStrategy.LEAST_INFLIGHT;
            case ROUND_ROBIN, CONSUMER_SELECTION_STRATEGY_UNSPECIFIED, UNRECOGNIZED -> ConsumerSelectionStrategy.ROUND_ROBIN;
        };
    }
}
