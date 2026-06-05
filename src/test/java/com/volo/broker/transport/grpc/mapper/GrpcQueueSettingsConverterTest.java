package com.volo.broker.transport.grpc.mapper;

import com.custombroker.grpc.ConsumerAckMode;
import com.custombroker.grpc.ConsumerSelectionStrategy;
import com.custombroker.grpc.DispatchPolicy;
import com.custombroker.grpc.MessageRetentionPolicy;
import com.custombroker.grpc.OverflowPolicy;
import com.custombroker.grpc.PersistenceMode;
import com.custombroker.grpc.PublishAckMode;
import com.custombroker.grpc.QueueCapacityPolicy;
import com.custombroker.grpc.QueueLifecyclePolicy;
import com.custombroker.grpc.QueueSettings;
import com.custombroker.grpc.RedeliveryPolicy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcQueueSettingsConverterTest {

    @Test
    void toDomain_shouldConvertFullSettings() {
        QueueSettings proto = QueueSettings.newBuilder()
                .setPersistenceMode(PersistenceMode.MEMORY_ONLY)
                .setPublishAckMode(PublishAckMode.PUBLISH_ACK_NONE)
                .setConsumerAckMode(ConsumerAckMode.CONSUMER_ACK_NONE)
                .setRedeliveryPolicy(RedeliveryPolicy.newBuilder()
                        .setEnabled(false)
                        .setMaxAttempts(1)
                        .setRequeueOnDisconnect(false)
                        .setRequeueOnSendFailure(false)
                        .build())
                .setRetentionPolicy(MessageRetentionPolicy.newBuilder()
                        .setTtlEnabled(true)
                        .setTtlMs(1000)
                        .build())
                .setLifecyclePolicy(QueueLifecyclePolicy.newBuilder()
                        .setAutoDelete(true)
                        .setIdleTimeoutEnabled(true)
                        .setIdleTimeoutMs(5000)
                        .build())
                .setCapacityPolicy(QueueCapacityPolicy.newBuilder()
                        .setLimited(true)
                        .setMaxMessages(100)
                        .setOverflowPolicy(OverflowPolicy.DROP_OLDEST)
                        .build())
                .setDispatchPolicy(DispatchPolicy.newBuilder()
                        .setPrefetchCount(5)
                        .setConsumerSelectionStrategy(ConsumerSelectionStrategy.LEAST_INFLIGHT)
                        .build())
                .build();

        com.volo.broker.broker.queue.settings.QueueSettings result =
                GrpcQueueSettingsConverter.toDomain(proto);

        assertThat(result.persistenceMode())
                .isEqualTo(com.volo.broker.broker.queue.settings.PersistenceMode.MEMORY_ONLY);

        assertThat(result.publishAckMode())
                .isEqualTo(com.volo.broker.broker.queue.settings.PublishAckMode.NONE);

        assertThat(result.consumerAckMode())
                .isEqualTo(com.volo.broker.broker.queue.settings.ConsumerAckMode.NONE);

        assertThat(result.retentionPolicy().ttlEnabled()).isTrue();
        assertThat(result.retentionPolicy().ttlMs()).isEqualTo(1000);

        assertThat(result.lifecyclePolicy().autoDelete()).isTrue();
        assertThat(result.lifecyclePolicy().idleTimeoutEnabled()).isTrue();

        assertThat(result.capacityPolicy().limited()).isTrue();
        assertThat(result.capacityPolicy().maxMessages()).isEqualTo(100);
        assertThat(result.capacityPolicy().overflowPolicy())
                .isEqualTo(com.volo.broker.broker.queue.settings.OverflowPolicy.DROP_OLDEST);

        assertThat(result.dispatchPolicy().prefetchCount()).isEqualTo(5);
        assertThat(result.dispatchPolicy().consumerSelectionStrategy())
                .isEqualTo(com.volo.broker.broker.queue.settings.ConsumerSelectionStrategy.LEAST_INFLIGHT);
    }

    @Test
    void toDomain_shouldUseDefaultsForUnspecifiedValues() {
        QueueSettings proto = QueueSettings.newBuilder().build();

        com.volo.broker.broker.queue.settings.QueueSettings result =
                GrpcQueueSettingsConverter.toDomain(proto);

        assertThat(result.persistenceMode())
                .isEqualTo(com.volo.broker.broker.queue.settings.PersistenceMode.PERSISTENT);

        assertThat(result.publishAckMode())
                .isEqualTo(com.volo.broker.broker.queue.settings.PublishAckMode.ENABLED);

        assertThat(result.consumerAckMode())
                .isEqualTo(com.volo.broker.broker.queue.settings.ConsumerAckMode.ENABLED);
    }
}