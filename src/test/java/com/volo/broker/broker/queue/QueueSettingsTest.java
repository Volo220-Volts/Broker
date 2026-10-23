package com.volo.broker.broker.queue;

import com.volo.broker.broker.queue.settings.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class QueueSettingsTest {

    @Test
    void defaults_shouldBeReliableQueueSettings() {
        QueueSettings settings = QueueSettings.defaults();

        assertThat(settings.persistenceMode()).isEqualTo(PersistenceMode.PERSISTENT);
        assertThat(settings.publishAckMode()).isEqualTo(PublishAckMode.ENABLED);
        assertThat(settings.consumerAckMode()).isEqualTo(ConsumerAckMode.ENABLED);
        assertThat(settings.redeliveryPolicy().enabled()).isTrue();
        assertThat(settings.retentionPolicy().ttlEnabled()).isFalse();
        assertThat(settings.capacityPolicy().limited()).isFalse();
        assertThat(settings.dispatchPolicy().prefetchCount()).isPositive();
    }

    @Test
    void constructor_shouldRejectPersistentQueueWithoutConsumerAck() {
        assertThatThrownBy(() -> new QueueSettings(
                PersistenceMode.PERSISTENT,
                PublishAckMode.ENABLED,
                ConsumerAckMode.NONE,
                RedeliveryPolicy.disabled(),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                QueueCapacityPolicy.unlimited(),
                DispatchPolicy.defaults()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PERSISTENT");
    }

    @Test
    void constructor_shouldRejectRedeliveryWhenConsumerAckDisabled() {
        assertThatThrownBy(() -> new QueueSettings(
                PersistenceMode.MEMORY_ONLY,
                PublishAckMode.ENABLED,
                ConsumerAckMode.NONE,
                RedeliveryPolicy.defaults(),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                QueueCapacityPolicy.unlimited(),
                DispatchPolicy.defaults()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Redelivery");
    }

    @Test
    void messageRetentionPolicy_shouldRejectEnabledTtlWithInvalidValue() {
        assertThatThrownBy(() -> new MessageRetentionPolicy(true, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ttlMs");
    }

    @Test
    void queueCapacityPolicy_shouldRejectLimitedQueueWithoutLimits() {
        assertThatThrownBy(() -> new QueueCapacityPolicy(
                true,
                0,
                0,
                OverflowPolicy.REJECT_PUBLISH
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capacity");
    }

    @Test
    void dispatchPolicy_shouldFallbackToDefaultPrefetchWhenInvalid() {
        DispatchPolicy policy = new DispatchPolicy(0, ConsumerSelectionStrategy.ROUND_ROBIN);

        assertThat(policy.prefetchCount()).isPositive();
        assertThat(policy.consumerSelectionStrategy()).isEqualTo(ConsumerSelectionStrategy.ROUND_ROBIN);
    }

    @Test
    void dispatchPolicy_shouldFallbackToRoundRobinWhenStrategyIsNull() {
        DispatchPolicy policy = new DispatchPolicy(5, null);

        assertThat(policy.prefetchCount()).isEqualTo(5);
        assertThat(policy.consumerSelectionStrategy()).isEqualTo(ConsumerSelectionStrategy.ROUND_ROBIN);
    }

    @Test
    void defaults_shouldHaveDisabledFlowControlPolicy() {
        QueueSettings settings = QueueSettings.defaults();

        assertThat(settings.flowControlPolicy()).isNotNull();
        assertThat(settings.flowControlPolicy().enabled()).isFalse();
    }

    @Test
    void constructor_shouldRejectFlowControlHwmGreaterOrEqualToCapacityMessages() {
        QueueCapacityPolicy capacity = new QueueCapacityPolicy(true, 100, 0, OverflowPolicy.REJECT_PUBLISH);
        FlowControlPolicy flow = new FlowControlPolicy(true, 100, 50, 0, 0, 0);

        assertThatThrownBy(() -> new QueueSettings(
                PersistenceMode.PERSISTENT,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                RedeliveryPolicy.defaults(),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                capacity,
                DispatchPolicy.defaults(),
                flow
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("highWatermarkMessages must be strictly less than capacityPolicy maxMessages");
    }

    @Test
    void constructor_shouldRejectFlowControlHwmGreaterOrEqualToCapacityBytes() {
        QueueCapacityPolicy capacity = new QueueCapacityPolicy(true, 0, 1024, OverflowPolicy.REJECT_PUBLISH);
        FlowControlPolicy flow = new FlowControlPolicy(true, 0, 0, 2048, 1024, 0);

        assertThatThrownBy(() -> new QueueSettings(
                PersistenceMode.PERSISTENT,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                RedeliveryPolicy.defaults(),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                capacity,
                DispatchPolicy.defaults(),
                flow
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("highWatermarkBytes must be strictly less than capacityPolicy maxBytes");
    }

    @Test
    void constructor_shouldAcceptFlowControlHwmLessThanCapacity() {
        QueueCapacityPolicy capacity = new QueueCapacityPolicy(true, 100, 1024, OverflowPolicy.REJECT_PUBLISH);
        FlowControlPolicy flow = new FlowControlPolicy(true, 80, 40, 512, 256, 1000);

        QueueSettings settings = new QueueSettings(
                PersistenceMode.PERSISTENT,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                RedeliveryPolicy.defaults(),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                capacity,
                DispatchPolicy.defaults(),
                flow
        );

        assertThat(settings.flowControlPolicy().enabled()).isTrue();
        assertThat(settings.flowControlPolicy().highWatermarkMessages()).isEqualTo(80);
    }
}