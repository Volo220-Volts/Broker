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
}