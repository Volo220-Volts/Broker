package com.volo.broker.broker.services;

import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.settings.*;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.persistence.MessageStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.Map;

class MessageLifecycleServiceTest {

    private final MessageStore messageStore = mock(MessageStore.class);
    private final MessageLifecycleService service = new MessageLifecycleService(messageStore);

    @Test
    void isExpired_shouldReturnTrueWhenTtlExceeded() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.withTtl(1)
        );

        assertThat(service.isExpired(queue, TestMessages.expiredMessage(1)))
                .isTrue();
    }

    @Test
    void isExpired_shouldReturnFalseWhenTtlDisabled() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        assertThat(service.isExpired(queue, TestMessages.expiredMessage(1)))
                .isFalse();
    }

    @Test
    void complete_shouldDeletePersistentMessage() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        service.complete(queue, TestMessages.message(100));

        verify(messageStore).delete(100L);
    }

    @Test
    void complete_shouldNotDeleteMemoryOnlyMessage() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.memoryOnlyNoConsumerAck()
        );

        service.complete(queue, TestMessages.message(100));

        verifyNoInteractions(messageStore);
    }

    @Test
    void canRedeliver_shouldReturnTrueWhenAttemptsBelowLimit() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        assertThat(service.canRedeliver(queue, TestMessages.message(1)))
                .isTrue();
    }

    @Test
    void canRedeliver_shouldReturnFalseWhenRedeliveryDisabled() {
        QueueSettings settings = new QueueSettings(
                PersistenceMode.MEMORY_ONLY,
                PublishAckMode.ENABLED,
                ConsumerAckMode.NONE,
                RedeliveryPolicy.disabled(),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                QueueCapacityPolicy.unlimited(),
                DispatchPolicy.defaults()
        );

        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("metrics.runtime"),
                settings
        );

        assertThat(service.canRedeliver(queue, TestMessages.message(1)))
                .isFalse();
    }

    @Test
    void canRedeliver_shouldReturnFalseWhenMaxAttemptsReached() {
        QueueSettings settings = new QueueSettings(
                PersistenceMode.MEMORY_ONLY,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                new RedeliveryPolicy(true, 2, true, true),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                QueueCapacityPolicy.unlimited(),
                DispatchPolicy.defaults()
        );

        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                settings
        );

        QueuedMessage message = new QueuedMessage(
                1L,
                "payload".getBytes(),
                Map.of(),
                "orders.created",
                System.currentTimeMillis(),
                true,
                2
        );

        assertThat(service.canRedeliver(queue, message))
                .isFalse();
    }

    @Test
    void canRedeliver_shouldReturnTrueWhenMaxAttemptsNotReached() {
        QueueSettings settings = new QueueSettings(
                PersistenceMode.MEMORY_ONLY,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                new RedeliveryPolicy(true, 3, true, true),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                QueueCapacityPolicy.unlimited(),
                DispatchPolicy.defaults()
        );

        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                settings
        );

        QueuedMessage message = new QueuedMessage(
                1L,
                "payload".getBytes(),
                Map.of(),
                "orders.created",
                System.currentTimeMillis(),
                true,
                2
        );

        assertThat(service.canRedeliver(queue, message))
                .isTrue();
    }
}
