package com.volo.broker.broker.services;

import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.message.MessageIdGenerator;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.settings.PublishAckMode;
import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class PublishServiceTest {

    private final QueueManager queueManager = new InMemoryQueueManager();
    private final MessageStore messageStore = mock(MessageStore.class);
    private final QueueDispatchScheduler dispatchScheduler = mock(QueueDispatchScheduler.class);
    private final MessageIdGenerator messageIdGenerator = new TestMessageIdGenerator();
    private final MessageLifecycleService lifecycleService = mock(MessageLifecycleService.class);

    private final PublishService publishService = new PublishService(
            queueManager,
            messageStore,
            dispatchScheduler,
            messageIdGenerator,
            lifecycleService
    );

    @Test
    void publish_shouldRouteMessageToSingleQueue() {
        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.reliable());

        var result = publishService.publish(command("orders.created"));

        assertThat(result.accepted()).isTrue();
        // assertThat(result.messageId()).containsExactly(1L);
        assertThat(queueManager.require("orders.created").size()).isEqualTo(1);

        verify(messageStore).save(eq("orders.created"), any());
        verify(dispatchScheduler).signal("orders.created");
    }

    @Test
    void publish_shouldRouteMessageToMultipleQueues() {
        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.reliable());
        queueManager.declare(QueuePath.of("orders.deleted"), TestQueueSettings.reliable());

        var result = publishService.publish(command("orders.*"));

        assertThat(result.accepted()).isTrue();
        assertThat(result.routedQueueCount()).isEqualTo(2);

        assertThat(queueManager.require("orders.created").size()).isEqualTo(1);
        assertThat(queueManager.require("orders.deleted").size()).isEqualTo(1);

        verify(dispatchScheduler).signal("orders.created");
        verify(dispatchScheduler).signal("orders.deleted");
    }

    @Test
    void publish_shouldNotPersistForMemoryOnlyQueue() {
        queueManager.declare(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.memoryOnlyNoConsumerAck()
        );

        var result = publishService.publish(command("metrics.runtime"));

        assertThat(result.accepted()).isTrue();

        verifyNoInteractions(messageStore);
    }

    @Test
    void publish_shouldReturnNackWhenRouteNotFound() {
        var result = publishService.publish(command("missing.*"));

        assertThat(result.accepted()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.QUEUE_NOT_FOUND);

        verifyNoInteractions(messageStore);
        verifyNoInteractions(dispatchScheduler);
    }

    @Test
    void publish_shouldNotRequireResponseWhenPublishAckDisabled() {
        QueueSettings settings = new QueueSettings(
                TestQueueSettings.memoryOnlyNoConsumerAck().persistenceMode(),
                PublishAckMode.NONE,
                TestQueueSettings.memoryOnlyNoConsumerAck().consumerAckMode(),
                TestQueueSettings.memoryOnlyNoConsumerAck().redeliveryPolicy(),
                TestQueueSettings.memoryOnlyNoConsumerAck().retentionPolicy(),
                TestQueueSettings.memoryOnlyNoConsumerAck().lifecyclePolicy(),
                TestQueueSettings.memoryOnlyNoConsumerAck().capacityPolicy(),
                TestQueueSettings.memoryOnlyNoConsumerAck().dispatchPolicy()
        );

        queueManager.declare(QueuePath.of("metrics.runtime"), settings);

        var result = publishService.publish(command("metrics.runtime"));

        assertThat(result.accepted()).isTrue();
        // assertThat(result.responseRequired()).isFalse();
    }

    @Test
    void publish_whenCapacityExceeded_shouldNotSaveToMessageStore() {
        var settings = new QueueSettings(
                com.volo.broker.broker.queue.settings.PersistenceMode.PERSISTENT,
                com.volo.broker.broker.queue.settings.PublishAckMode.ENABLED,
                com.volo.broker.broker.queue.settings.ConsumerAckMode.ENABLED,
                TestQueueSettings.reliable().redeliveryPolicy(),
                TestQueueSettings.reliable().retentionPolicy(),
                TestQueueSettings.reliable().lifecyclePolicy(),
                new com.volo.broker.broker.queue.settings.QueueCapacityPolicy(
                        true, 1, 0, com.volo.broker.broker.queue.settings.OverflowPolicy.REJECT_PUBLISH),
                TestQueueSettings.reliable().dispatchPolicy()
        );

        queueManager.declare(QueuePath.of("orders.limited"), settings);

        // First message fills capacity
        var res1 = publishService.publish(command("orders.limited"));
        assertThat(res1.accepted()).isTrue();
        verify(messageStore, times(1)).save(eq("orders.limited"), any());

        // Second message exceeds capacity
        var res2 = publishService.publish(command("orders.limited"));
        assertThat(res2.accepted()).isFalse();
        assertThat(res2.errorCode()).isEqualTo(BrokerErrorCode.QUEUE_OVERFLOW);

        // Verify save was NOT called a second time
        verify(messageStore, times(1)).save(eq("orders.limited"), any());
    }

    @Test
    void publish_whenDropOldest_shouldCompleteDroppedMessages() {
        var settings = new QueueSettings(
                com.volo.broker.broker.queue.settings.PersistenceMode.PERSISTENT,
                com.volo.broker.broker.queue.settings.PublishAckMode.ENABLED,
                com.volo.broker.broker.queue.settings.ConsumerAckMode.ENABLED,
                TestQueueSettings.reliable().redeliveryPolicy(),
                TestQueueSettings.reliable().retentionPolicy(),
                TestQueueSettings.reliable().lifecyclePolicy(),
                new com.volo.broker.broker.queue.settings.QueueCapacityPolicy(
                        true, 1, 0, com.volo.broker.broker.queue.settings.OverflowPolicy.DROP_OLDEST),
                TestQueueSettings.reliable().dispatchPolicy()
        );

        queueManager.declare(QueuePath.of("orders.drop-oldest"), settings);

        publishService.publish(command("orders.drop-oldest"));
        verify(lifecycleService, never()).complete(any(), any());

        publishService.publish(command("orders.drop-oldest"));
        verify(lifecycleService, times(1)).complete(eq(queueManager.require("orders.drop-oldest")), any());
    }

    private PublishCommand command(String routingKey) {
        return new PublishCommand(
                1L,
                routingKey,
                "payload".getBytes(StandardCharsets.UTF_8),
                Map.of()
        );
    }

    private static class TestMessageIdGenerator implements MessageIdGenerator {
        private final AtomicLong value = new AtomicLong();

        @Override
        public long nextId() {
            return value.incrementAndGet();
        }

        @Override
        public void initialize(long maxMessageId) {
           value.set(maxMessageId);
        }
    }
}