package com.volo.broker.broker.integration;

import com.volo.broker.TestConsumerChannel;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.model.AckCommand;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.dispatch.DispatchManager;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.MessageIdGenerator;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.services.ConsumerAcknowledgementService;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.broker.services.PublishService;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class InMemoryBrokerFlowTest {

    @Test
    void publishDispatchAck_shouldCompleteMessage() {
        QueueManager queueManager = new InMemoryQueueManager();
        SubscriptionManager subscriptionManager = new DefaultSubscriptionManager();
        MessageStore messageStore = mock(MessageStore.class);
        QueueDispatchScheduler scheduler = mock(QueueDispatchScheduler.class);

        PublishService publishService = new PublishService(
                queueManager,
                messageStore,
                scheduler,
                new TestMessageIdGenerator()
        );

        MessageLifecycleService lifecycleService = new MessageLifecycleService(messageStore);

        DispatchManager dispatchManager = new DispatchManager(
                queueManager,
                subscriptionManager,
                lifecycleService
        );

        ConsumerAcknowledgementService acknowledgementService = new ConsumerAcknowledgementService(
                subscriptionManager,
                queueManager,
                scheduler,
                lifecycleService
        );

        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.reliable());

        var publishResult = publishService.publish(new PublishCommand(
                1L,
                "orders.created",
                "payload".getBytes(StandardCharsets.UTF_8),
                Map.of()
        ));

        assertThat(publishResult.accepted()).isTrue();

        TestConsumerChannel channel = new TestConsumerChannel();
        subscriptionManager.register("orders.created", channel, 10);

        dispatchManager.dispatchLoop("orders.created");

        assertThat(channel.deliveries()).hasSize(1);

        long deliveryTag = channel.lastDelivery().deliveryTag();
        long messageId = channel.lastDelivery().message().getMessageId();

        acknowledgementService.ack(new AckCommand(
                channel.consumerTag(),
                deliveryTag,
                messageId
        ));

        verify(messageStore).save(eq("orders.created"), any());
        verify(messageStore).delete(messageId);
    }

    @Test
    void publishWithWildcard_shouldDeliverToAllMatchedQueues() {
        QueueManager queueManager = new InMemoryQueueManager();
        SubscriptionManager subscriptionManager = new DefaultSubscriptionManager();
        MessageStore messageStore = mock(MessageStore.class);
        QueueDispatchScheduler scheduler = mock(QueueDispatchScheduler.class);

        PublishService publishService = new PublishService(
                queueManager,
                messageStore,
                scheduler,
                new TestMessageIdGenerator()
        );

        DispatchManager dispatchManager = new DispatchManager(
                queueManager,
                subscriptionManager,
                new MessageLifecycleService(messageStore)
        );

        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.reliable());
        queueManager.declare(QueuePath.of("orders.deleted"), TestQueueSettings.reliable());

        publishService.publish(new PublishCommand(
                1L,
                "orders.*",
                "payload".getBytes(StandardCharsets.UTF_8),
                Map.of()
        ));

        TestConsumerChannel createdConsumer = new TestConsumerChannel();
        TestConsumerChannel deletedConsumer = new TestConsumerChannel();

        subscriptionManager.register("orders.created", createdConsumer, 10);
        subscriptionManager.register("orders.deleted", deletedConsumer, 10);

        dispatchManager.dispatchLoop("orders.created");
        dispatchManager.dispatchLoop("orders.deleted");

        assertThat(createdConsumer.deliveries()).hasSize(1);
        assertThat(deletedConsumer.deliveries()).hasSize(1);
    }

    private static class TestMessageIdGenerator implements MessageIdGenerator {
        private final AtomicLong value = new AtomicLong();

        @Override
        public long nextId() {
            return value.incrementAndGet();
        }
    }
}
