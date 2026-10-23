package com.volo.broker.broker.services;

import com.volo.broker.TestConsumerChannel;
import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.model.NackCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.subscription.Subscription;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class NackRedeliveryPolicyTest {

    private final QueueManager queueManager = new InMemoryQueueManager();
    private final SubscriptionManager subscriptionManager = new DefaultSubscriptionManager();
    private final QueueDispatchScheduler dispatchScheduler = mock(QueueDispatchScheduler.class);
    private final MessageStore messageStore = mock(MessageStore.class);
    private final MessageLifecycleService lifecycleService = new MessageLifecycleService(messageStore);

    private final ConsumerAcknowledgementService service = new ConsumerAcknowledgementService(
            subscriptionManager,
            queueManager,
            dispatchScheduler,
            lifecycleService
    );

    @Test
    void nackWithRequeue_shouldCompleteMessageWhenMaxAttemptsReached() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        TestConsumerChannel channel = new TestConsumerChannel();
        subscriptionManager.register("orders.created", channel, 10);

        Subscription subscription = subscriptionManager.findByConsumerTag(channel.consumerTag())
                .orElseThrow();

        QueuedMessage message = new QueuedMessage(
                100L,
                "payload".getBytes(),
                Map.of(),
                "orders.created",
                System.currentTimeMillis(),
                true,
                5
        );

        subscription.track(1L, message);

        service.nack(new NackCommand(
                channel.consumerTag(),
                1L,
                100L,
                true
        ));

        assertThat(queueManager.require("orders.created").size())
                .isZero();

        verify(messageStore).delete(100L);
        verify(dispatchScheduler).signal("orders.created");
    }

    @Test
    void nackWithRequeue_shouldRequeueWhenAttemptsAreBelowLimit() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        TestConsumerChannel channel = new TestConsumerChannel();
        subscriptionManager.register("orders.created", channel, 10);

        Subscription subscription = subscriptionManager.findByConsumerTag(channel.consumerTag())
                .orElseThrow();

        subscription.track(1L, TestMessages.message(100));

        service.nack(new NackCommand(
                channel.consumerTag(),
                1L,
                100L,
                true
        ));

        assertThat(queueManager.require("orders.created").size())
                .isEqualTo(1);

        QueuedMessage redelivered = queueManager.require("orders.created").poll().message().orElseThrow();

        assertThat(redelivered.getMessageId()).isEqualTo(100);
        assertThat(redelivered.isRedelivered()).isTrue();
        assertThat(redelivered.getDeliveryAttempt()).isEqualTo(1);

        verify(messageStore, never()).delete(anyLong());
        verify(dispatchScheduler).signal("orders.created");
    }
}