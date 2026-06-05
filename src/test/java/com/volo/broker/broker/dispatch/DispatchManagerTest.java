package com.volo.broker.broker.dispatch;

import com.volo.broker.TestConsumerChannel;
import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DispatchManagerTest {

    private final QueueManager queueManager = new InMemoryQueueManager();
    private final SubscriptionManager subscriptionManager = new DefaultSubscriptionManager();
    private final MessageStore messageStore = mock(MessageStore.class);
    private final MessageLifecycleService lifecycleService = new MessageLifecycleService(messageStore);

    private final DispatchManager dispatchManager = new DispatchManager(
            queueManager,
            subscriptionManager,
            lifecycleService
    );

    @Test
    void dispatchLoop_shouldDeliverMessageToConsumer() {
        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.reliable());
        queueManager.require("orders.created").enqueue(TestMessages.message(1));

        TestConsumerChannel channel = new TestConsumerChannel();
        subscriptionManager.register("orders.created", channel, 10);

        dispatchManager.dispatchLoop("orders.created");

        assertThat(channel.deliveries()).hasSize(1);
        assertThat(channel.lastDelivery().message().getMessageId()).isEqualTo(1);
    }

    @Test
    void dispatchLoop_shouldRespectPrefetch() {
        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.withPrefetch(1));
        queueManager.require("orders.created").enqueue(TestMessages.message(1));
        queueManager.require("orders.created").enqueue(TestMessages.message(2));

        TestConsumerChannel channel = new TestConsumerChannel();
        subscriptionManager.register("orders.created", channel, 1);

        dispatchManager.dispatchLoop("orders.created");

        assertThat(channel.deliveries()).hasSize(1);
        assertThat(queueManager.require("orders.created").size()).isEqualTo(1);
    }

    @Test
    void dispatchLoop_shouldCompleteImmediatelyWhenConsumerAckDisabled() {
        queueManager.declare(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.memoryOnlyNoConsumerAck()
        );

        queueManager.require("metrics.runtime").enqueue(TestMessages.message(1));

        TestConsumerChannel channel = new TestConsumerChannel();
        subscriptionManager.register("metrics.runtime", channel, 10);

        dispatchManager.dispatchLoop("metrics.runtime");

        assertThat(channel.deliveries()).hasSize(1);
        assertThat(queueManager.require("metrics.runtime").size()).isZero();

        verifyNoInteractions(messageStore);
    }

    @Test
    void dispatchLoop_shouldDropExpiredMessageBeforeDelivery() {
        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.withTtl(1));
        queueManager.require("orders.created").enqueue(TestMessages.expiredMessage(1));

        TestConsumerChannel channel = new TestConsumerChannel();
        subscriptionManager.register("orders.created", channel, 10);

        dispatchManager.dispatchLoop("orders.created");

        assertThat(channel.deliveries()).isEmpty();
        verify(messageStore).delete(1L);
    }

    // @Test
    // void dispatchLoop_shouldRequeueOnSendFailureWhenPolicyEnabled() {
    //     queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.reliable());
    //     queueManager.require("orders.created").enqueue(TestMessages.message(1));

    //     TestConsumerChannel channel = new TestConsumerChannel();
    //     channel.failOnSend();

    //     subscriptionManager.register("orders.created", channel, 10);

    //     dispatchManager.dispatchLoop("orders.created");

    //     assertThat(queueManager.require("orders.created").size()).isEqualTo(1);

    //     verify(messageStore, never()).delete(anyLong());
    // }
}