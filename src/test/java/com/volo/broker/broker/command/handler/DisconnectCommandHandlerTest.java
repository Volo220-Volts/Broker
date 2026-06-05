package com.volo.broker.broker.command.handler;

import com.volo.broker.TestConsumerChannel;
import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.model.DisconnectCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.subscription.Subscription;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DisconnectCommandHandlerTest {

    private final QueueManager queueManager = new InMemoryQueueManager();
    private final SubscriptionManager subscriptionManager = new DefaultSubscriptionManager();
    private final QueueDispatchScheduler dispatchScheduler = mock(QueueDispatchScheduler.class);
    private final MessageStore messageStore = mock(MessageStore.class);
    private final MessageLifecycleService lifecycleService = new MessageLifecycleService(messageStore);

    private final DisconnectCommandHandler handler = new DisconnectCommandHandler(
            subscriptionManager,
            queueManager,
            dispatchScheduler,
            lifecycleService
    );

    @Test
    void disconnect_shouldDrainInflightAndRequeueInOriginalOrder() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        TestConsumerChannel channel = new TestConsumerChannel();
        subscriptionManager.register("orders.created", channel, 10);

        Subscription subscription = subscriptionManager.findByConsumerTag(channel.consumerTag())
                .orElseThrow();

        subscription.track(1L, TestMessages.message(100));
        subscription.track(2L, TestMessages.message(200));

        handler.handle(new DisconnectCommand(channel.consumerTag()));

        assertThat(subscriptionManager.findByConsumerTag(channel.consumerTag()))
                .isEmpty();

        assertThat(queueManager.require("orders.created").size())
                .isEqualTo(2);

        QueuedMessage first = queueManager.require("orders.created").poll().orElseThrow();
        QueuedMessage second = queueManager.require("orders.created").poll().orElseThrow();

        assertThat(first.getMessageId()).isEqualTo(100);
        assertThat(second.getMessageId()).isEqualTo(200);

        assertThat(first.isRedelivered()).isTrue();
        assertThat(second.isRedelivered()).isTrue();

        verify(dispatchScheduler).signal("orders.created");
        verify(messageStore, never()).delete(anyLong());
    }

    @Test
    void disconnect_shouldCompleteInflightMessagesWhenRedeliveryDisabled() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliableWithRedeliveryDisabled()
        );

        TestConsumerChannel channel = new TestConsumerChannel();
        subscriptionManager.register("orders.created", channel, 10);

        Subscription subscription = subscriptionManager.findByConsumerTag(channel.consumerTag())
                .orElseThrow();

        subscription.track(1L, TestMessages.message(100));
        subscription.track(2L, TestMessages.message(200));

        handler.handle(new DisconnectCommand(channel.consumerTag()));

        assertThat(queueManager.require("orders.created").size())
                .isZero();

        verify(messageStore).delete(100L);
        verify(messageStore).delete(200L);
        verify(dispatchScheduler).signal("orders.created");
    }

    @Test
    void disconnect_shouldDoNothingForUnknownConsumer() {
        handler.handle(new DisconnectCommand("missing-consumer"));

        verifyNoInteractions(dispatchScheduler);
        verifyNoInteractions(messageStore);
    }
}