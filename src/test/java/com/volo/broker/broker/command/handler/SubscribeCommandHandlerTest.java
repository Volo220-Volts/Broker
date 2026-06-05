package com.volo.broker.broker.command.handler;

import com.volo.broker.TestConsumerChannel;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.model.SubscribeCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.subscription.SubscriptionManager;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SubscribeCommandHandlerTest {

    private final QueueManager queueManager = new InMemoryQueueManager();
    private final SubscriptionManager subscriptionManager = new DefaultSubscriptionManager();
    private final QueueDispatchScheduler dispatchScheduler = mock(QueueDispatchScheduler.class);

    private final SubscribeCommandHandler handler = new SubscribeCommandHandler(
            queueManager,
            dispatchScheduler,
            subscriptionManager
    );

    @Test
    void handle_shouldRegisterConsumerAndSignalDispatch() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.withPrefetch(5)
        );

        TestConsumerChannel channel = new TestConsumerChannel();

        handler.handle(new SubscribeCommand(
                "orders.created",
                3,
                channel.consumerTag(),
                channel
        ));

        var subscription = subscriptionManager.findByConsumerTag(channel.consumerTag());

        assertThat(subscription).isPresent();
        assertThat(subscription.get().queueName()).isEqualTo("orders.created");
        assertThat(subscription.get().getPrefetchCount()).isEqualTo(3);

        verify(dispatchScheduler).signal("orders.created");
    }

    @Test
    void handle_shouldUseQueuePrefetchWhenClientPrefetchIsInvalid() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.withPrefetch(5)
        );

        TestConsumerChannel channel = new TestConsumerChannel();

        handler.handle(new SubscribeCommand(
                "orders.created",
                -1,
                channel.consumerTag(),
                channel
        ));

        var subscription = subscriptionManager.findByConsumerTag(channel.consumerTag())
                .orElseThrow();

        assertThat(subscription.getPrefetchCount()).isEqualTo(5);
    }

    @Test
    void handle_shouldLimitClientPrefetchByQueuePrefetch() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.withPrefetch(5)
        );

        TestConsumerChannel channel = new TestConsumerChannel();

        handler.handle(new SubscribeCommand(
                "orders.created",
                100,
                channel.consumerTag(),
                channel
        ));

        var subscription = subscriptionManager.findByConsumerTag(channel.consumerTag())
                .orElseThrow();

        assertThat(subscription.getPrefetchCount()).isEqualTo(5);
    }

    @Test
    void handle_shouldThrowWhenQueueDoesNotExist() {
        TestConsumerChannel channel = new TestConsumerChannel();

        assertThatThrownBy(() -> handler.handle(new SubscribeCommand(
                "missing.queue",
                10,
                channel.consumerTag(),
                channel
        ))).isInstanceOf(RuntimeException.class);

        verifyNoInteractions(dispatchScheduler);
    }
}