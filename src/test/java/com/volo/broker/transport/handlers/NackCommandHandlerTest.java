package com.volo.broker.transport.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.volo.broker.broker.command.handler.NackCommandHandler;
import com.volo.broker.broker.command.model.NackCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.subscription.ConsumerChannel;
import com.volo.broker.broker.subscription.Subscription;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;

@ExtendWith(MockitoExtension.class)
class NackCommandHandlerTest {

    @Mock
    SubscriptionManager subscriptionManager;

    @Mock
    QueueManager queueManager;

    @Mock
    QueueDispatchScheduler dispatchScheduler;

    @Mock
    ConsumerChannel channel;

    @Mock
    MessageStore messageStore;

    NackCommandHandler handler;

    @BeforeEach
    void setUp() {
        handler = new NackCommandHandler(
                subscriptionManager,
                queueManager,
                dispatchScheduler,
                messageStore
        );
    }

    @Test
    void handle_shouldRequeueWhenRequeueTrue() {
        String queueName = "orders";
        String consumerTag = "consumer-1";

        Subscription subscription = new Subscription(
                consumerTag,
                queueName,
                10,
                channel
        );

        QueuedMessage message = new QueuedMessage(
                1L,
                "payload".getBytes(),
                Map.of(),
                queueName,
                System.currentTimeMillis(),
                DeliveryGuarantee.AT_LEAST_ONCE,
                false,
                0
        );

        long deliveryTag = subscription.nextDeliveryTag();
        subscription.track(deliveryTag, message);

        when(subscriptionManager.findByConsumerTag(consumerTag))
                .thenReturn(Optional.of(subscription));

        NackCommand command = new NackCommand(
                consumerTag,
                deliveryTag,
                1L,
                true
        );

        handler.handle(command);

        verify(queueManager).requeue(eq(queueName), any(QueuedMessage.class));
        verify(dispatchScheduler).signal(queueName);
        assertEquals(0, subscription.getInFlightTracker().size());
    }

    @Test
    void handle_shouldNotRequeueWhenRequeueFalse() {
        String queueName = "orders";
        String consumerTag = "consumer-1";

        Subscription subscription = new Subscription(
                consumerTag,
                queueName,
                10,
                channel
        );

        QueuedMessage message = new QueuedMessage(
                1L,
                "payload".getBytes(),
                Map.of(),
                queueName,
                System.currentTimeMillis(),
                DeliveryGuarantee.AT_LEAST_ONCE,
                false,
                0
        );

        long deliveryTag = subscription.nextDeliveryTag();
        subscription.track(deliveryTag, message);

        when(subscriptionManager.findByConsumerTag(consumerTag))
                .thenReturn(Optional.of(subscription));

        NackCommand command = new NackCommand(
                consumerTag,
                deliveryTag,
                1L,
                false
        );

        handler.handle(command);

        verify(queueManager, never()).requeue(eq(queueName), any(QueuedMessage.class));
        verify(dispatchScheduler).signal(queueName);
        assertEquals(0, subscription.getInFlightTracker().size());
    }
}
