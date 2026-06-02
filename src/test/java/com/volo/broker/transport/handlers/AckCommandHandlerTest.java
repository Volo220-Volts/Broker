package com.volo.broker.transport.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
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

import com.volo.broker.broker.command.handler.AckCommandHandler;
import com.volo.broker.broker.command.model.AckCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.subscription.ConsumerChannel;
import com.volo.broker.broker.subscription.Subscription;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;

@ExtendWith(MockitoExtension.class)
class AckCommandHandlerTest {

    @Mock
    SubscriptionManager subscriptionManager;

    @Mock
    QueueDispatchScheduler dispatchScheduler;

    @Mock
    ConsumerChannel channel;

    @Mock
    MessageStore messageStore;

    AckCommandHandler handler;

    @BeforeEach
    void setUp() {
        handler = new AckCommandHandler(dispatchScheduler, subscriptionManager, messageStore);
    }

    @Test
    void handle_shouldRemoveMessageFromInflightAndSignalDispatch() {
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

        AckCommand command = new AckCommand(consumerTag, deliveryTag, 1L);

        handler.handle(command);

        assertEquals(0, subscription.getInFlightTracker().size());
        verify(dispatchScheduler).signal(queueName);
    }

    @Test
    void handle_shouldNotSignalWhenDeliveryTagNotFound() {
        String queueName = "orders";
        String consumerTag = "consumer-1";

        Subscription subscription = new Subscription(
                consumerTag,
                queueName,
                10,
                channel
        );

        when(subscriptionManager.findByConsumerTag(consumerTag))
                .thenReturn(Optional.of(subscription));

        AckCommand command = new AckCommand(consumerTag, 999L, 1L);

        handler.handle(command);

        verify(dispatchScheduler, never()).signal(anyString());
    }
}
