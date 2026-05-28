package com.volo.broker.transport.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.mockito.ArgumentCaptor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.volo.broker.domain.subscription.ConsumerChannel;
import com.volo.broker.core.command.handler.DisconnectCommandHandler;
import com.volo.broker.core.command.model.DisconnectCommand;
import com.volo.broker.core.dispatch.QueueDispatchScheduler;
import com.volo.broker.domain.message.DeliveryGuarantee;
import com.volo.broker.domain.queue.QueueManager;
import com.volo.broker.domain.message.QueuedMessage;
import com.volo.broker.domain.subscription.Subscription;
import com.volo.broker.domain.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;

@ExtendWith(MockitoExtension.class)
class DisconnectCommandHandlerTest {

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

    DisconnectCommandHandler handler;

    @BeforeEach
    void setUp() {
        handler = new DisconnectCommandHandler(
                subscriptionManager,
                queueManager,
                dispatchScheduler
        );
    }

    @Test
    void handle_shouldDrainInflightAndRequeueInReverseOrder() {
        String queueName = "orders";
        String consumerTag = "consumer-1";

        Subscription subscription = new Subscription(
                consumerTag,
                queueName,
                10,
                channel
        );

        QueuedMessage msg1 = message(1L, queueName);
        QueuedMessage msg2 = message(2L, queueName);
        QueuedMessage msg3 = message(3L, queueName);

        long tag1 = subscription.nextDeliveryTag();
        subscription.track(tag1, msg1);

        long tag2 = subscription.nextDeliveryTag();
        subscription.track(tag2, msg2);

        long tag3 = subscription.nextDeliveryTag();
        subscription.track(tag3, msg3);

        when(subscriptionManager.findByConsumerTag(consumerTag))
                .thenReturn(Optional.of(subscription));

        DisconnectCommand command = new DisconnectCommand(consumerTag);

        handler.handle(command);

        verify(subscriptionManager).unregister(consumerTag);

        ArgumentCaptor<QueuedMessage> messageCaptor = ArgumentCaptor.forClass(QueuedMessage.class);
        verify(queueManager, times(3)).requeue(eq(queueName), messageCaptor.capture());
        
        List<QueuedMessage> requeued = messageCaptor.getAllValues();
        assertEquals(3, requeued.size());
        assertEquals(3, requeued.get(0).getMessageId());
        assertEquals(2, requeued.get(1).getMessageId());
        assertEquals(1, requeued.get(2).getMessageId());

        verify(dispatchScheduler).signal(queueName);
        assertEquals(0, subscription.getInFlightTracker().size());
    }

    private QueuedMessage message(long id, String queueName) {
        return new QueuedMessage(
                id,
                ("payload-" + id).getBytes(),
                Map.of(),
                queueName,
                System.currentTimeMillis(),
                DeliveryGuarantee.AT_LEAST_ONCE,
                false,
                0
        );
    }
}
