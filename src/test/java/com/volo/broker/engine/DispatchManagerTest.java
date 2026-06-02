package com.volo.broker.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.volo.broker.broker.dispatch.DispatchManager;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.MessageDelivery;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.subscription.ConsumerChannel;
import com.volo.broker.broker.subscription.Subscription;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;

@ExtendWith(MockitoExtension.class)
public class DispatchManagerTest {
    @Mock
    QueueManager queueManager;

    @Mock
    SubscriptionManager subscriptionManager;

    @Mock
    ConsumerChannel channel;

    @Mock
    MessageStore messageStore;

    DispatchManager dispatchManager;

    @BeforeEach
    void setUp() {
        dispatchManager = new DispatchManager(queueManager, subscriptionManager);
    }

    @Test
    void dispatchLoop_shouldDeliverMessageAndTrackInflight() {
        String queueName = "orders";

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

        Subscription subscription = new Subscription(
                "consumer-1",
                queueName,
                10,
                channel
        );

        when(subscriptionManager.nextReady(queueName))
                .thenReturn(Optional.of(subscription))
                .thenReturn(Optional.empty());

        when(queueManager.poll(queueName))
                .thenReturn(Optional.of(message));

        when(channel.canAcceptMore()).thenReturn(true);

        dispatchManager.dispatchLoop(queueName);

        verify(channel, times(1)).send(any(MessageDelivery.class));

        assertEquals(1, subscription.getInFlightTracker().size());
    }

    @Test
    void dispatchLoop_shouldRequeueMessageWhenSendFails() {
        String queueName = "orders";

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

        Subscription subscription = new Subscription(
                "consumer-1",
                queueName,
                10,
                channel
        );

        when(subscriptionManager.nextReady(queueName))
                .thenReturn(Optional.of(subscription));

        when(queueManager.poll(queueName))
                .thenReturn(Optional.of(message))
                .thenReturn(Optional.empty());

        when(channel.canAcceptMore()).thenReturn(true);

        doThrow(new RuntimeException("stream closed"))
                .when(channel)
                .send(any(MessageDelivery.class));

        dispatchManager.dispatchLoop(queueName);

        verify(queueManager).requeue(eq(queueName), any(QueuedMessage.class));
        assertEquals(0, subscription.getInFlightTracker().size());
    }

    @Test
    void dispatchLoop_shouldDoNothingWhenNoReadySubscriber() {
        String queueName = "orders";

        when(subscriptionManager.nextReady(queueName))
                .thenReturn(Optional.empty());

        dispatchManager.dispatchLoop(queueName);

        verify(queueManager, never()).poll(anyString());
        verify(channel, never()).send(any());
    }

    @Test
    void dispatchLoop_shouldDispatchMessageAndTrackInflight() {
        String queueName = "orders";
        QueuedMessage message = message(1L, queueName);
        Subscription subscription = new Subscription("consumer-1", queueName, 10, channel);

        when(channel.canAcceptMore()).thenReturn(true);
        when(subscriptionManager.nextReady(queueName))
                .thenReturn(Optional.of(subscription))
                .thenReturn(Optional.empty());
        when(queueManager.poll(queueName)).thenReturn(Optional.of(message));

        dispatchManager.dispatchLoop(queueName);

        verify(channel).send(any(MessageDelivery.class));
        assertEquals(1, subscription.getInFlightTracker().size());
    }

    @Test
    void dispatchLoop_shouldRequeueWhenSendFails() {
        String queueName = "orders";
        QueuedMessage message = message(1L, queueName);
        Subscription subscription = new Subscription("consumer-1", queueName, 10, channel);

        when(channel.canAcceptMore()).thenReturn(true);
        when(subscriptionManager.nextReady(queueName)).thenReturn(Optional.of(subscription));
        when(queueManager.poll(queueName)).thenReturn(Optional.of(message)).thenReturn(Optional.empty());
        doThrow(new RuntimeException("stream closed")).when(channel).send(any(MessageDelivery.class));

        dispatchManager.dispatchLoop(queueName);

        verify(queueManager).requeue(eq(queueName), any(QueuedMessage.class));
        assertEquals(0, subscription.getInFlightTracker().size());
    }

    @Test
    void dispatchLoop_shouldRespectPrefetchLimit() {
        String queueName = "orders";

        QueuedMessage message1 = new QueuedMessage(
                1L,
                "payload-1".getBytes(),
                Map.of(),
                queueName,
                System.currentTimeMillis(),
                DeliveryGuarantee.AT_LEAST_ONCE,
                false,
                0
        );

        Subscription subscription = new Subscription(
                "consumer-1",
                queueName,
                1,
                channel
        );

        when(channel.canAcceptMore()).thenReturn(true);

        when(subscriptionManager.nextReady(queueName))
                .thenReturn(Optional.of(subscription))
                .thenReturn(Optional.empty());

        when(queueManager.poll(queueName))
                .thenReturn(Optional.of(message1));

        dispatchManager.dispatchLoop(queueName);

        verify(channel, times(1)).send(any(MessageDelivery.class));
        verify(queueManager, times(1)).poll(queueName);

        assertEquals(1, subscription.getInFlightTracker().size());
    }

    private QueuedMessage message(long id, String queueName) {
        return new QueuedMessage(
                id,
                ("payload-" + id).getBytes(StandardCharsets.UTF_8),
                Map.of(),
                queueName,
                System.currentTimeMillis(),
                DeliveryGuarantee.AT_LEAST_ONCE,
                false,
                0
        );
    }
}
