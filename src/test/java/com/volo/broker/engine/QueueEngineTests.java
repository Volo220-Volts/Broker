package com.volo.broker.engine;

import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.volo.broker.broker.inflight.DefaultInFlightTracker;
import com.volo.broker.broker.inflight.InFlightTracker;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.subscription.ConsumerChannel;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.subscription.Subscription;
import com.volo.broker.broker.subscription.SubscriptionManager;

class QueueEngineTests {

    @Test
    void inMemoryQueueManager_shouldPreserveFifoAndAllowRequeue() {
        QueueManager manager = new InMemoryQueueManager();
        String queueName = "test-queue";
        manager.createQueue(queueName);

        QueuedMessage first = new QueuedMessage(1L, "first".getBytes(), Map.of("k", "v"), "route", System.currentTimeMillis(), DeliveryGuarantee.AT_LEAST_ONCE, false, 0);
        QueuedMessage second = new QueuedMessage(2L, "second".getBytes(), Map.of(), "route", System.currentTimeMillis(), DeliveryGuarantee.AT_LEAST_ONCE, false, 0);

        manager.enqueue(queueName, first);
        manager.enqueue(queueName, second);

        assertEquals(2, manager.size(queueName), "Queue size should be 2 after two enqueues");

        Optional<QueuedMessage> poll1 = manager.poll(queueName);
        assertTrue(poll1.isPresent());
        assertEquals(1L, poll1.get().getMessageId());

        manager.requeue(queueName, poll1.get());
        assertEquals(2, manager.size(queueName), "Requeue should preserve queue count when a message is returned");

        Optional<QueuedMessage> poll2 = manager.poll(queueName);
        assertTrue(poll2.isPresent());
        assertEquals(2L, poll2.get().getMessageId());

        Optional<QueuedMessage> poll3 = manager.poll(queueName);
        assertTrue(poll3.isPresent());
        assertEquals(1L, poll3.get().getMessageId());
    }

    @Test
    void defaultInFlightTracker_shouldTrackAndDrainCorrectly() {
        InFlightTracker tracker = new DefaultInFlightTracker();

        long firstTag = tracker.nextTag();
        long secondTag = tracker.nextTag();

        assertEquals(1L, firstTag);
        assertEquals(2L, secondTag);
        assertEquals(0, tracker.size());

        QueuedMessage messageA = new QueuedMessage(10L, new byte[0], null, "route", System.currentTimeMillis(), DeliveryGuarantee.AT_LEAST_ONCE, true, 1);
        tracker.track(firstTag, messageA);
        assertEquals(1, tracker.size());

        QueuedMessage removed = tracker.remove(firstTag);
        assertNotNull(removed);
        assertEquals(10L, removed.getMessageId());
        assertEquals(0, tracker.size());

        tracker.track(secondTag, messageA);
        assertEquals(1, tracker.drainAll().size());
        assertEquals(0, tracker.size(), "drainAll should clear all in-flight entries");
    }

    @Test
    void defaultSubscriptionManager_shouldRoundRobinAndRespectPrefetch() {
        SubscriptionManager subscriptionManager = new DefaultSubscriptionManager();
        String queueName = "round-robin-queue";

        ConsumerChannel channel1 = mock(ConsumerChannel.class);
        ConsumerChannel channel2 = mock(ConsumerChannel.class);

        when(channel1.consumerTag()).thenReturn("consumer-1");
        when(channel2.consumerTag()).thenReturn("consumer-2");
        when(channel1.canAcceptMore()).thenReturn(true);
        when(channel2.canAcceptMore()).thenReturn(true);

        subscriptionManager.register(queueName, channel1, 2);
        subscriptionManager.register(queueName, channel2, 2);

        Optional<Subscription> first = subscriptionManager.nextReady(queueName);
        Optional<Subscription> second = subscriptionManager.nextReady(queueName);
        Optional<Subscription> third = subscriptionManager.nextReady(queueName);

        assertTrue(first.isPresent());
        assertTrue(second.isPresent());
        assertTrue(third.isPresent());
        assertNotEquals(first.get().consumerTag(), second.get().consumerTag(), "Round robin should alternate consumers");
        assertEquals(first.get().consumerTag(), third.get().consumerTag(), "Round robin should cycle back to the first consumer");

        subscriptionManager.unregister("consumer-1");
        Optional<Subscription> nextAfterUnregister = subscriptionManager.nextReady(queueName);
        assertTrue(nextAfterUnregister.isPresent());
        assertEquals("consumer-2", nextAfterUnregister.get().consumerTag());
    }


















































































































































}
