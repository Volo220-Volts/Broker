package com.volo.broker.broker.subscription;

import com.volo.broker.TestConsumerChannel;
import com.volo.broker.broker.queue.settings.ConsumerSelectionStrategy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultSubscriptionManagerTest {

    @Test
    void register_shouldStoreSubscriptionByConsumerTag() {
        DefaultSubscriptionManager manager = new DefaultSubscriptionManager();
        TestConsumerChannel channel = new TestConsumerChannel();

        manager.register("orders.created", channel, 10);

        assertThat(manager.findByConsumerTag(channel.consumerTag()))
                .isPresent()
                .get()
                .extracting(Subscription::queueName)
                .isEqualTo("orders.created");
    }

    @Test
    void unregister_shouldRemoveSubscription() {
        DefaultSubscriptionManager manager = new DefaultSubscriptionManager();
        TestConsumerChannel channel = new TestConsumerChannel();

        manager.register("orders.created", channel, 10);
        manager.unregister(channel.consumerTag());

        assertThat(manager.findByConsumerTag(channel.consumerTag())).isEmpty();
        assertThat(manager.hasAvailableSubscriber("orders.created")).isFalse();
    }

    @Test
    void nextReady_shouldUseRoundRobinStrategy() {
        DefaultSubscriptionManager manager = new DefaultSubscriptionManager();

        TestConsumerChannel first = new TestConsumerChannel();
        TestConsumerChannel second = new TestConsumerChannel();

        manager.register("orders.created", first, 10);
        manager.register("orders.created", second, 10);

        Subscription selected1 = manager.nextReady(
                "orders.created",
                ConsumerSelectionStrategy.ROUND_ROBIN
        ).orElseThrow();

        Subscription selected2 = manager.nextReady(
                "orders.created",
                ConsumerSelectionStrategy.ROUND_ROBIN
        ).orElseThrow();

        assertThat(selected1.consumerTag()).isEqualTo(first.consumerTag());
        assertThat(selected2.consumerTag()).isEqualTo(second.consumerTag());
    }

    @Test
    void nextReady_shouldSkipSubscriberWhenPrefetchReached() {
        DefaultSubscriptionManager manager = new DefaultSubscriptionManager();

        TestConsumerChannel first = new TestConsumerChannel();
        TestConsumerChannel second = new TestConsumerChannel();

        manager.register("orders.created", first, 1);
        manager.register("orders.created", second, 1);

        Subscription firstSub = manager.findByConsumerTag(first.consumerTag()).orElseThrow();
        firstSub.track(1L, com.volo.broker.TestMessages.message(1));

        Subscription selected = manager.nextReady(
                "orders.created",
                ConsumerSelectionStrategy.FIRST_AVAILABLE
        ).orElseThrow();

        assertThat(selected.consumerTag()).isEqualTo(second.consumerTag());
    }

    @Test
    void leastInFlight_shouldReturnSubscriberWithSmallestInflightCount() {
        DefaultSubscriptionManager manager = new DefaultSubscriptionManager();

        TestConsumerChannel first = new TestConsumerChannel();
        TestConsumerChannel second = new TestConsumerChannel();

        manager.register("orders.created", first, 10);
        manager.register("orders.created", second, 10);

        Subscription firstSub = manager.findByConsumerTag(first.consumerTag()).orElseThrow();
        firstSub.track(1L, com.volo.broker.TestMessages.message(1));
        firstSub.track(2L, com.volo.broker.TestMessages.message(2));

        Subscription selected = manager.nextReady(
                "orders.created",
                ConsumerSelectionStrategy.LEAST_INFLIGHT
        ).orElseThrow();

        assertThat(selected.consumerTag()).isEqualTo(second.consumerTag());
    }

    @Test
    void hasAvailableSubscriber_shouldReturnFalseWhenAllReachedPrefetch() {
        DefaultSubscriptionManager manager = new DefaultSubscriptionManager();
        TestConsumerChannel channel = new TestConsumerChannel();

        manager.register("orders.created", channel, 1);

        Subscription subscription = manager.findByConsumerTag(channel.consumerTag()).orElseThrow();
        subscription.track(1L, com.volo.broker.TestMessages.message(1));

        assertThat(manager.hasAvailableSubscriber("orders.created")).isFalse();
    }
}
