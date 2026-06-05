package com.volo.broker.broker.subscription;

import java.util.Optional;

import com.volo.broker.broker.queue.settings.ConsumerSelectionStrategy;

public interface SubscriptionManager {
    Subscription register(String queueName, ConsumerChannel channel, int prefetch);
    Optional<Subscription> unregister(String consumerTag);
    Optional<Subscription> nextReady(String queueName, ConsumerSelectionStrategy strategy);
    Optional<Subscription> findByConsumerTag(String consumerTag);
    boolean hasAvailableSubscriber(String queueName);
    boolean hasSubscribers(String queueName);
}
