package com.volo.broker.broker.subscription;

import java.util.Optional;

public interface SubscriptionManager {
    Subscription register(String queueName, ConsumerChannel channel, int prefetch);
    Optional<Subscription> unregister(String consumerTag);
    Optional<Subscription> nextReady(String queueName);
    Optional<Subscription> findByConsumerTag(String consumerTag);
    boolean hasAvailableSubscriber(String queueName);
}
