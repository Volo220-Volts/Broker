package com.volo.broker.broker.subscription;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.queue.settings.ConsumerSelectionStrategy;

@Component
public class DefaultSubscriptionManager implements SubscriptionManager {
    private final Map<String, CopyOnWriteArrayList<Subscription>> subsByQueue = new ConcurrentHashMap<>();
    private final Map<String, Subscription> byConsumer = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> rrIndex = new ConcurrentHashMap<>();

    @Override
    public Subscription register(String queueName, ConsumerChannel channel, int prefetch) {
        Subscription s = new Subscription(channel.consumerTag(), queueName, prefetch, channel);

        subsByQueue.computeIfAbsent(queueName, k -> new CopyOnWriteArrayList<>())
            .add(s);

        byConsumer.put(s.consumerTag(), s);
        rrIndex.computeIfAbsent(queueName, k -> new AtomicInteger(0));
        return s;
    }

    @Override
    public Optional<Subscription> unregister(String consumerTag) {
        Subscription s = byConsumer.remove(consumerTag);
        if (s == null) return Optional.empty();

        List<Subscription> list = subsByQueue.get(s.queueName());
        if (list != null) list.removeIf(x -> x.consumerTag().equals(consumerTag));
        
        rrIndex.remove(consumerTag);

        return Optional.of(s);
    }

    @Override
    public Optional<Subscription> nextReady(String queueName, ConsumerSelectionStrategy strategy) {
        List<Subscription> subscriptions = subsByQueue.get(queueName);
        if (subscriptions == null || subscriptions.isEmpty()) {
            return Optional.empty();
        }

        ConsumerSelectionStrategy effectiveStrategy = strategy == null
                ? ConsumerSelectionStrategy.ROUND_ROBIN
                : strategy;

        return switch (effectiveStrategy) {
            case ROUND_ROBIN -> nextRoundRobin(queueName, subscriptions);
            case FIRST_AVAILABLE -> firstAvailable(subscriptions);
            case LEAST_INFLIGHT -> leastInFlight(subscriptions);
        };
    }

    @Override
    public Optional<Subscription> findByConsumerTag(String consumerTag) {
        return Optional.ofNullable(byConsumer.get(consumerTag));
    }

    @Override
    public boolean hasAvailableSubscriber(String queueName) {
        List<Subscription> list = subsByQueue.get(queueName);
        if(list == null || list.isEmpty()) return false;

        for(var s : list){
            if(s.canAcceptMore()) return true;
        }
        return false;
    }

    private Optional<Subscription> nextRoundRobin(String queueName, List<Subscription> subscriptions) {
        synchronized (subscriptions) {
            int size = subscriptions.size();
            if (size == 0) {
                return Optional.empty();
            }

            AtomicInteger index = rrIndex.computeIfAbsent(queueName, ignored -> new AtomicInteger(0));
            int start = Math.floorMod(index.get(), size);

            for (int offset = 0; offset < size; offset++) {
                int currentIndex = (start + offset) % size;
                Subscription candidate = subscriptions.get(currentIndex);

                if (candidate.canAcceptMore()) {
                    index.set((currentIndex + 1) % size);
                    return Optional.of(candidate);
                }
            }

            return Optional.empty();
        }
    }

    private Optional<Subscription> firstAvailable(List<Subscription> subscriptions) {
        synchronized (subscriptions) {
            return subscriptions.stream()
                    .filter(Subscription::canAcceptMore)
                    .findFirst();
        }
    }

    private Optional<Subscription> leastInFlight(List<Subscription> subscriptions) {
        synchronized (subscriptions) {
            return subscriptions.stream()
                    .filter(Subscription::canAcceptMore)
                    .min(Comparator.comparingInt(s -> s.getInFlightTracker().size()));
        }
    }

    @Override
    public boolean hasSubscribers(String queueName) {
        List<Subscription> subscriptions = subsByQueue.get(queueName);

        if (subscriptions == null) {
            return false;
        }

        synchronized (subscriptions) {
            return !subscriptions.isEmpty();
        }
    }
}
