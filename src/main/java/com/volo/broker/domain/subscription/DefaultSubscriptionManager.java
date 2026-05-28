package com.volo.broker.domain.subscription;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

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
        
        return Optional.of(s);
    }

    @Override
    public Optional<Subscription> nextReady(String queueName) {
        CopyOnWriteArrayList<Subscription> list = subsByQueue.get(queueName);
        if (list == null || list.isEmpty()) return Optional.empty();
        AtomicInteger idx = rrIndex.computeIfAbsent(queueName, k -> new AtomicInteger(0));
        int start = idx.getAndIncrement();
        int size = list.size();
        for (int i = 0; i < size; i++) {
            int pos = Math.floorMod(start + i, size);
            Subscription s = list.get(pos);
            if (s.canAcceptMore()) return Optional.of(s);
        }
        return Optional.empty();
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
}
