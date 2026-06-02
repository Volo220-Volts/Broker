package com.volo.broker.broker.queue;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

import com.volo.broker.broker.message.QueuedMessage;

public class InMemoryDequeManager implements QueueManager{
    private final ConcurrentMap<String, ConcurrentLinkedDeque<QueuedMessage>> queues = new ConcurrentHashMap<>();

    @Override
    public void createQueue(String name) {
        queues.computeIfAbsent(name, k -> new ConcurrentLinkedDeque<>());
    }

    @Override
    public Optional<String> getQueue(String name) {
        return queues.containsKey(name) ? Optional.of(name) : Optional.empty();
    }

    @Override
    public void deleteQueue(String name) {
        queues.remove(name);
    }

    @Override
    public void enqueue(String queueName, QueuedMessage message) {
        queues.computeIfAbsent(queueName, k -> new ConcurrentLinkedDeque<>()).offer(message);
    }

    @Override
    public Optional<QueuedMessage> poll(String queueName) {
        ConcurrentLinkedDeque<QueuedMessage> q = queues.get(queueName);
        if (q == null) return Optional.empty();
        return Optional.ofNullable(q.poll());
    }

    @Override
    public void requeue(String queueName, QueuedMessage message) {
        queues.computeIfAbsent(queueName, k -> new ConcurrentLinkedDeque<>()).offerFirst(message);
    }

    @Override
    public int size(String queueName) {
        ConcurrentLinkedDeque<QueuedMessage> q = queues.get(queueName);
        return q == null ? 0 : q.size();
    }
}
