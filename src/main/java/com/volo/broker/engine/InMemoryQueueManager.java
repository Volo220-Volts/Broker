package com.volo.broker.engine;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;


public class InMemoryQueueManager implements QueueManager {
    private final ConcurrentMap<String, ConcurrentLinkedQueue<QueuedMessage>> queues = new ConcurrentHashMap<>();

    @Override
    public void createQueue(String name) {
        queues.computeIfAbsent(name, k -> new ConcurrentLinkedQueue<>());
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
        queues.computeIfAbsent(queueName, k -> new ConcurrentLinkedQueue<>()).offer(message);
    }

    @Override
    public Optional<QueuedMessage> poll(String queueName) {
        ConcurrentLinkedQueue<QueuedMessage> q = queues.get(queueName);
        if (q == null) return Optional.empty();
        return Optional.ofNullable(q.poll());
    }

    @Override
    public void requeue(String queueName, QueuedMessage message) {
        // best-effort requeue: put at tail (no direct head-insert for linked queue)
        enqueue(queueName, message);
    }

    @Override
    public int size(String queueName) {
        ConcurrentLinkedQueue<QueuedMessage> q = queues.get(queueName);
        return q == null ? 0 : q.size();
    }
}
