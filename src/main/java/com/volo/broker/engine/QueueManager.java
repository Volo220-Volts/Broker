package com.volo.broker.engine;

import java.util.Optional;

public interface QueueManager {
    void createQueue(String name);
    Optional<String> getQueue(String name);
    void deleteQueue(String name);
    void enqueue(String queueName, QueuedMessage message);
    Optional<QueuedMessage> poll(String queueName);
    void requeue(String queueName, QueuedMessage message);
    int size(String queueName);
}
