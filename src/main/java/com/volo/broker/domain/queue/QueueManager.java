package com.volo.broker.domain.queue;

import java.util.Optional;

import com.volo.broker.domain.message.QueuedMessage;

public interface QueueManager {
    void createQueue(String name);
    Optional<String> getQueue(String name);
    void deleteQueue(String name);
    void enqueue(String queueName, QueuedMessage message);
    Optional<QueuedMessage> poll(String queueName);
    void requeue(String queueName, QueuedMessage message);
    int size(String queueName);
}
