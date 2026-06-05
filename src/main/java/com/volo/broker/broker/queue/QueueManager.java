package com.volo.broker.broker.queue;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.broker.router.QueuePath;

public interface QueueManager {
    // void createQueue(String name);
    // Optional<String> getQueue(String name);
    // void deleteQueue(String name);
    // void enqueue(String queueName, QueuedMessage message);
    // Optional<QueuedMessage> poll(String queueName);
    // Optional<QueuedMessage> pollOldest(String queueName);
    // void requeue(String queueName, QueuedMessage message);
    // int size(String queueName);
    // long sizeBytes(String queueName);

    BrokerQueue declare(QueuePath path, QueueSettings settings);

    void delete(QueuePath path);

    BrokerQueue require(String queueName);

    Optional<BrokerQueue> find(String queueName);

    List<BrokerQueue> resolve(String routingKey);

    Collection<BrokerQueue> all();
}
