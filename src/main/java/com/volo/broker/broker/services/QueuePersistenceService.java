package com.volo.broker.broker.services;

import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.persistence.QueueStore;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class QueuePersistenceService {
    private final QueueStore queueStore;
    private final MessageStore messageStore;

    public QueuePersistenceService(
            QueueStore queueStore,
            MessageStore messageStore
    ) {
        this.queueStore = Objects.requireNonNull(queueStore);
        this.messageStore = Objects.requireNonNull(messageStore);
    }

    public void saveQueueIfDurable(BrokerQueue queue) {
        if (queue.settings().lifecyclePolicy().durable()) {
            queueStore.save(queue);
        }
    }

    public void deleteQueue(String queueName) {
        messageStore.deleteByQueuePath(queueName);
        queueStore.delete(queueName);
    }
}