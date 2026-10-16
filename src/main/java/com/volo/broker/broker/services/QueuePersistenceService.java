package com.volo.broker.broker.services;

import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.persistence.QueueMetadataStore;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class QueuePersistenceService {
    private final QueueMetadataStore queueMetadataStore;
    private final MessageStore messageStore;

    public QueuePersistenceService(
            QueueMetadataStore queueMetadataStore,
            MessageStore messageStore
    ) {
        this.queueMetadataStore = Objects.requireNonNull(queueMetadataStore);
        this.messageStore = Objects.requireNonNull(messageStore);
    }

    public void saveQueueIfDurable(BrokerQueue queue) {
        if (queue.settings().lifecyclePolicy().durable()) {
            queueMetadataStore.save(queue);
        }
    }

    public void deleteQueue(String queueName) {
        messageStore.deleteByQueuePath(queueName);
        queueMetadataStore.delete(queueName);
    }
}