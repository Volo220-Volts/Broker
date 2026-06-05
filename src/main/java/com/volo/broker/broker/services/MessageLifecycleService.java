package com.volo.broker.broker.services;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.settings.PersistenceMode;
import com.volo.broker.persistence.MessageStore;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class MessageLifecycleService {
    private final MessageStore messageStore;

    public MessageLifecycleService(MessageStore messageStore) {
        this.messageStore = Objects.requireNonNull(messageStore);
    }

    public boolean isExpired(BrokerQueue queue, QueuedMessage message) {
        return queue.settings()
                .retentionPolicy()
                .expired(message.getCreatedAt(), System.currentTimeMillis());
    }

    public boolean canRedeliver(BrokerQueue queue, QueuedMessage message) {
        if (!queue.settings().redeliveryPolicy().enabled()) {
            return false;
        }

        return message.getDeliveryAttempt() < queue.settings().redeliveryPolicy().maxAttempts();
    }

    public void complete(BrokerQueue queue, QueuedMessage message) {
        if (queue.settings().persistenceMode() == PersistenceMode.PERSISTENT) {
            messageStore.delete(message.getMessageId());
        }
    }
}