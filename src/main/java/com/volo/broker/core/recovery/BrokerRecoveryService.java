package com.volo.broker.core.recovery;

import java.util.List;

import org.springframework.stereotype.Component;

import com.volo.broker.domain.queue.QueueManager;
import com.volo.broker.core.dispatch.QueueDispatchScheduler;
import com.volo.broker.domain.message.QueuedMessage;
import com.volo.broker.persistence.MessageStore;

import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class BrokerRecoveryService {

    private final MessageStore messageStore;
    private final QueueManager queueManager;
    private final QueueDispatchScheduler dispatchScheduler;

    
    public void recover() {
        List<QueuedMessage> messages = messageStore.loadAllPending();

        for (QueuedMessage message : messages) {
            String queueName = message.getRoutingKey() == null || message.getRoutingKey().isBlank()
                    ? "default"
                    : message.getRoutingKey();

            if (queueManager.getQueue(queueName).isEmpty()) {
                queueManager.createQueue(queueName);
            }

            queueManager.enqueue(queueName, message);
            dispatchScheduler.signal(queueName);
        }
    }
}
