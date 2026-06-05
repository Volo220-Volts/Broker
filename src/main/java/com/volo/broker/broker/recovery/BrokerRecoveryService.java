package com.volo.broker.broker.recovery;

import java.util.List;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.persistence.MessageStore;

import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class BrokerRecoveryService {

    private final MessageStore messageStore;
    private final QueueManager queueManager;
    private final QueueDispatchScheduler dispatchScheduler;

    
    public void recover() {

        // Todo: Implement recovery logic to load pending messages from the message store.

        // List<QueuedMessage> messages = messageStore.loadAllPending();

        // for (QueuedMessage message : messages) {
        //     String queueName = message.getRoutingKey() == null || message.getRoutingKey().isBlank()
        //             ? "default"
        //             : message.getRoutingKey();

        //     if (queueManager.getQueue(queueName).isEmpty()) {
        //         queueManager.createQueue(queueName);
        //     }

        //     queueManager.enqueue(queueName, message);
        //     dispatchScheduler.signal(queueName);
        // }
    }
}
