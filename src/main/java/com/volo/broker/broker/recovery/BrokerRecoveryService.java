package com.volo.broker.broker.recovery;

import com.volo.broker.broker.message.MessageIdGenerator;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.persistence.QueueStore;
import com.volo.broker.persistence.dto.StoredMessageDto;
import com.volo.broker.persistence.dto.StoredQueueDto;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class BrokerRecoveryService implements ApplicationRunner {
    private final QueueStore queueStore;
    private final MessageStore messageStore;
    private final QueueManager queueManager;
    private final MessageIdGenerator messageIdGenerator;

    public BrokerRecoveryService(
            QueueStore queueStore,
            MessageStore messageStore,
            QueueManager queueManager,
            MessageIdGenerator messageIdGenerator
    ) {
        this.queueStore = Objects.requireNonNull(queueStore);
        this.messageStore = Objects.requireNonNull(messageStore);
        this.queueManager = Objects.requireNonNull(queueManager);
        this.messageIdGenerator = Objects.requireNonNull(messageIdGenerator);
    }

    @Override
    public void run(ApplicationArguments args) {
        restoreQueues();
        restoreMessages();
        restoreMessageIdGenerator();
    }

    private void restoreQueues() {
        for (StoredQueueDto queueDto : queueStore.loadAll()) {
            QueuePath queuePath = QueuePath.of(queueDto.queuePath());

            if (queueManager.find(queuePath.value()).isEmpty()) {
                queueManager.declare(queuePath, queueDto.settings());
            }
        }
    }

    private void restoreMessages() {
        for (StoredMessageDto messageDto : messageStore.loadAllPending()) {
            queueManager.find(messageDto.queuePath())
                    .ifPresentOrElse(
                            queue -> queue.enqueue(messageDto.message().asRedelivered()),
                            () -> messageStore.delete(messageDto.message().getMessageId())
                    );
        }
    }

    private void restoreMessageIdGenerator() {
        messageIdGenerator.initialize(messageStore.findMaxMessageId());
    }
}