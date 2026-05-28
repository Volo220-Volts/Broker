package com.volo.broker.core.command.handler;

import com.volo.broker.core.command.CommandHandler;
import com.volo.broker.core.command.model.PublishCommand;
import com.volo.broker.core.command.model.PublishResult;
import com.volo.broker.core.dispatch.QueueDispatchScheduler;
import com.volo.broker.core.error.BrokerErrorCode;
import com.volo.broker.domain.message.DeliveryGuarantee;
import com.volo.broker.domain.message.MessageIdGenerator;
import com.volo.broker.domain.queue.QueueManager;
import com.volo.broker.domain.message.QueuedMessage;
import com.volo.broker.persistence.MessageStore;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Component
public class PublishCommandHandler implements CommandHandler<PublishCommand, PublishResult> {
    private final QueueManager queueManager;
    private final MessageStore messageStore;
    private final QueueDispatchScheduler dispatchScheduler;
    private final MessageIdGenerator messageIdGenerator;

    public PublishCommandHandler(
            QueueManager queueManager,
            MessageStore messageStore,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator
    ) {
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.messageStore = Objects.requireNonNull(messageStore, "messageStore must not be null");
        this.dispatchScheduler = Objects.requireNonNull(dispatchScheduler, "dispatchScheduler must not be null");
        this.messageIdGenerator = Objects.requireNonNull(messageIdGenerator, "messageIdGenerator must not be null");
    }

    @Override
    public Class<PublishCommand> getCommandType() {
        return PublishCommand.class;
    }

    @Override
    @Transactional
    public PublishResult handle(PublishCommand command) {
        try {
            String queueName = resolveQueueName(command);
            ensureQueueExists(queueName);

            QueuedMessage message = new QueuedMessage(
                    messageIdGenerator.nextId(),
                    command.payload(),
                    command.headers(),
                    command.routingKey(),
                    System.currentTimeMillis(),
                    command.deliveryGuarantee(),
                    false,
                    0
            );

            if (command.deliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
                messageStore.save(queueName, message);
            }

            queueManager.enqueue(queueName, message);
            dispatchScheduler.signal(queueName);

            return PublishResult.ack(message.getMessageId());
        } catch (IllegalArgumentException exception) {
            return PublishResult.nack(BrokerErrorCode.INVALID_REQUEST, exception.getMessage());
        } catch (Throwable throwable) {
            return PublishResult.nack(BrokerErrorCode.INTERNAL_ERROR, "Message was not accepted by broker");
        }
    }

    private void ensureQueueExists(String queueName) {
        if (queueManager.getQueue(queueName).isEmpty()) {
            queueManager.createQueue(queueName);
        }
    }

    private String resolveQueueName(PublishCommand command) {
        if (command.routingKey() == null || command.routingKey().isBlank()) {
            throw new IllegalArgumentException("routingKey is required");
        }
        return command.routingKey();
    }
}
