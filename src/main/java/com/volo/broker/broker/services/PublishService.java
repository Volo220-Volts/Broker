package com.volo.broker.broker.services;

import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.error.BrokerException;
import com.volo.broker.broker.message.MessageIdGenerator;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.exception.QueueOverflowException;
import com.volo.broker.broker.queue.settings.PersistenceMode;
import com.volo.broker.persistence.MessageStore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
public class PublishService {
    private final QueueManager queueManager;
    private final MessageStore messageStore;
    private final QueueDispatchScheduler dispatchScheduler;
    private final MessageIdGenerator messageIdGenerator;

    public PublishService(
            QueueManager queueManager,
            MessageStore messageStore,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator
    ) {
        this.queueManager = Objects.requireNonNull(queueManager);
        this.messageStore = Objects.requireNonNull(messageStore);
        this.dispatchScheduler = Objects.requireNonNull(dispatchScheduler);
        this.messageIdGenerator = Objects.requireNonNull(messageIdGenerator);
    }

    public PublishResult publish(PublishCommand command) {
        try {
            validate(command);

            List<BrokerQueue> queues = queueManager.resolve(command.routingKey());

            if (queues.isEmpty()) {
                return PublishResult.nack(
                        command.clientPublishId(),
                        BrokerErrorCode.QUEUE_NOT_FOUND, // todo: consider using a different error code for this scenario
                        "No queue matched routing key: " + command.routingKey()
                );
            }

            List<QueuedMessage> messages = queues.stream()
                    .map(queue -> createMessage(command))
                    .toList();

            for (int i = 0; i < queues.size(); i++) {
                BrokerQueue queue = queues.get(i);
                QueuedMessage message = messages.get(i);

                if (queue.settings().persistenceMode() == PersistenceMode.PERSISTENT) {
                    messageStore.save(queue.queuePath(), message);
                }

                queue.enqueue(message);
                dispatchScheduler.signal(queue.queuePath());
            }

            return PublishResult.ack(command.clientPublishId(), queues.size());
        } catch (QueueOverflowException exception) {
            return PublishResult.nack(
                    command.clientPublishId(),
                    BrokerErrorCode.QUEUE_OVERFLOW,
                    exception.getMessage()
            );
        } catch (IllegalArgumentException exception) {
            return PublishResult.nack(
                    command.clientPublishId(),
                    BrokerErrorCode.INVALID_REQUEST,
                    exception.getMessage()
            );
        } catch (BrokerException exception) {
            return PublishResult.nack(
                    command.clientPublishId(),
                    exception.code(),
                    exception.getMessage()
            );
        } catch (Throwable throwable) {
            return PublishResult.nack(
                    command.clientPublishId(),
                    BrokerErrorCode.INTERNAL_ERROR,
                    "Message was not accepted by broker"
            );
        }
    }

    private QueuedMessage createMessage(PublishCommand command) {
        return new QueuedMessage(
                messageIdGenerator.nextId(),
                command.payload(),
                command.headers(),
                command.routingKey(),
                System.currentTimeMillis(),
                false,
                0
        );
    }

    private void validate(PublishCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("publish command must not be null");
        }

        if (command.routingKey() == null || command.routingKey().isBlank()) {
            throw new IllegalArgumentException("routingKey is required");
        }

        if (command.payload().length == 0) {
            throw new IllegalArgumentException("payload must not be empty");
        }
    }
}
