package com.volo.broker.broker.services;

import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.error.BrokerException;
import com.volo.broker.broker.message.MessageIdGenerator;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.EnqueueResult;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.exception.QueueOverflowException;
import com.volo.broker.broker.queue.settings.PersistenceMode;
import com.volo.broker.persistence.MessageStore;
import com.volo.broker.broker.flow.FlowTransition;
import com.volo.broker.broker.flow.GlobalMemoryGuard;
import com.volo.broker.broker.flow.QueueFlowController;
import org.springframework.beans.factory.annotation.Autowired;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class PublishService {
    private final QueueManager queueManager;
    private final MessageStore messageStore;
    private final QueueDispatchScheduler dispatchScheduler;
    private final MessageIdGenerator messageIdGenerator;
    private final MessageLifecycleService lifecycleService;
    private final QueueFlowController flowController;
    private final GlobalMemoryGuard globalMemoryGuard;

    public PublishService(
            QueueManager queueManager,
            MessageStore messageStore,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator
    ) {
        this(queueManager, messageStore, dispatchScheduler, messageIdGenerator, new MessageLifecycleService(messageStore), null, null);
    }

    public PublishService(
            QueueManager queueManager,
            MessageStore messageStore,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator,
            MessageLifecycleService lifecycleService
    ) {
        this(queueManager, messageStore, dispatchScheduler, messageIdGenerator, lifecycleService, null, null);
    }

    @Autowired
    public PublishService(
            QueueManager queueManager,
            MessageStore messageStore,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator,
            MessageLifecycleService lifecycleService,
            @Nullable QueueFlowController flowController,
            @Nullable GlobalMemoryGuard globalMemoryGuard
    ) {
        this.queueManager = Objects.requireNonNull(queueManager);
        this.messageStore = Objects.requireNonNull(messageStore);
        this.dispatchScheduler = Objects.requireNonNull(dispatchScheduler);
        this.messageIdGenerator = Objects.requireNonNull(messageIdGenerator);
        this.lifecycleService = Objects.requireNonNull(lifecycleService);
        this.flowController = flowController;
        this.globalMemoryGuard = globalMemoryGuard;
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
                if (!queue.canAccept(message)) {
                    throw new QueueOverflowException("Queue capacity exceeded: " + queue.queuePath());
                }
            }

            List<String> routedQueuePaths = new ArrayList<>(queues.size());
            for (int i = 0; i < queues.size(); i++) {
                BrokerQueue queue = queues.get(i);
                QueuedMessage message = messages.get(i);

                if (queue.settings().persistenceMode() == PersistenceMode.PERSISTENT) {
                    messageStore.save(queue.queuePath(), message);
                }

                EnqueueResult enqueueResult = queue.enqueue(message);
                for (QueuedMessage dropped : enqueueResult.droppedMessages()) {
                    lifecycleService.complete(queue, dropped);
                    if (globalMemoryGuard != null) {
                        FlowTransition gt = globalMemoryGuard.recordBytesRemoved(dropped.sizeBytes());
                        if (flowController != null) {
                            flowController.handleGlobalTransition(gt);
                        }
                    }
                }

                if (globalMemoryGuard != null) {
                    FlowTransition gt = globalMemoryGuard.recordBytesAdded(message.sizeBytes());
                    if (flowController != null) {
                        flowController.handleGlobalTransition(gt);
                    }
                }

                if (flowController != null) {
                    long maxBlockMs = queue.settings().flowControlPolicy() != null
                            ? queue.settings().flowControlPolicy().maxBlockMs()
                            : 0;
                    flowController.handleTransition(queue.queuePath(), enqueueResult.transition(), maxBlockMs);
                }

                routedQueuePaths.add(queue.queuePath());
                dispatchScheduler.signal(queue.queuePath());
            }

            return PublishResult.ack(command.clientPublishId(), routedQueuePaths);
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
