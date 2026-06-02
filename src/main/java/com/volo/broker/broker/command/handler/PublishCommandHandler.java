package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.MessageIdGenerator;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.router.RoutePattern;
import com.volo.broker.broker.router.RouteTree;
import com.volo.broker.broker.validation.QueuePathValidator;
import com.volo.broker.persistence.MessageStore;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.Set;

@Component
public class PublishCommandHandler implements CommandHandler<PublishCommand, PublishResult> {
    private final QueueManager queueManager;
    private final MessageStore messageStore;
    private final QueueDispatchScheduler dispatchScheduler;
    private final MessageIdGenerator messageIdGenerator;
    private final RouteTree routeTree;
    private final QueuePathValidator queuePathValidator;

    public PublishCommandHandler(
            QueueManager queueManager,
            MessageStore messageStore,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator,
            RouteTree routeTree,
            QueuePathValidator queuePathValidator
    ) {
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.messageStore = Objects.requireNonNull(messageStore, "messageStore must not be null");
        this.dispatchScheduler = Objects.requireNonNull(dispatchScheduler, "dispatchScheduler must not be null");
        this.messageIdGenerator = Objects.requireNonNull(messageIdGenerator, "messageIdGenerator must not be null");
        this.routeTree = Objects.requireNonNull(routeTree, "routeTree must not be null");
        this.queuePathValidator = Objects.requireNonNull(queuePathValidator, "queuePathValidator must not be null");
    }

    @Override
    public Class<PublishCommand> getCommandType() {
        return PublishCommand.class;
    }

    @Override
    @Transactional
    public PublishResult handle(PublishCommand command) {
        try {
            Set<String> queueNames = resolveQueueNames(command);
            if (queueNames.isEmpty()) {
                return PublishResult.nack(BrokerErrorCode.INVALID_REQUEST, "routingKey did not match any queue");
            }

            long acknowledgedMessageId = -1;
            for (String queueName : queueNames) {
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

                if (acknowledgedMessageId < 0) {
                    acknowledgedMessageId = message.getMessageId();
                }
            }

            return PublishResult.ack(acknowledgedMessageId);
        } catch (IllegalArgumentException exception) {
            return PublishResult.nack(BrokerErrorCode.INVALID_REQUEST, exception.getMessage());
        } catch (Throwable throwable) {
            return PublishResult.nack(BrokerErrorCode.INTERNAL_ERROR, "Message was not accepted by broker");
        }
    }

    private Set<String> resolveQueueNames(PublishCommand command) {
        if (command.routingKey() == null || command.routingKey().isBlank()) {
            throw new IllegalArgumentException("routingKey is required");
        }

        Set<String> queueNames = routeTree.resolve(command.routingKey());

        if (queueNames.isEmpty() && !RoutePattern.containsWildcard(command.routingKey())) {
            QueuePath queuePath = queuePathValidator.validateAndParse(command.routingKey());
            if (queueManager.getQueue(queuePath.value()).isEmpty()) {
                queueManager.createQueue(queuePath.value());
            }
            routeTree.attachQueue(queuePath);
            queueNames = Set.of(queuePath.value());
        }

        return queueNames;
    }
}
