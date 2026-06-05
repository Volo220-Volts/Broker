package com.volo.broker.broker.command.handler;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.SubscribeCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.subscription.SubscriptionManager;

import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class SubscribeCommandHandler implements CommandHandler<SubscribeCommand, Void> {

    private final QueueManager queueManager;
    private final QueueDispatchScheduler dispatchScheduler;
    private final SubscriptionManager subscriptionManager;

    @Override
    public Class<SubscribeCommand> getCommandType() {
        return SubscribeCommand.class;
    }

    @Override
    public Void handle(SubscribeCommand command) {
        validate(command);

        var maybeQueue = queueManager.find(command.queueName());

        if(maybeQueue.isEmpty()){
            //queueManager.createQueue(command.queueName());
            throw new IllegalArgumentException("Queue does not exist: " + command.queueName());
        }

        var queue = maybeQueue.get();

        // if (!definitionRegistry.exists(command.queueName())) {
        //     definitionRegistry.declare(QueueDefinition.create(command.queueName()));
        // }


        int effectivePrefetch = resolvePrefetch(
                command.prefetchCount(),
                queue.settings().dispatchPolicy().prefetchCount()
        );

        subscriptionManager.register(
            command.queueName(),
            command.channel(),
            effectivePrefetch
        );

        dispatchScheduler.signal(command.queueName());

        return null;
    }

      private int resolvePrefetch(int requestedPrefetch, int queuePrefetch) {
        if (requestedPrefetch <= 0) {
            return queuePrefetch;
        }

        return Math.min(requestedPrefetch, queuePrefetch);
    }

    private void validate(SubscribeCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("subscribe command must not be null");
        }

        if (command.queueName() == null || command.queueName().isBlank()) {
            throw new IllegalArgumentException("queueName is required");
        }
    }

}
