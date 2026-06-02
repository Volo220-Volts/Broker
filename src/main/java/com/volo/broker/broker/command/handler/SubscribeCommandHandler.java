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
        if(queueManager.getQueue(command.queueName()).isEmpty()){
            queueManager.createQueue(command.queueName());
        }

        subscriptionManager.register(
            command.queueName(),
            command.channel(),
            command.prefetchCount()
        );

        dispatchScheduler.signal(command.queueName());

        return null;
    }

}
