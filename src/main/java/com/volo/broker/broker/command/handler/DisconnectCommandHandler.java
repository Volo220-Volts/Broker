package com.volo.broker.broker.command.handler;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.DisconnectCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.broker.subscription.SubscriptionManager;
import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class DisconnectCommandHandler implements CommandHandler<DisconnectCommand, Void> {

    private final SubscriptionManager subscriptionManager;
    private final QueueManager queueManager;
    private final QueueDispatchScheduler dispatchScheduler;
    private final MessageLifecycleService messageLifecycleService;
    @Override
    public Class<DisconnectCommand> getCommandType() {
        return DisconnectCommand.class;
    }

   @Override
    public Void handle(DisconnectCommand cmd) {
        subscriptionManager.findByConsumerTag(cmd.consumerTag())
                .ifPresent(sub -> {
                    subscriptionManager.unregister(cmd.consumerTag());

                    List<QueuedMessage> drained =
                            new ArrayList<>(sub.getInFlightTracker().drainAll());

                    dispatchScheduler.signal(sub.queueName());

                    for (int i = drained.size() - 1; i >= 0; i--) {
                        QueuedMessage message = drained.get(i);

                        var queue = queueManager.find(sub.queueName()).get();

                        boolean shouldRequeue = queue.settings().redeliveryPolicy().enabled()
                                && queue.settings().redeliveryPolicy().requeueOnDisconnect()
                                && message.getDeliveryAttempt() < queue.settings().redeliveryPolicy().maxAttempts();

                        if (shouldRequeue) {
                            queueManager.find(sub.queueName()).ifPresent(q -> q.requeue(message.asRedelivered()));
                        } else {
                            messageLifecycleService.complete(queue, message);
                        }
                    }
                });

        return null;
    }
}
