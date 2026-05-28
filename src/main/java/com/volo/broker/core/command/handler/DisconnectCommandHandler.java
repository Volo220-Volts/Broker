package com.volo.broker.core.command.handler;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.volo.broker.core.command.CommandHandler;
import com.volo.broker.core.command.model.DisconnectCommand;
import com.volo.broker.core.dispatch.QueueDispatchScheduler;
import com.volo.broker.domain.message.DeliveryGuarantee;
import com.volo.broker.domain.queue.QueueManager;
import com.volo.broker.domain.message.QueuedMessage;
import com.volo.broker.domain.subscription.SubscriptionManager;

import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class DisconnectCommandHandler implements CommandHandler<DisconnectCommand, Void> {

    private final SubscriptionManager subscriptionManager;
    private final QueueManager queueManager;
    private final QueueDispatchScheduler dispatchScheduler;
    @Override
    public Class<DisconnectCommand> getCommandType() {
        return DisconnectCommand.class;
    }

   @Override
    public Void handle(DisconnectCommand cmd) {
        subscriptionManager.findByConsumerTag(cmd.consumerTag)
                .ifPresent(sub -> {
                    subscriptionManager.unregister(cmd.consumerTag);

                    List<QueuedMessage> drained =
                            new ArrayList<>(sub.getInFlightTracker().drainAll());

                    for (int i = drained.size() - 1; i >= 0; i--) {
                        QueuedMessage message = drained.get(i);

                        if (message.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
                            queueManager.requeue(sub.queueName(), message.asRedelivered());
                        }
                    }

                    dispatchScheduler.signal(sub.queueName());
                });

        return null;
    }
}
