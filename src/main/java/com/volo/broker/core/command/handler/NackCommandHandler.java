package com.volo.broker.core.command.handler;

import org.springframework.stereotype.Component;

import com.volo.broker.core.command.CommandHandler;
import com.volo.broker.core.command.model.NackCommand;
import com.volo.broker.core.dispatch.QueueDispatchScheduler;
import com.volo.broker.domain.message.DeliveryGuarantee;
import com.volo.broker.domain.queue.QueueManager;
import com.volo.broker.domain.message.QueuedMessage;
import com.volo.broker.domain.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;

import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class NackCommandHandler implements CommandHandler<NackCommand, Void> {

    private final SubscriptionManager subscriptionManager;
    private final QueueManager queueManager;
    private final QueueDispatchScheduler dispatchScheduler;
    private final MessageStore messageStore;
     
    @Override
    public Class<NackCommand> getCommandType() {
        return NackCommand.class;
    }

    @Override
    public Void handle(NackCommand cmd) {
        subscriptionManager.findByConsumerTag(cmd.consumerTag)
                .ifPresent(sub -> {
                    QueuedMessage removed = sub.remove(cmd.deliveryTag);

                    if (removed == null) {
                        return;
                    }

                    if (removed.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
                        if (cmd.requeue) {
                            queueManager.requeue(sub.queueName(), removed.asRedelivered());
                        } else {
                            messageStore.delete(removed.getMessageId());
                        }
                    }

                    dispatchScheduler.signal(sub.queueName());
                });

        return null;
    }
}
