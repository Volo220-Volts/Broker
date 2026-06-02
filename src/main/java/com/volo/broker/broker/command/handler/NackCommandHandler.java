package com.volo.broker.broker.command.handler;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.NackCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.subscription.SubscriptionManager;
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
        subscriptionManager.findByConsumerTag(cmd.consumerTag())
                .ifPresent(sub -> {
                    QueuedMessage removed = sub.remove(cmd.deliveryTag());

                    if (removed == null) {
                        return;
                    }

                    if (removed.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
                        if (cmd.requeue()) {
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
