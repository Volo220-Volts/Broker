package com.volo.broker.core.command.handler;

import org.springframework.stereotype.Component;

import com.volo.broker.core.command.CommandHandler;
import com.volo.broker.core.command.model.AckCommand;
import com.volo.broker.core.dispatch.QueueDispatchScheduler;
import com.volo.broker.domain.message.DeliveryGuarantee;
import com.volo.broker.domain.message.QueuedMessage;
import com.volo.broker.domain.subscription.SubscriptionManager;
import com.volo.broker.persistence.MessageStore;

import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor

public class AckCommandHandler implements CommandHandler<AckCommand, Void> {

    private final QueueDispatchScheduler dispatchSheduler;
    private final SubscriptionManager subscriptionManager;
    private final MessageStore messageStore;

    @Override
    public Class<AckCommand> getCommandType() {
        return AckCommand.class;
    }

    @Override
    public Void handle(AckCommand command) {
        subscriptionManager.findByConsumerTag(command.consumerTag)
            .ifPresent(sub -> {
                QueuedMessage removed = sub.remove(command.deliveryTag);

                if(removed == null) return;

                if(removed.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE){
                    messageStore.delete(removed.getMessageId());
                }
                
                dispatchSheduler.signal(sub.queueName());
            });
        return null;
    }
}
