package com.volo.broker.core.dispatch;

import java.util.Optional;

import org.springframework.stereotype.Component;

import com.volo.broker.domain.message.DeliveryGuarantee;
import com.volo.broker.domain.message.MessageDelivery;
import com.volo.broker.domain.message.QueuedMessage;
import com.volo.broker.domain.queue.QueueManager;
import com.volo.broker.domain.subscription.Subscription;
import com.volo.broker.domain.subscription.SubscriptionManager;
import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class DispatchManager {
    private final QueueManager queueManager;
    private final SubscriptionManager subscriptionManager;
    
    public void dispatchLoop(String queueName) {
        while (true) {
            Optional<Subscription> maybeSub = subscriptionManager.nextReady(queueName);
            if (maybeSub.isEmpty()) {
                return;
            }

            Subscription sub = maybeSub.get();
            if(!sub.canAcceptMore()) break;

            Optional<QueuedMessage> maybeMessage = queueManager.poll(queueName);
            if (maybeMessage.isEmpty()) {
                return;
            }

            QueuedMessage message = maybeMessage.get();

            if(message.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE){
                dispatchAtLeastOnce(queueName, sub, message);
            } else {
                dispatchAtMostOnce(sub, message);
            }
        }
    }
    
    private void dispatchAtMostOnce(Subscription sub, QueuedMessage message){
        long deliveryTag = sub.nextDeliveryTag();
        try{
            sub.channel().send(MessageDelivery.of(deliveryTag, message));
        } catch (Throwable e) {}
    }
    
    private void dispatchAtLeastOnce(
        String queueName,
        Subscription sub,
        QueuedMessage message) 
    {
        long deliveryTag = sub.nextDeliveryTag();

        sub.track(deliveryTag, message);

        try {
            sub.channel().send(MessageDelivery.of(deliveryTag, message));

        } catch (Throwable ex) {
            sub.remove(deliveryTag);

            queueManager.requeue(queueName, message.asRedelivered());
            return;
        }
    }

}
