package com.volo.broker.broker.dispatch;

import java.util.Optional;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.message.MessageDelivery;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.settings.ConsumerAckMode;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.subscription.Subscription;
import com.volo.broker.broker.subscription.SubscriptionManager;
import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class DispatchManager {
    private final QueueManager queueManager;
    private final SubscriptionManager subscriptionManager;
    private final MessageLifecycleService lifecycleService;
    
     public void dispatchLoop(String queueName) {
        while (true) {
            var queue = queueManager.find(queueName).get();
            Optional<Subscription> maybeSubscription = subscriptionManager.nextReady(
                    queueName,
                    queue.settings().dispatchPolicy().consumerSelectionStrategy()
            );

            if (maybeSubscription.isEmpty()) {
                return;
            }

            Subscription subscription = maybeSubscription.get();

            if (!subscription.canAcceptMore()) {
                return;
            }


            Optional<QueuedMessage> maybeMessage = queue.poll();
            if (maybeMessage.isEmpty()) {
                return;
            }

            QueuedMessage message = maybeMessage.get();

            if (lifecycleService.isExpired(queue, message)) {
                lifecycleService.complete(queue, message);
                continue;
            }

            dispatch(queue, subscription, message);
        }
    }
    private void dispatch(
            BrokerQueue queue,
            Subscription subscription,
            QueuedMessage message
    ) {
        long deliveryTag = subscription.nextDeliveryTag();
        boolean consumerAckRequired = queue.settings().consumerAckMode() == ConsumerAckMode.ENABLED;

        if (consumerAckRequired) {
            subscription.track(deliveryTag, message);
        }

        try {
            subscription.channel().send(MessageDelivery.of(deliveryTag, message));

            queue.touch();

            if (!consumerAckRequired) {
                lifecycleService.complete(queue, message);
            }

        } catch (Throwable throwable) {
             if (consumerAckRequired) {
                subscription.remove(deliveryTag);
            }

            boolean shouldRequeue = queue.settings().redeliveryPolicy().requeueOnSendFailure()
                    && lifecycleService.canRedeliver(queue, message);

            if (shouldRequeue) {
                queue.requeue(message.asRedelivered());
                return;
            }

            lifecycleService.complete(queue, message);
        }
    }

}
