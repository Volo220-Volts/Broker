package com.volo.broker.broker.services;

import com.volo.broker.broker.command.model.AckCommand;
import com.volo.broker.broker.command.model.NackCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.subscription.SubscriptionManager;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class ConsumerAcknowledgementService {
    private final SubscriptionManager subscriptionManager;
    private final QueueManager queueManager;
    private final QueueDispatchScheduler dispatchScheduler;
    private final MessageLifecycleService lifecycleService;

    public ConsumerAcknowledgementService(
            SubscriptionManager subscriptionManager,
            QueueManager queueManager,
            QueueDispatchScheduler dispatchScheduler,
            MessageLifecycleService lifecycleService
    ) {
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager);
        this.queueManager = Objects.requireNonNull(queueManager);
        this.dispatchScheduler = Objects.requireNonNull(dispatchScheduler);
        this.lifecycleService = Objects.requireNonNull(lifecycleService);
    }

    public void ack(AckCommand command) {
        subscriptionManager.findByConsumerTag(command.consumerTag())
                .ifPresent(subscription -> {
                    QueuedMessage message = subscription.remove(command.deliveryTag());
                    if (message == null) {
                        return;
                    }

                    BrokerQueue queue = queueManager.require(subscription.queueName());
                    lifecycleService.complete(queue, message);
                    queue.touch();

                    dispatchScheduler.signal(queue.queuePath());
                });
    }

    public void nack(NackCommand command) {
        subscriptionManager.findByConsumerTag(command.consumerTag())
                .ifPresent(subscription -> {
                    QueuedMessage message = subscription.remove(command.deliveryTag());
                    if (message == null) {
                        return;
                    }

                    BrokerQueue queue = queueManager.require(subscription.queueName());

                    if (command.requeue() && lifecycleService.canRedeliver(queue, message)) {
                        queue.requeue(message.asRedelivered());
                    } else {
                        lifecycleService.complete(queue, message);
                    }

                    queue.touch();
                    dispatchScheduler.signal(queue.queuePath());
                });
    }
}
