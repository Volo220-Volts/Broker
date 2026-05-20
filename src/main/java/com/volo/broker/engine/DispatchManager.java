package com.volo.broker.engine;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DispatchManager {
    private final QueueManager queueManager;
    private final SubscriptionManager subscriptionManager;
    private final ExecutorService executor;

    public DispatchManager(QueueManager queueManager, SubscriptionManager subscriptionManager,
            @Value("${broker.dispatch.threads:2}") int threads) {
        this.queueManager = queueManager;
        this.subscriptionManager = subscriptionManager;
        this.executor = Executors.newFixedThreadPool(Math.max(1, threads), new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
            @Override
            public Thread newThread(Runnable r) {
                Thread t = defaultFactory.newThread(r);
                t.setDaemon(true);
                t.setName("queue-dispatch-" + t.threadId());
                return t;
            }
        });
    }
    
    public void tryDispatch(String queueName) {
    executor.execute(() -> {
        try {
            while (true) {
                Optional<Subscription> maybeSub = subscriptionManager.nextReady(queueName);
                if (maybeSub.isEmpty()) break;

                Subscription sub = maybeSub.get();
                if (!sub.canAcceptMore()) break;
                Optional<QueuedMessage> maybeMessage = queueManager.poll(queueName);
                if (maybeMessage.isEmpty()) break;

                QueuedMessage message = maybeMessage.get();
                long deliveryTag = sub.nextDeliveryTag();

                try {
                    sub.channel().send(MessageDelivery.of(deliveryTag, message));
                    sub.track(deliveryTag, message);
                } catch (Exception ex) {
                    queueManager.requeue(queueName, message);
                    sub.remove(deliveryTag);
                    // sub.getInFlightTracker().remove(deliveryTag);
                    break;
                }
            }
        } catch (Throwable t) {
            t.printStackTrace(System.err);
        }
    });
}

    // public void tryDispatch(String queueName) {
    //     executor.execute(() -> {
    //         try {
    //             while (true) {
    //                 Optional<Subscription> maybeSub = subscriptionManager.nextReady(queueName);
    //                 if (maybeSub.isEmpty()) break;
    //                 Subscription sub = maybeSub.get();
    //                 if (!sub.canAcceptMore()) break;
    //                 Optional<QueuedMessage> maybeMessage = queueManager.poll(queueName);
    //                 if (maybeMessage.isEmpty()) break;
    //                 QueuedMessage message = maybeMessage.get();
    //                 long deliveryTag = sub.nextDeliveryTag();
                    
    //                 // send asynchronously (channel may block on network)
    //                 executor.execute(() -> {
    //                     try {
    //                         sub.channel().send(MessageDelivery.of(deliveryTag, message));
    //                         sub.track(deliveryTag, message);
    //                     } catch (Exception ex) {
    //                         // on send failure, put back into queue
    //                         queueManager.requeue(queueName, message);
    //                         sub.remove(deliveryTag);
    //                         // sub.getInFlightTracker().remove(deliveryTag);
    //                     }
    //                 });
    //             }
    //         } catch (Throwable t) {
    //             t.printStackTrace(System.err);
    //         }
    //     });
    // }

    public void acknowledge(String consumerTag, long deliveryTag) {
        subscriptionManager.findByConsumerTag(consumerTag).ifPresent(sub -> {
            QueuedMessage removed = sub.remove(deliveryTag);
            System.out.println("ACK received for consumerTag=" + consumerTag + ", deliveryTag=" + deliveryTag + "messageId=" + removed.getMessageId() + ", removed=" + (removed != null));
            if (removed != null) {
                tryDispatch(sub.queueName());
            }
        });
    }

    public void negativeAcknowledge(String consumerTag, long deliveryTag, boolean requeue) {
        subscriptionManager.findByConsumerTag(consumerTag).ifPresent(sub -> {
            QueuedMessage removed = sub.remove(deliveryTag);
            System.out.println("NACK received for consumerTag=" + consumerTag + ", deliveryTag=" + deliveryTag + "messageId=" + removed.getMessageId() + ", removed=" + (removed != null));
            if (removed != null) {
                if (requeue) {
                    queueManager.requeue(sub.queueName(), removed);
                }
                tryDispatch(sub.queueName());
            }
        });
    }

    public void shutdown() {
        executor.shutdownNow();
    }
}
