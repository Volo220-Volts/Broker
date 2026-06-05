package com.volo.broker.broker.services;

import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.subscription.SubscriptionManager;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class QueueLifecycleService {
    private final QueueManager queueManager;
    private final SubscriptionManager subscriptionManager;

    public QueueLifecycleService(
            QueueManager queueManager,
            SubscriptionManager subscriptionManager
    ) {
        this.queueManager = Objects.requireNonNull(queueManager);
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager);
    }

    public void cleanupIdleQueues() {
        long now = System.currentTimeMillis();

        for (BrokerQueue queue : queueManager.all()) {
            if (shouldDelete(queue, now)) {
                queueManager.delete(queue.path());
            }
        }
    }

    boolean shouldDelete(BrokerQueue queue, long now) {
        if (!queue.isEmpty()) {
            return false;
        }

        if (subscriptionManager.hasSubscribers(queue.queuePath())) {
            return false;
        }

        if (queue.settings().lifecyclePolicy().autoDelete()) {
            return true;
        }

        if (!queue.settings().lifecyclePolicy().idleTimeoutEnabled()) {
            return false;
        }

        long idleForMs = now - queue.lastActivityAt();

        return idleForMs >= queue.settings().lifecyclePolicy().idleTimeoutMs();
    }
}