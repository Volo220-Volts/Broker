package com.volo.broker.background;

import com.volo.broker.engine.QueueManager;
import com.volo.broker.engine.SubscriptionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Example periodic task that inspects queue and subscription state and emits
 * lightweight metrics/logging. This demonstrates registering tasks with the scheduler.
 */
@Component
public class ExampleCleanupTask implements BackgroundTask {
    private static final Logger log = LoggerFactory.getLogger(ExampleCleanupTask.class);

    private final QueueManager queueManager;
    private final SubscriptionManager subscriptionManager;
    private final BackgroundTaskScheduler scheduler;

    public ExampleCleanupTask(QueueManager queueManager, SubscriptionManager subscriptionManager, BackgroundTaskScheduler scheduler) {
        this.queueManager = queueManager;
        this.subscriptionManager = subscriptionManager;
        this.scheduler = scheduler;

        // register with modest cadence so it doesn't spam logs in tests
        try {
            scheduler.register(this, TaskSchedule.ofSeconds(5, 30));
        } catch (Exception e) {
            log.warn("Failed to register ExampleCleanupTask with scheduler", e);
        }
    }

    @Override
    public String name() {
        return "example-cleanup";
    }

    @Override
    public void execute() {
        try {
            // Lightweight health insight — avoid heavy operations here.
            boolean hasAvailable = subscriptionManager.hasAvailableSubscriber("default");
            log.debug("ExampleCleanupTask: hasAvailableSubscriber(default)={}", hasAvailable);
            // More useful logic could be added (requeueing stale inflight, pruning, metrics)
        } catch (Throwable t) {
            log.warn("ExampleCleanupTask failed", t);
        }
    }
}
