package com.volo.broker.broker.services;

import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.TestConsumerChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueueLifecycleServiceTest {

    private final QueueManager queueManager = new InMemoryQueueManager();
    private final SubscriptionManager subscriptionManager = new DefaultSubscriptionManager();

    private final QueueLifecycleService service = new QueueLifecycleService(
            queueManager,
            subscriptionManager
    );

    @Test
    void cleanupIdleQueues_shouldDeleteAutoDeleteQueueWhenEmptyAndNoSubscribers() {
        queueManager.declare(
                QueuePath.of("temporary.reply"),
                TestQueueSettings.autoDeleteMemoryOnly()
        );

        service.cleanupIdleQueues();

        assertThat(queueManager.find("temporary.reply"))
                .isEmpty();
    }

    @Test
    void cleanupIdleQueues_shouldNotDeleteAutoDeleteQueueWhenItHasMessages() {
        queueManager.declare(
                QueuePath.of("temporary.reply"),
                TestQueueSettings.autoDeleteMemoryOnly()
        );

        queueManager.require("temporary.reply")
                .enqueue(TestMessages.message(1));

        service.cleanupIdleQueues();

        assertThat(queueManager.find("temporary.reply"))
                .isPresent();
    }

    @Test
    void cleanupIdleQueues_shouldNotDeleteAutoDeleteQueueWhenItHasSubscribers() {
        queueManager.declare(
                QueuePath.of("temporary.reply"),
                TestQueueSettings.autoDeleteMemoryOnly()
        );

        subscriptionManager.register(
                "temporary.reply",
                new TestConsumerChannel(),
                10
        );

        service.cleanupIdleQueues();

        assertThat(queueManager.find("temporary.reply"))
                .isPresent();
    }

    @Test
    void shouldDelete_shouldReturnTrueWhenIdleTimeoutExceeded() {
        queueManager.declare(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.withIdleTimeout(1_000L)
        );

        var queue = queueManager.require("metrics.runtime");

        boolean shouldDelete = service.shouldDelete(
                queue,
                queue.lastActivityAt() + 1_000L
        );

        assertThat(shouldDelete).isTrue();
    }

    @Test
    void shouldDelete_shouldReturnFalseWhenIdleTimeoutNotExceeded() {
        queueManager.declare(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.withIdleTimeout(1_000L)
        );

        var queue = queueManager.require("metrics.runtime");

        boolean shouldDelete = service.shouldDelete(
                queue,
                queue.lastActivityAt() + 999L
        );

        assertThat(shouldDelete).isFalse();
    }

    @Test
    void shouldDelete_shouldReturnFalseWhenIdleTimeoutDisabledAndAutoDeleteDisabled() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        var queue = queueManager.require("orders.created");

        boolean shouldDelete = service.shouldDelete(
                queue,
                queue.lastActivityAt() + 60_000L
        );

        assertThat(shouldDelete).isFalse();
    }
}