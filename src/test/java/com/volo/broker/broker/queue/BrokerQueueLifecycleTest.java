package com.volo.broker.broker.queue;

import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.router.QueuePath;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerQueueLifecycleTest {

    @Test
    void enqueue_shouldUpdateLastActivityAt() throws InterruptedException {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        long before = queue.lastActivityAt();

        Thread.sleep(5);

        queue.enqueue(TestMessages.message(1));

        assertThat(queue.lastActivityAt())
                .isGreaterThan(before);
    }

    @Test
    void poll_shouldUpdateLastActivityAt() throws InterruptedException {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        queue.enqueue(TestMessages.message(1));

        long before = queue.lastActivityAt();

        Thread.sleep(5);

        queue.poll();

        assertThat(queue.lastActivityAt())
                .isGreaterThan(before);
    }

    @Test
    void touch_shouldUpdateLastActivityAt() throws InterruptedException {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        long before = queue.lastActivityAt();

        Thread.sleep(5);

        queue.touch();

        assertThat(queue.lastActivityAt())
                .isGreaterThan(before);
    }
}