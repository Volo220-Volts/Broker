package com.volo.broker.broker.dispatch;

import org.junit.jupiter.api.Test;

import com.volo.broker.broker.queue.InMemoryQueueManager;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class QueueDispatchSchedulerTest {

    @Test
    void signal_shouldRunDispatchLoop() {
        AtomicInteger calls = new AtomicInteger();

        var queueManager = new InMemoryQueueManager();

        DispatchManager dispatchManager = new TestDispatchManager(calls);

            QueueDispatchScheduler scheduler = new QueueDispatchScheduler(
                    dispatchManager,
                    queueManager
            );

        try {
            scheduler.signal("orders.created");

            awaitUntil(() -> calls.get() == 1);

            assertThat(calls.get()).isEqualTo(1);
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void signal_shouldNotRunSameQueueConcurrently() {
        AtomicInteger calls = new AtomicInteger();

        var queueManager = new InMemoryQueueManager();

        DispatchManager dispatchManager = new SlowDispatchManager(calls);

            QueueDispatchScheduler scheduler = new QueueDispatchScheduler(
                    dispatchManager,
                    queueManager
            );
        try {
            scheduler.signal("orders.created");
            scheduler.signal("orders.created");
            scheduler.signal("orders.created");

            awaitUntil(() -> calls.get() >= 1);

            assertThat(calls.get()).isEqualTo(1);
        } finally {
            scheduler.shutdown();
        }
    }

    private static void awaitUntil(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 1000;

        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private interface BooleanSupplier {
        boolean getAsBoolean();
    }

    private static class TestDispatchManager extends DispatchManager {
        private final AtomicInteger calls;

        TestDispatchManager(AtomicInteger calls) {
            super(null, null, null);
            this.calls = calls;
        }

        @Override
        public void dispatchLoop(String queueName) {
            calls.incrementAndGet();
        }
    }

    private static class SlowDispatchManager extends DispatchManager {
        private final AtomicInteger calls;

        SlowDispatchManager(AtomicInteger calls) {
            super(null, null, null);
            this.calls = calls;
        }

        @Override
        public void dispatchLoop(String queueName) {
            calls.incrementAndGet();

            try {
                Thread.sleep(300);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }
}