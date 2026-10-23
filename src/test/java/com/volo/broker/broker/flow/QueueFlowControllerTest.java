package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.assertThat;

class QueueFlowControllerTest {

    private GlobalMemoryGuard globalMemoryGuard;
    private QueueFlowController controller;

    @BeforeEach
    void setUp() {
        FlowControlProperties props = new FlowControlProperties(true, null, null, 16);
        globalMemoryGuard = new GlobalMemoryGuard(props);
        controller = new QueueFlowController(globalMemoryGuard, props);
    }

    @Test
    @DisplayName("isAnyBlocked returns false when queues are open and true when any is blocked")
    void isAnyBlockedChecks() {
        assertThat(controller.isAnyBlocked(List.of("q1", "q2"))).isFalse();

        controller.handleTransition("q1", FlowTransition.TO_BLOCKED);
        assertThat(controller.isAnyBlocked(List.of("q1", "q2"))).isTrue();
        assertThat(controller.isAnyBlocked(List.of("q2"))).isFalse();

        controller.handleTransition("q1", FlowTransition.TO_OPEN);
        assertThat(controller.isAnyBlocked(List.of("q1", "q2"))).isFalse();
    }

    @Test
    @DisplayName("park double-check immediately resumes if queue is not blocked")
    void parkResumesImmediatelyWhenNotBlocked() {
        TestFlowGate gate = new TestFlowGate();
        controller.park(gate, List.of("open.queue"));

        assertThat(gate.resumed.get()).isTrue();
    }

    @Test
    @DisplayName("parked gates resume when queue transitions to OPEN")
    void parkedGatesResumeOnTransitionToOpen() throws InterruptedException {
        controller.handleTransition("busy.queue", FlowTransition.TO_BLOCKED);

        int gateCount = 50;
        CountDownLatch latch = new CountDownLatch(gateCount);
        TestFlowGate[] gates = new TestFlowGate[gateCount];

        for (int i = 0; i < gateCount; i++) {
            gates[i] = new TestFlowGate(latch);
            controller.park(gates[i], List.of("busy.queue"));
            assertThat(gates[i].resumed.get()).isFalse();
        }

        // Transition to OPEN -> all gates should resume via virtual thread executor
        controller.handleTransition("busy.queue", FlowTransition.TO_OPEN);

        boolean finished = latch.await(5, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        for (TestFlowGate g : gates) {
            assertThat(g.resumed.get()).isTrue();
        }
    }

    @Test
    @DisplayName("gate parked on multiple queues only resumes when ALL target queues are OPEN")
    void gateWaitingOnMultipleQueues() throws InterruptedException {
        controller.handleTransition("q1", FlowTransition.TO_BLOCKED);
        controller.handleTransition("q2", FlowTransition.TO_BLOCKED);

        CountDownLatch latch = new CountDownLatch(1);
        TestFlowGate gate = new TestFlowGate(latch);

        controller.park(gate, List.of("q1", "q2"));
        assertThat(gate.resumed.get()).isFalse();

        // Open q1 only -> gate still waits on q2
        controller.handleTransition("q1", FlowTransition.TO_OPEN);
        Thread.sleep(100);
        assertThat(gate.resumed.get()).isFalse();

        // Open q2 -> now gate can resume
        controller.handleTransition("q2", FlowTransition.TO_OPEN);
        boolean finished = latch.await(2, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        assertThat(gate.resumed.get()).isTrue();
    }

    @Test
    @DisplayName("deleting a queue unblocks waiting gates")
    void deletedQueueUnblocksWaiters() throws InterruptedException {
        controller.handleTransition("temp.queue", FlowTransition.TO_BLOCKED);

        CountDownLatch latch = new CountDownLatch(1);
        TestFlowGate gate = new TestFlowGate(latch);
        controller.park(gate, List.of("temp.queue"));

        controller.onQueueDeleted("temp.queue");

        boolean finished = latch.await(2, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        assertThat(gate.resumed.get()).isTrue();
    }

    @Test
    @DisplayName("overdue timeout task triggers notifyOverdue when gate waits longer than maxBlockMs")
    void overdueTimeout_triggersNotifyOverdue() throws InterruptedException {
        // maxBlockMs = 50ms
        controller.handleTransition("timed.queue", FlowTransition.TO_BLOCKED, 50L);

        CountDownLatch overdueLatch = new CountDownLatch(1);
        TestFlowGate gate = new TestFlowGate(null, overdueLatch);
        gate.pausedAt = System.currentTimeMillis() - 100; // already waited 100ms

        controller.park(gate, List.of("timed.queue"));

        // Wait for background timeout task (runs every 200ms) or call checkOverdueGates directly
        controller.checkOverdueGates();

        assertThat(gate.overdueNotified.get()).isTrue();
    }

    private static class TestFlowGate implements FlowGate {
        final AtomicBoolean resumed = new AtomicBoolean(false);
        final AtomicBoolean overdueNotified = new AtomicBoolean(false);
        final CountDownLatch latch;
        final CountDownLatch overdueLatch;
        volatile long pausedAt = 0;

        TestFlowGate() {
            this(null, null);
        }

        TestFlowGate(CountDownLatch latch) {
            this(latch, null);
        }

        TestFlowGate(CountDownLatch latch, CountDownLatch overdueLatch) {
            this.latch = latch;
            this.overdueLatch = overdueLatch;
        }

        @Override
        public void resume() {
            resumed.set(true);
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public long pausedAtTimestamp() {
            return pausedAt;
        }

        @Override
        public void notifyOverdue() {
            overdueNotified.set(true);
            if (overdueLatch != null) {
                overdueLatch.countDown();
            }
        }

        @Override
        public void onFlowNotice(boolean blocked, String reason) {
        }
    }
}

