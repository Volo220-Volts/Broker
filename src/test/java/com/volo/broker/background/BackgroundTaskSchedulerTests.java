package com.volo.broker.background;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BackgroundTaskSchedulerTests {

    private BackgroundTaskScheduler scheduler = new BackgroundTaskScheduler();

    @AfterEach
    void teardown() {
        scheduler.shutdown();
    }

    @Test
    void scheduledTaskRunsRepeatedly() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        BackgroundTask task = new BackgroundTask() {
            @Override
            public String name() { return "test-task"; }

            @Override
            public void execute() { latch.countDown(); }
        };

        scheduler.register(task, TaskSchedule.of(0, 100, TimeUnit.MILLISECONDS));

        assertTrue(latch.await(1, TimeUnit.SECONDS), "Task should have executed repeatedly");
        assertTrue(scheduler.unregister("test-task"));
    }
}
