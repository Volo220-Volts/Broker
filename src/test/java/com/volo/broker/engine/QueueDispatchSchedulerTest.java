package com.volo.broker.engine;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.volo.broker.core.dispatch.DispatchManager;
import com.volo.broker.core.dispatch.QueueDispatchScheduler;
import com.volo.broker.domain.queue.QueueManager;

@ExtendWith(MockitoExtension.class)
class QueueDispatchSchedulerTest {

    @Mock
    DispatchManager dispatchManager;

    @Mock
    QueueManager queueManager;

    @Test
    void signal_shouldRunOnlyOneDispatchLoopPerQueueAtSameTime() throws Exception {
        String queueName = "orders";

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        doAnswer(invocation -> {
            started.countDown();
            assertTrue(release.await(1, TimeUnit.SECONDS));
            return null;
        }).when(dispatchManager).dispatchLoop(queueName);

        when(queueManager.size(queueName)).thenReturn(0);

        QueueDispatchScheduler scheduler = new QueueDispatchScheduler(dispatchManager, queueManager, 2);

        try {
            scheduler.signal(queueName);
            scheduler.signal(queueName);
            scheduler.signal(queueName);

            assertTrue(started.await(1, TimeUnit.SECONDS));
            release.countDown();

            Thread.sleep(150);

            verify(dispatchManager, times(1)).dispatchLoop(queueName);
        } finally {
            scheduler.shutdown();
        }
    }
}
