package com.volo.broker.background;

import com.volo.broker.broker.services.QueueLifecycleService;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class QueueLifecycleCleanupTask implements BackgroundTask {
    private final QueueLifecycleService lifecycleService;
    private final TaskSchedule schedule;

    public QueueLifecycleCleanupTask(QueueLifecycleService lifecycleService) {
        this.lifecycleService = Objects.requireNonNull(lifecycleService);
        this.schedule = TaskSchedule.ofSeconds(10, 10);
    }

    @Override
    public void run() {
        lifecycleService.cleanupIdleQueues();
    }

    @Override
    public TaskSchedule schedule() {
        return schedule;        
    }
}