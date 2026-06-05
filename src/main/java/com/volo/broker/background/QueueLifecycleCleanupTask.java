package com.volo.broker.background;

import com.volo.broker.broker.services.QueueLifecycleService;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class QueueLifecycleCleanupTask implements BackgroundTask {
    private final QueueLifecycleService lifecycleService;

    public QueueLifecycleCleanupTask(QueueLifecycleService lifecycleService) {
        this.lifecycleService = Objects.requireNonNull(lifecycleService);
    }

    @Override
    public String name() {
        return QueueLifecycleCleanupTask.class.getSimpleName();
    }

    @Override
    public void execute() {
        lifecycleService.cleanupIdleQueues();
    }
}