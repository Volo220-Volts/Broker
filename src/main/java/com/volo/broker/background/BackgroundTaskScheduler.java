package com.volo.broker.background;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;


@Component
public class BackgroundTaskScheduler {
    private static final Logger logger = LoggerFactory.getLogger(BackgroundTaskScheduler.class);

    private final ScheduledExecutorService executor;
    private final List<BackgroundTask> tasks;

     public BackgroundTaskScheduler(
            ScheduledExecutorService executorService,
            List<BackgroundTask> tasks
    ) {
        this.executor = Objects.requireNonNull(executorService);
        this.tasks = List.copyOf(tasks);
    }

    @PostConstruct
    public void start() {
        for (BackgroundTask task : tasks) {
            TaskSchedule schedule = task.schedule();

            executor.scheduleWithFixedDelay(
                    safeRunnable(task),
                    schedule.getInitialDelay(),
                    schedule.getPeriod(),
                    schedule.getUnit()
            );

            logger.info("Background task started: {}", task.getClass().getSimpleName());
        }
    }

    @PreDestroy
    public void stop() {
        executor.shutdownNow();
    }

    private Runnable safeRunnable(BackgroundTask task) {
        return () -> {
            try {
                task.run();
            } catch (Throwable throwable) {
                logger.warn("Background task failed: {}", task.getClass().getSimpleName(), throwable);
            }
        };
    }

    // public BackgroundTaskScheduler() {
    //     int threads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
    //     this.executor = Executors.newScheduledThreadPool(threads, r -> {
    //         Thread t = new Thread(r);
    //         t.setDaemon(true);
    //         t.setName("background-task-" + t.threadId());
    //         return t;
    //     });
    //     log.info("BackgroundTaskScheduler started with {} threads", threads);
    // }

    // public void register(BackgroundTask task, TaskSchedule schedule) {
    //     Objects.requireNonNull(task);
    //     Objects.requireNonNull(schedule);
    //     String name = task.name();
    //     if (tasks.containsKey(name)) {
    //         throw new IllegalStateException("Task already registered: " + name);
    //     }
    //     ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
    //         try {
    //             task.execute();
    //         } catch (Throwable t) {
    //             log.error("Background task '{}' failed", name, t);
    //         }
    //     }, schedule.getInitialDelay(), schedule.getPeriod(), schedule.getUnit());
    //     tasks.put(name, future);
    //     log.info("Registered background task '{}' ({} {}, initialDelay={})", name, schedule.getPeriod(), schedule.getUnit(), schedule.getInitialDelay());
    // }

    // public boolean unregister(String name) {
    //     ScheduledFuture<?> f = tasks.remove(name);
    //     if (f == null) return false;
    //     boolean cancelled = f.cancel(false);
    //     log.info("Unregistered background task '{}' cancelled={} ", name, cancelled);
    //     return cancelled;
    // }

    // @Override
    // public void destroy() {
    //     log.info("Shutting down BackgroundTaskScheduler, cancelling {} tasks", tasks.size());
    //     tasks.values().forEach(f -> f.cancel(false));
    //     executor.shutdownNow();
    // }

    
    // public void shutdown() {
    //     destroy();
    // }
}
