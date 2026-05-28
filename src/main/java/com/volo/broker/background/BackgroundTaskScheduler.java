package com.volo.broker.background;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.springframework.beans.factory.DisposableBean;
import java.util.Objects;
import java.util.concurrent.*;


@Component
public class BackgroundTaskScheduler implements DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(BackgroundTaskScheduler.class);

    private final ScheduledExecutorService executor;
    private final ConcurrentMap<String, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();

    public BackgroundTaskScheduler() {
        int threads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        this.executor = Executors.newScheduledThreadPool(threads, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("background-task-" + t.threadId());
            return t;
        });
        log.info("BackgroundTaskScheduler started with {} threads", threads);
    }

    public void register(BackgroundTask task, TaskSchedule schedule) {
        Objects.requireNonNull(task);
        Objects.requireNonNull(schedule);
        String name = task.name();
        if (tasks.containsKey(name)) {
            throw new IllegalStateException("Task already registered: " + name);
        }
        ScheduledFuture<?> future = executor.scheduleAtFixedRate(() -> {
            try {
                task.execute();
            } catch (Throwable t) {
                log.error("Background task '{}' failed", name, t);
            }
        }, schedule.getInitialDelay(), schedule.getPeriod(), schedule.getUnit());
        tasks.put(name, future);
        log.info("Registered background task '{}' ({} {}, initialDelay={})", name, schedule.getPeriod(), schedule.getUnit(), schedule.getInitialDelay());
    }

    public boolean unregister(String name) {
        ScheduledFuture<?> f = tasks.remove(name);
        if (f == null) return false;
        boolean cancelled = f.cancel(false);
        log.info("Unregistered background task '{}' cancelled={} ", name, cancelled);
        return cancelled;
    }

    @Override
    public void destroy() {
        log.info("Shutting down BackgroundTaskScheduler, cancelling {} tasks", tasks.size());
        tasks.values().forEach(f -> f.cancel(false));
        executor.shutdownNow();
    }

    
    public void shutdown() {
        destroy();
    }
}
