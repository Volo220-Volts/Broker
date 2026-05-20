package com.volo.broker.background;

/**
 * Simple background task contract. Implementations should be idempotent
 * and resilient to occasional failures — the scheduler will catch exceptions.
 */
public interface BackgroundTask {
    /**
     * Human-readable unique name for the task (used for registration/unregistration).
     */
    String name();

    /**
     * Execute the task. Exceptions are caught by the scheduler and will not stop other tasks.
     */
    void execute();
}
