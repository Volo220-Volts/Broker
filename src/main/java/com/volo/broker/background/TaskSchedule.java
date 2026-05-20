package com.volo.broker.background;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Simple schedule descriptor for periodic background tasks.
 */
public final class TaskSchedule {
    private final long initialDelay;
    private final long period;
    private final TimeUnit unit;

    public TaskSchedule(long initialDelay, long period, TimeUnit unit) {
        this.initialDelay = initialDelay;
        this.period = period;
        this.unit = Objects.requireNonNull(unit);
    }

    public long getInitialDelay() { return initialDelay; }
    public long getPeriod() { return period; }
    public TimeUnit getUnit() { return unit; }

    public static TaskSchedule of(long initialDelay, long period, TimeUnit unit) {
        return new TaskSchedule(initialDelay, period, unit);
    }

    public static TaskSchedule ofSeconds(long initialDelaySecs, long periodSecs) {
        return new TaskSchedule(initialDelaySecs, periodSecs, TimeUnit.SECONDS);
    }
}
