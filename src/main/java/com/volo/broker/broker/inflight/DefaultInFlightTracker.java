package com.volo.broker.broker.inflight;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.volo.broker.broker.message.QueuedMessage;

public class DefaultInFlightTracker implements InFlightTracker {
    private final AtomicLong counter = new AtomicLong(0);
    private final ConcurrentMap<Long, QueuedMessage> inflight = new ConcurrentSkipListMap<>();

    @Override
    public long nextTag() {
        return counter.incrementAndGet();
    }

    @Override
    public void track(long deliveryTag, QueuedMessage message) {
        inflight.put(deliveryTag, message);
    }

    @Override
    public QueuedMessage remove(long deliveryTag) {
        return inflight.remove(deliveryTag);
    }

    @Override
    public Collection<QueuedMessage> drainAll() {
        var drained = java.util.List.copyOf(inflight.values());
        inflight.clear();
        return drained;
    }

    @Override
    public int size() {
        return inflight.size();
    }
}
