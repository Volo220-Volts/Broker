package com.volo.broker.engine;

import java.util.Collection;

public interface InFlightTracker {
    long nextTag();
    void track(long deliveryTag, QueuedMessage message);
    QueuedMessage remove(long deliveryTag);
    Collection<QueuedMessage> drainAll();
    int size();
}
