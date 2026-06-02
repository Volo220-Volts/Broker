package com.volo.broker.broker.inflight;

import java.util.Collection;

import com.volo.broker.broker.message.QueuedMessage;

public interface InFlightTracker {
    long nextTag();
    void track(long deliveryTag, QueuedMessage message);
    QueuedMessage remove(long deliveryTag);
    Collection<QueuedMessage> drainAll();
    int size();
}
