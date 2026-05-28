package com.volo.broker.domain.inflight;

import java.util.Collection;

import com.volo.broker.domain.message.QueuedMessage;

public interface InFlightTracker {
    long nextTag();
    void track(long deliveryTag, QueuedMessage message);
    QueuedMessage remove(long deliveryTag);
    Collection<QueuedMessage> drainAll();
    int size();
}
