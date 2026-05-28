package com.volo.broker.domain.subscription;

import java.util.Objects;

import com.volo.broker.domain.inflight.DefaultInFlightTracker;
import com.volo.broker.domain.inflight.InFlightTracker;
import com.volo.broker.domain.message.QueuedMessage;

public class Subscription {
    private final String consumerTag;
    private final String queueName;
    private final int prefetchCount;
    private final InFlightTracker inFlightTracker;
    private final ConsumerChannel channel;

    public Subscription(String consumerTag, String queueName, int prefetchCount, ConsumerChannel channel) {
        this.consumerTag = Objects.requireNonNull(consumerTag);
        this.queueName = Objects.requireNonNull(queueName);
        this.prefetchCount = Math.max(1, prefetchCount);
        this.channel = Objects.requireNonNull(channel);
        this.inFlightTracker = new DefaultInFlightTracker();
    }

    public String consumerTag() { return consumerTag; }
    public String queueName() { return queueName; }
    public int getPrefetchCount() { return prefetchCount; }
    public InFlightTracker getInFlightTracker() { return inFlightTracker; }
    public ConsumerChannel channel() { return channel; }

    public boolean canAcceptMore() {
        return inFlightTracker.size() < prefetchCount && channel.canAcceptMore();
    }

    public long nextDeliveryTag() { return inFlightTracker.nextTag(); }

    public void track(long tag, QueuedMessage message) { inFlightTracker.track(tag, message); }

    public QueuedMessage remove(long tag) { return inFlightTracker.remove(tag); }
}
