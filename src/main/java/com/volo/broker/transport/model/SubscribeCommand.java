package com.volo.broker.transport.model;

import com.volo.broker.engine.ConsumerChannel;

public final class SubscribeCommand implements ConsumerCommand {
    public final String queueName;
    public final int prefetchCount;
    public final String consumerTag;
    public final ConsumerChannel channel;

    public SubscribeCommand(String queueName, int prefetchCount, String consumerTag, ConsumerChannel channel) {
        this.queueName = queueName;
        this.prefetchCount = Math.max(1, prefetchCount);
        this.consumerTag = consumerTag;
        this.channel = channel;
    }
}
