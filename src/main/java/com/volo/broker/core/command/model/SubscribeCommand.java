package com.volo.broker.core.command.model;

import com.volo.broker.core.command.TransportCommand;
import com.volo.broker.domain.subscription.ConsumerChannel;

public final class SubscribeCommand implements TransportCommand {
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
