package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;
import com.volo.broker.broker.subscription.ConsumerChannel;

public record SubscribeCommand(
    String queueName,
    int prefetchCount,
    String consumerTag,
    ConsumerChannel channel
) implements TransportCommand {

    public SubscribeCommand(String queueName, int prefetchCount, String consumerTag, ConsumerChannel channel) {
        this.queueName = queueName;
        this.prefetchCount = prefetchCount;
        this.consumerTag = consumerTag;
        this.channel = channel;
    }
}
