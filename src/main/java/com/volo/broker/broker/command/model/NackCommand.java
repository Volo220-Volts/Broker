package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;

public record NackCommand(
    String consumerTag,
    long deliveryTag,
    long messageId,
    boolean requeue
) implements TransportCommand {

    
}
