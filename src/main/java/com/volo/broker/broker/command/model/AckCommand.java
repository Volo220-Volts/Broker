package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;

public record AckCommand(
    String consumerTag,
    long deliveryTag,
    long messageId
) implements TransportCommand {

}
