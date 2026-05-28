package com.volo.broker.core.command.model;

import com.volo.broker.core.command.TransportCommand;

public final class AckCommand implements TransportCommand {
    public final String consumerTag;
    public final long deliveryTag;
    public final long messageId;

    public AckCommand(String consumerTag, long deliveryTag, long messageId) {
        this.consumerTag = consumerTag;
        this.deliveryTag = deliveryTag;
        this.messageId = messageId;
    }
}
