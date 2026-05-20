package com.volo.broker.transport.model;

public final class AckCommand implements ConsumerCommand {
    public final String consumerTag;
    public final long deliveryTag;
    public final long messageId;

    public AckCommand(String consumerTag, long deliveryTag, long messageId) {
        this.consumerTag = consumerTag;
        this.deliveryTag = deliveryTag;
        this.messageId = messageId;
    }
}
