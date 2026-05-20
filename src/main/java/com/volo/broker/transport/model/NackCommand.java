package com.volo.broker.transport.model;

public final class NackCommand implements ConsumerCommand {
    public final String consumerTag;
    public final long deliveryTag;
    public final long messageId;
    public final boolean requeue;

    public NackCommand(String consumerTag, long deliveryTag, long messageId, boolean requeue) {
        this.consumerTag = consumerTag;
        this.deliveryTag = deliveryTag;
        this.messageId = messageId;
        this.requeue = requeue;
    }
}
