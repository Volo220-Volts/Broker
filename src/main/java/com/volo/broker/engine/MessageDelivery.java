package com.volo.broker.engine;

public final class MessageDelivery {
    private final long deliveryTag;
    private final QueuedMessage message;

    private MessageDelivery(long deliveryTag, QueuedMessage message) {
        this.deliveryTag = deliveryTag;
        this.message = message;
    }

    public static MessageDelivery of(long deliveryTag, QueuedMessage message) {
        return new MessageDelivery(deliveryTag, message);
    }

    public long deliveryTag() { return deliveryTag; }
    public QueuedMessage message() { return message; }
}
