package com.volo.broker.engine;

public interface ConsumerChannel {
    String consumerTag();
    boolean canAcceptMore();
    void send(MessageDelivery delivery);
    void close(String reason);
    void onAck(long deliveryTag);
    void onNack(long deliveryTag, boolean requeue);
}
