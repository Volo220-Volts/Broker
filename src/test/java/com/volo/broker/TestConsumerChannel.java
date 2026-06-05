package com.volo.broker;

import com.volo.broker.broker.message.MessageDelivery;
import com.volo.broker.broker.subscription.ConsumerChannel;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestConsumerChannel implements ConsumerChannel {
    private final String consumerTag = UUID.randomUUID().toString();
    private final List<MessageDelivery> deliveries = new ArrayList<>();

    private boolean closed;
    private boolean failOnSend;

    @Override
    public String consumerTag() {
        return consumerTag;
    }

    @Override
    public boolean canAcceptMore() {
        return !closed;
    }

    @Override
    public void send(MessageDelivery delivery) {
        if (failOnSend) {
            throw new RuntimeException("send failed");
        }

        deliveries.add(delivery);
    }

    @Override
    public void close(String reason) {
        closed = true;
    }

    @Override
    public void onAck(long deliveryTag) {
    }

    @Override
    public void onNack(long deliveryTag, boolean requeue) {
    }

    public List<MessageDelivery> deliveries() {
        return deliveries;
    }

    public MessageDelivery lastDelivery() {
        return deliveries.get(deliveries.size() - 1);
    }

    public void failOnSend() {
        this.failOnSend = true;
    }
}