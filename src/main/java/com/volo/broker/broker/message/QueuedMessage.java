package com.volo.broker.broker.message;

import java.util.Map;

import lombok.Getter;

@Getter
public final class QueuedMessage {

    private final long messageId;
    private final byte[] payload;
    private final Map<String, String> headers;
    private final String routingKey;
    private final long createdAt;
    private final DeliveryGuarantee deliveryGuarantee;
    private final boolean redelivered;
    private final int deliveryAttempt;

    public QueuedMessage(
            long messageId,
            byte[] payload,
            Map<String, String> headers,
            String routingKey,
            long createdAt,
            DeliveryGuarantee deliveryGuarantee,
            boolean redelivered,
            int deliveryAttempt
    ) {
        this.messageId = messageId;
        this.payload = payload == null ? new byte[0] : payload.clone();
        this.headers = headers == null ? Map.of() : Map.copyOf(headers);
        this.routingKey = routingKey;
        this.createdAt = createdAt;
        this.deliveryGuarantee = deliveryGuarantee == null
                ? DeliveryGuarantee.AT_LEAST_ONCE
                : deliveryGuarantee;
        this.redelivered = redelivered;
        this.deliveryAttempt = Math.max(0, deliveryAttempt);
    }

    public QueuedMessage asRedelivered() {
        return new QueuedMessage(
                messageId,
                payload,
                headers,
                routingKey,
                createdAt,
                deliveryGuarantee,
                true,
                deliveryAttempt + 1
        );
    }
}
