package com.volo.broker.engine;

import java.util.Collections;
import java.util.Map;

public final class QueuedMessage {
    private final long messageId;
    private final byte[] payload;
    private final Map<String, String> headers;
    private final String routingKey;
    private final long createdAt;
    private final DeliveryMode deliveryMode;

    public QueuedMessage(long messageId, byte[] payload, Map<String, String> headers, String routingKey, long createdAt, DeliveryMode deliveryMode) {
        this.messageId = messageId;
        this.payload = payload == null ? new byte[0] : payload.clone();
        this.headers = headers == null ? Collections.emptyMap() : Collections.unmodifiableMap(headers);
        this.routingKey = routingKey;
        this.createdAt = createdAt;
        this.deliveryMode = deliveryMode == null ? DeliveryMode.TRANSIENT : deliveryMode;
    }

    public long getMessageId() { return messageId; }
    public byte[] getPayload() { return payload.clone(); }
    public Map<String, String> getHeaders() { return headers; }
    public String getRoutingKey() { return routingKey; }
    public long getCreatedAt() { return createdAt; }
    public DeliveryMode getDeliveryMode() { return deliveryMode; }
}
