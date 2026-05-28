package com.volo.broker.core.command.model;

import com.volo.broker.core.command.TransportCommand;
import com.volo.broker.domain.message.DeliveryGuarantee;

import java.util.Map;
import java.util.Objects;

public final class PublishCommand implements TransportCommand {
    private final String clientPublishId;
    private final String exchange;
    private final String routingKey;
    private final byte[] payload;
    private final Map<String, String> headers;
    private final DeliveryGuarantee deliveryGuarantee;

    public PublishCommand(
            String clientPublishId,
            String exchange,
            String routingKey,
            byte[] payload,
            Map<String, String> headers,
            DeliveryGuarantee deliveryGuarantee
    ) {
        this.clientPublishId = clientPublishId == null ? "" : clientPublishId.trim();
        this.exchange = exchange == null ? "" : exchange.trim();
        this.routingKey = routingKey == null ? "" : routingKey.trim();
        this.payload = payload == null ? new byte[0] : payload.clone();
        this.headers = headers == null ? Map.of() : Map.copyOf(headers);
        this.deliveryGuarantee = Objects.requireNonNullElse(deliveryGuarantee, DeliveryGuarantee.AT_LEAST_ONCE);
    }

    public String clientPublishId() {
        return clientPublishId;
    }

    public String exchange() {
        return exchange;
    }

    public String routingKey() {
        return routingKey;
    }

    public byte[] payload() {
        return payload.clone();
    }

    public Map<String, String> headers() {
        return headers;
    }

    public DeliveryGuarantee deliveryGuarantee() {
        return deliveryGuarantee;
    }
}
