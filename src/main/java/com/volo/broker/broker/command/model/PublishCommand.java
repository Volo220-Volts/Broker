package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;
import com.volo.broker.broker.message.DeliveryGuarantee;

import java.util.Map;
import java.util.Objects;

public record PublishCommand(
    String clientPublishId,
    String exchange,
    String routingKey,
    byte[] payload,
    Map<String, String> headers,
    DeliveryGuarantee deliveryGuarantee
) implements TransportCommand {

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
}
