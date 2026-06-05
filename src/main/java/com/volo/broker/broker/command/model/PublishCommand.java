package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;
import java.util.Map;

public record PublishCommand(
    Long clientPublishId,
    String routingKey,
    byte[] payload,
    Map<String, String> headers
) implements TransportCommand {

    public PublishCommand(
            Long clientPublishId,
            String routingKey,
            byte[] payload,
            Map<String, String> headers
            ) {
        this.clientPublishId = clientPublishId;
        this.routingKey = routingKey == null ? "" : routingKey.trim();
        this.payload = payload == null ? new byte[0] : payload.clone();
        this.headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
