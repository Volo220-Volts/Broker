package com.volo.broker.transport.model;

import java.util.Map;

public final class PublishCommand {
    public final String routingKey;
    public final byte[] payload;
    public final Map<String, String> headers;

    public PublishCommand(String routingKey, byte[] payload, Map<String, String> headers) {
        this.routingKey = routingKey;
        this.payload = payload == null ? new byte[0] : payload.clone();
        this.headers = headers;
    }
}
