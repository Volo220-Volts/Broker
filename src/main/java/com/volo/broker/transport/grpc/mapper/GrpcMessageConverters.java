package com.volo.broker.transport.grpc.mapper;

import com.custombroker.grpc.KeepAlive;
import com.custombroker.grpc.Message;
import com.custombroker.grpc.PublishAction;
import com.custombroker.grpc.PublishAck;
import com.custombroker.grpc.PublishNack;
import com.custombroker.grpc.PublisherResponse;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.error.BrokerException;
import com.volo.broker.broker.message.QueuedMessage;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

public final class GrpcMessageConverters {
    private static final int MAX_ROUTING_KEY_LENGTH = 255;
    private static final int MAX_PAYLOAD_BYTES = 1024 * 1024;
    private static final int MAX_HEADERS = 64;
    private static final int MAX_HEADER_KEY_LENGTH = 128;
    private static final int MAX_HEADER_VALUE_LENGTH = 1024;

    private GrpcMessageConverters() {
    }

    public static PublishCommand toPublishCommand(PublishAction action) {
        validatePublishAction(action);

        return new PublishCommand(
                action.getClientPublishId(),
                action.getRoutingKey(),
                action.getPayload().toByteArray(),
                sanitizeHeaders(action.getHeadersMap())
        );
    }

    public static PublisherResponse toPublisherResponse(PublishResult result) {
        if (result.accepted()) {
            return PublisherResponse.newBuilder()
                    .setAck(PublishAck.newBuilder()
                            .setClientPublishId(result.clientPublishId())
                            .setRoutedQueueCount(result.routedQueueCount())
                            .build())
                    .build();
        }

        return PublisherResponse.newBuilder()
                .setNack(PublishNack.newBuilder()
                        .setClientPublishId(result.clientPublishId())
                        .setError(GrpcErrorMapper.fromCode(result.errorCode(), result.errorMessage()))
                        .build())
                .build();
    }

    public static PublisherResponse toPongResponse() {
        return PublisherResponse.newBuilder()
                .setPong(KeepAlive.newBuilder()
                        .setTimestamp(System.currentTimeMillis())
                        .build())
                .build();
    }

    public static Message toProtoMessage(QueuedMessage message) {
        Message.Builder builder = Message.newBuilder()
                .setId(message.getMessageId())
                .setPayload(ByteString.copyFrom(message.getPayload()))
                .setTimestamp(message.getCreatedAt())
                .putAllHeaders(message.getHeaders());

        if (message.getRoutingKey() != null) {
            builder.setRoutingKey(message.getRoutingKey());
        }

        return builder.build();
    }

    private static void validatePublishAction(PublishAction action) {
        if (action == null) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "Publish action is required");
        }

        if (action.getRoutingKey() == null || action.getRoutingKey().isBlank()) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "routingKey is required");
        }

        if (action.getRoutingKey().length() > MAX_ROUTING_KEY_LENGTH) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "routingKey is too long");
        }

        if (action.getPayload() == null || action.getPayload().size() == 0) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "payload must not be empty");
        }

        if (action.getPayload().size() > MAX_PAYLOAD_BYTES) {
            throw new BrokerException(BrokerErrorCode.PAYLOAD_TOO_LARGE, "payload size exceeds broker limit");
        }
    }

    private static Map<String, String> sanitizeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }

        if (headers.size() > MAX_HEADERS) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "too many headers");
        }

        Map<String, String> sanitized = new HashMap<>();

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key == null || key.isBlank()) {
                throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "header key must not be blank");
            }

            if (key.length() > MAX_HEADER_KEY_LENGTH) {
                throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "header key is too long");
            }

            if (value != null && value.length() > MAX_HEADER_VALUE_LENGTH) {
                throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "header value is too long");
            }

            sanitized.put(key.trim(), value == null ? "" : value);
        }

        return Map.copyOf(sanitized);
    }
}
