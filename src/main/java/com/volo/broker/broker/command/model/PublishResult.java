package com.volo.broker.broker.command.model;

import com.volo.broker.broker.error.BrokerErrorCode;

import java.util.List;
import java.util.Objects;

public record PublishResult(
        boolean accepted,
        Long clientPublishId,
        int routedQueueCount,
        BrokerErrorCode errorCode,
        String errorMessage,
        List<String> routedQueuePaths
) {
    public PublishResult {
        routedQueuePaths = Objects.requireNonNullElse(routedQueuePaths, List.of());
    }

    public PublishResult(
            boolean accepted,
            Long clientPublishId,
            int routedQueueCount,
            BrokerErrorCode errorCode,
            String errorMessage
    ) {
        this(accepted, clientPublishId, routedQueueCount, errorCode, errorMessage, List.of());
    }

    public static PublishResult ack(Long clientPublishId, int routedQueueCount) {
        return new PublishResult(true, clientPublishId, routedQueueCount, null, null, List.of());
    }

    public static PublishResult ack(Long clientPublishId, List<String> routedQueuePaths) {
        List<String> paths = routedQueuePaths != null ? List.copyOf(routedQueuePaths) : List.of();
        return new PublishResult(true, clientPublishId, paths.size(), null, null, paths);
    }

    public static PublishResult nack(Long clientPublishId, BrokerErrorCode errorCode, String errorMessage) {
        return new PublishResult(false, clientPublishId, 0, errorCode, errorMessage, List.of());
    }
}
