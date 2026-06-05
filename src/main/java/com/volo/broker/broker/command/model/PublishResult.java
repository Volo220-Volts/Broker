package com.volo.broker.broker.command.model;

import com.volo.broker.broker.error.BrokerErrorCode;

public record PublishResult ( 
    boolean accepted,
    Long clientPublishId,
    int routedQueueCount,
    BrokerErrorCode errorCode,
    String errorMessage){

    public static PublishResult ack(Long clientPublishId, int routedQueueCount) {
        return new PublishResult(true, clientPublishId, routedQueueCount, null, null);
    }

    public static PublishResult nack(Long clientPublishId, BrokerErrorCode errorCode, String errorMessage) {
        return new PublishResult(false, clientPublishId, 0, errorCode, errorMessage);
    }
}
