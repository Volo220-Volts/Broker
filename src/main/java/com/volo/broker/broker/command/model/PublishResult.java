package com.volo.broker.broker.command.model;

import com.volo.broker.broker.error.BrokerErrorCode;

public record PublishResult ( 
    boolean accepted,
    long messageId,
    BrokerErrorCode errorCode,
    String errorMessage){

    public static PublishResult ack(long messageId) {
        return new PublishResult(true, messageId, null, null);
    }

    public static PublishResult nack(BrokerErrorCode errorCode, String errorMessage) {
        return new PublishResult(false, -1, errorCode, errorMessage);
    }
}
