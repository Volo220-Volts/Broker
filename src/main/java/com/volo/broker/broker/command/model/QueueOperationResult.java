package com.volo.broker.broker.command.model;

import com.volo.broker.broker.error.BrokerErrorCode;

public record QueueOperationResult(
        boolean success,
        BrokerErrorCode errorCode,
        String errorMessage
) {

    public static QueueOperationResult ofSuccess() {
        return new QueueOperationResult(true, null, "");
    }

    public static QueueOperationResult ofFailure(BrokerErrorCode errorCode, String errorMessage) {
        return new QueueOperationResult(false, errorCode == null ? BrokerErrorCode.INTERNAL_ERROR : errorCode,
                errorMessage == null ? "" : errorMessage);
    }
}
