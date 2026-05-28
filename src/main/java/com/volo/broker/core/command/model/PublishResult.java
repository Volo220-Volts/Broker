package com.volo.broker.core.command.model;

import com.volo.broker.core.error.BrokerErrorCode;

public final class PublishResult {
    private final boolean accepted;
    private final long messageId;
    private final BrokerErrorCode errorCode;
    private final String errorMessage;

    private PublishResult(boolean accepted, long messageId, BrokerErrorCode errorCode, String errorMessage) {
        this.accepted = accepted;
        this.messageId = messageId;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public static PublishResult ack(long messageId) {
        return new PublishResult(true, messageId, null, null);
    }

    public static PublishResult nack(BrokerErrorCode errorCode, String errorMessage) {
        return new PublishResult(false, -1, errorCode, errorMessage);
    }

    public boolean accepted() {
        return accepted;
    }

    public long messageId() {
        return messageId;
    }

    public BrokerErrorCode errorCode() {
        return errorCode;
    }

    public String errorMessage() {
        return errorMessage;
    }
}
