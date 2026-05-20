package com.volo.broker.transport.model;

public final class PublishResult {
    public final boolean success;
    public final long messageId;
    public final String errorMessage;

    public PublishResult(boolean success, long messageId, String errorMessage) {
        this.success = success;
        this.messageId = messageId;
        this.errorMessage = errorMessage;
    }
}
