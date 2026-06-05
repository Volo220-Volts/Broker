package com.volo.broker.broker.error;

public enum BrokerErrorCode {
    INVALID_REQUEST,
    PAYLOAD_TOO_LARGE,
    UNSUPPORTED_DELIVERY_GUARANTEE,
    INTERNAL_ERROR,
    QUEUE_ALREADY_EXISTS,
    QUEUE_NOT_FOUND,
    QUEUE_OVERFLOW
}
