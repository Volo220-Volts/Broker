package com.volo.broker.persistence.model;

public enum MessageStatus {
    READY,
    INFLIGHT,
    ACKED,
    DEAD
}
