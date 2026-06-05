package com.volo.broker.broker.message;

public interface MessageIdGenerator {
    long nextId();

    void initialize(long maxMessageId);
}
