package com.volo.broker.broker.message;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class GlobalLongMessageIdGenerator implements MessageIdGenerator {
    private final AtomicLong counter = new AtomicLong();

    @Override
    public long nextId() {
        return counter.incrementAndGet();
    }

    @Override
    public void initialize(long maxMessageId) {
        counter.set(maxMessageId);
    }
}
