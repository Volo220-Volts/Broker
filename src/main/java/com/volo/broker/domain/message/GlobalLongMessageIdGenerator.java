package com.volo.broker.domain.message;

import com.volo.broker.persistence.StoredMessageRepository;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class GlobalLongMessageIdGenerator implements MessageIdGenerator {
    private final AtomicLong counter;

    public GlobalLongMessageIdGenerator(StoredMessageRepository repository) {
        this.counter = new AtomicLong(Math.max(0, repository.findMaxMessageId()));
    }

    @Override
    public long nextId() {
        return counter.incrementAndGet();
    }
}
