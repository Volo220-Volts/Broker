package com.volo.broker.broker.message;

import com.volo.broker.persistence.StoredMessageRepository;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

@Component
public class GlobalLongMessageIdGenerator implements MessageIdGenerator {
    private final AtomicLong counter;
    private final Logger logger = Logger.getLogger(GlobalLongMessageIdGenerator.class.getName());

    public GlobalLongMessageIdGenerator(StoredMessageRepository repository) {

        Long maxId = 0L;
        try {
            maxId = repository.findMaxMessageId();
        } catch (Exception e) {
            logger.warning("Failed to retrieve max message ID from repository. Starting from 0. Error: ");
        }

        this.counter = new AtomicLong(Math.max(0, maxId));
    }

    @Override
    public long nextId() {
        return counter.incrementAndGet();
    }
}
