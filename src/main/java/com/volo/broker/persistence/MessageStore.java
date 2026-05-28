package com.volo.broker.persistence;

import com.volo.broker.domain.message.QueuedMessage;

import java.util.List;

public interface MessageStore {

    QueuedMessage save(String queueName, QueuedMessage message);

    void delete(long messageId);
    
    List<QueuedMessage> loadAllPending();

    long findMaxMessageId();
}