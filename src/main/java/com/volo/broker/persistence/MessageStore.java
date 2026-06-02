package com.volo.broker.persistence;

import java.util.List;

import com.volo.broker.broker.message.QueuedMessage;

public interface MessageStore {

    QueuedMessage save(String queueName, QueuedMessage message);

    void delete(long messageId);
    
    List<QueuedMessage> loadAllPending();

    long findMaxMessageId();
}