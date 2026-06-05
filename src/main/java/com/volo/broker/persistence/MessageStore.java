package com.volo.broker.persistence;

import java.util.List;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.dto.StoredMessageDto;

public interface MessageStore {

    QueuedMessage save(String queuePath, QueuedMessage message);

    void delete(long messageId);

    void deleteByQueuePath(String queuePath);
    
    List<StoredMessageDto> loadAllPending();

    long findMaxMessageId();
}