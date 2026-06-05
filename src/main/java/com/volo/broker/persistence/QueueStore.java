package com.volo.broker.persistence;

import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.persistence.dto.StoredQueueDto;

import java.util.List;

public interface QueueStore {
    void save(BrokerQueue queue);

    void delete(String queuePath);

    List<StoredQueueDto> loadAll();
}