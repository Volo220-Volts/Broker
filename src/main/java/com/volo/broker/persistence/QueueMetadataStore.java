package com.volo.broker.persistence;

import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.persistence.dto.StoredQueueDto;

import java.util.List;
import java.util.Optional;

/**
 * Persistence contract for queue definitions, configuration, and metadata.
 */
public interface QueueMetadataStore {

    /**
     * Persists or updates the queue metadata for the given queue.
     */
    void save(BrokerQueue queue);

    /**
     * Persists or updates queue metadata with explicit path and settings.
     */
    void save(String queuePath, QueueSettings settings);

    /**
     * Deletes the metadata of the specified queue.
     */
    void delete(String queuePath);

    /**
     * Loads all persisted queue definitions.
     */
    List<StoredQueueDto> loadAll();

    /**
     * Finds the persisted settings for a specific queue, if present.
     */
    Optional<QueueSettings> find(String queuePath);
}

