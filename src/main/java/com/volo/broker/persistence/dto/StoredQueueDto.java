package com.volo.broker.persistence.dto;

import com.volo.broker.broker.queue.settings.QueueSettings;

public record StoredQueueDto(
        String queuePath,
        QueueSettings settings
) {
}