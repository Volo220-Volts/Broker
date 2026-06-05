package com.volo.broker.persistence.dto;

import com.volo.broker.broker.message.QueuedMessage;

public record StoredMessageDto(
        String queuePath,
        QueuedMessage message
) {
}