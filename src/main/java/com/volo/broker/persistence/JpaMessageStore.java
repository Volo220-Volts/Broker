package com.volo.broker.persistence;

import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.dto.StoredMessageDto;
import com.volo.broker.persistence.entity.StoredMessageEntity;


@Component
public class JpaMessageStore implements MessageStore {

    private final StoredMessageRepository repository;
    private final ObjectMapper objectMapper;

    private static final TypeReference<Map<String, String>> HEADERS_TYPE =
            new TypeReference<>() {
            };

    public JpaMessageStore(
            StoredMessageRepository repository
    ) {
        this.repository = repository;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    @Transactional
    public QueuedMessage save(String queuePath, QueuedMessage message) {
        StoredMessageEntity entity = new StoredMessageEntity();

        entity.setId(message.getMessageId());
        entity.setQueuePath(queuePath);
        entity.setRoutingKey(message.getRoutingKey());
        entity.setPayload(message.getPayload());
        entity.setHeadersJson(writeHeaders(message.getHeaders()));
        entity.setCreatedAt(message.getCreatedAt());
        entity.setDeliveryAttempt(message.getDeliveryAttempt());
        entity.setRedelivered(message.isRedelivered());

        repository.save(entity);

        return message;
    }

    @Override
    @Transactional
    public void delete(long messageId) {
        int deleted = repository.deleteByMessageId(messageId);

        if (deleted != 1) {
            throw new IllegalStateException("Persistent message was not deleted: " + messageId);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<StoredMessageDto> loadAllPending() {
        return repository.findAllByOrderByIdAsc()
                .stream()
                .map(this::toDto)
                .toList();
    }

    @Override
    @Transactional(readOnly = true)
    public long findMaxMessageId() {
        return repository.findMaxMessageId();
    }

    @Override
    public void deleteByQueuePath(String queueName) {
        repository.deleteByQueuePath(queueName);
    }

     private StoredMessageDto toDto(StoredMessageEntity entity) {
        QueuedMessage message = new QueuedMessage(
                entity.getId(),
                entity.getPayload(),
                readHeaders(entity.getHeadersJson()),
                entity.getRoutingKey(),
                entity.getCreatedAt(),
                entity.isRedelivered(),
                entity.getDeliveryAttempt()
        );

        return new StoredMessageDto(
                entity.getQueuePath(),
                message
        );
    }

    private String writeHeaders(Map<String, String> headers) {
        try {
            return objectMapper.writeValueAsString(headers == null ? Map.of() : headers);
        } catch (Exception exception) {
            throw new IllegalStateException("Could not serialize message headers", exception);
        }
    }

    private Map<String, String> readHeaders(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }

        try {
            return objectMapper.readValue(json, HEADERS_TYPE);
        } catch (Exception exception) {
            throw new IllegalStateException("Could not deserialize message headers", exception);
        }
    }


}
