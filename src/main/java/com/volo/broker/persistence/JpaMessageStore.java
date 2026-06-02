package com.volo.broker.persistence;

import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.entity.StoredMessageEntity;


@Component
public class JpaMessageStore implements MessageStore {

    private final StoredMessageRepository repository;
    private final ObjectMapper objectMapper;

    public JpaMessageStore(
            StoredMessageRepository repository
    ) {
        this.repository = repository;
        this.objectMapper = new ObjectMapper();
    }


    private String toJson(Map<String, String> headers) {
        try {
            return objectMapper.writeValueAsString(headers == null ? Map.of() : headers);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize headers", ex);
        }
    }

    private Map<String, String> fromJson(String json) {
        try {
            if (json == null || json.isBlank()) {
                return Map.of();
            }

            return objectMapper.readValue(
                    json,
                    new TypeReference<Map<String, String>>() {}
            );
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to deserialize headers", ex);
        }
    }

    @Override
    @Transactional
    public QueuedMessage save(String queueName, QueuedMessage message) {
        StoredMessageEntity entity = new StoredMessageEntity();

        entity.setId(message.getMessageId());
        entity.setQueueName(queueName);
        entity.setRoutingKey(message.getRoutingKey());
        entity.setPayload(message.getPayload());
        entity.setHeadersJson(toJson(message.getHeaders()));
        entity.setDeliveryGuarantee(message.getDeliveryGuarantee().name());
        entity.setCreatedAt(message.getCreatedAt());

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
    public List<QueuedMessage> loadAllPending() {
        return repository.findAllByOrderByIdAsc()
                .stream()
                .map(this::toQueuedMessage)
                .toList();
    }

    @Override
    @Transactional(readOnly = true)
    public long findMaxMessageId() {
        return repository.findMaxMessageId();
    }

    private QueuedMessage toQueuedMessage(StoredMessageEntity entity) {
        return new QueuedMessage(
                entity.getId(),
                entity.getPayload(),
                fromJson(entity.getHeadersJson()),
                entity.getRoutingKey(),
                entity.getCreatedAt(),
                DeliveryGuarantee.valueOf(entity.getDeliveryGuarantee()),
                true,
                1
        );
    }

}
