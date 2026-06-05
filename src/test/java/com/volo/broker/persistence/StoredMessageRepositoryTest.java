package com.volo.broker.persistence;

import com.volo.broker.persistence.entity.StoredMessageEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
class StoredMessageRepositoryTest {

    @Autowired
    private StoredMessageRepository repository;

    @Test
    void save_shouldAssignId() {
        StoredMessageEntity entity = entity("orders.created", 100L);

        StoredMessageEntity saved = repository.saveAndFlush(entity);

        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getId()).isEqualTo(100L);
    }

    @Test
    void findAll_shouldReturnPendingMessages() {
        repository.save(entity("orders.created", 100L));
        repository.save(entity("orders.deleted", 200L));
        repository.flush();

        assertThat(repository.findAll())
                .hasSize(2)
                .extracting(StoredMessageEntity::getId)
                .containsExactlyInAnyOrder(100L, 200L);
    }

    @Test
    void deleteByMessageId_shouldRemoveMessage() {
        repository.saveAndFlush(entity("orders.created", 100L));

        repository.deleteByMessageId(100L);
        repository.flush();

        assertThat(repository.findAll())
                .isEmpty();
    }

    @Test
    void findMaxMessageId_shouldReturnMaxId() {
        repository.save(entity("orders.created", 100L));
        repository.save(entity("orders.deleted", 200L));
        repository.flush();

        Long maxId = repository.findMaxMessageId();

        assertThat(maxId).isEqualTo(200L);
    }

    private StoredMessageEntity entity(String queueName, long messageId) {
        StoredMessageEntity entity = new StoredMessageEntity();

        entity.setId(messageId);
        entity.setQueueName(queueName);
        entity.setRoutingKey(queueName);
        entity.setPayload(("payload-" + messageId).getBytes());
        entity.setHeadersJson("{}");
        entity.setCreatedAt(System.currentTimeMillis());

        return entity;
    }
}