package com.volo.broker.persistence;

import com.volo.broker.broker.message.QueuedMessage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class JpaMessageStoreTest {

    @Autowired
    private JpaMessageStore messageStore;

    @Autowired
    private StoredMessageRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    @Test
    void save_shouldPersistMessage() {
        QueuedMessage message = message(100L, "orders.created");

        messageStore.save("orders.created", message);

        repository.flush();
        
        assertThat(repository.findAll())
                .hasSize(1)
                .first()
                .satisfies(entity -> {
                    assertThat(entity.getId()).isEqualTo(100L);
                    assertThat(entity.getQueueName()).isEqualTo("orders.created");
                    assertThat(entity.getRoutingKey()).isEqualTo("orders.created");
                });
    }

    @Test
    void delete_shouldRemoveMessageByMessageId() {
        QueuedMessage message = message(100L, "orders.created");

        messageStore.save("orders.created", message);
        messageStore.delete(100L);

        assertThat(repository.findAll())
                .isEmpty();
    }

    @Test
    void loadAllPending_shouldRestoreMessages() {
        messageStore.save("orders.created", message(100L, "orders.created"));
        messageStore.save("orders.deleted", message(200L, "orders.deleted"));

        var pending = messageStore.loadAllPending();

        assertThat(pending)
                .hasSize(2);
    }

    @Test
    void findMaxMessageId_shouldReturnMaxStoredMessageId() {
        messageStore.save("orders.created", message(100L, "orders.created"));
        messageStore.save("orders.deleted", message(200L, "orders.deleted"));

        long maxId = messageStore.findMaxMessageId();

        assertThat(maxId).isEqualTo(200L);
    }

    private QueuedMessage message(long id, String routingKey) {
        return new QueuedMessage(
                id,
                ("payload-" + id).getBytes(),
                Map.of("type", "test"),
                routingKey,
                System.currentTimeMillis(),
                false,
                0
        );
    }
}