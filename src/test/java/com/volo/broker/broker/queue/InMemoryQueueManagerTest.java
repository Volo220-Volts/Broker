package com.volo.broker.broker.queue;

import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.queue.exception.QueueAlreadyExistsException;
import com.volo.broker.broker.queue.exception.QueueNotFoundException;
import com.volo.broker.broker.router.QueuePath;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class InMemoryQueueManagerTest {

    @Test
    void declare_shouldCreateQueue() {
        QueueManager queueManager = new InMemoryQueueManager();

        BrokerQueue queue = queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        assertThat(queue.name()).isEqualTo("orders.created");
        assertThat(queue.settings().persistent()).isTrue();
    }

    @Test
    void declare_shouldThrowWhenQueueAlreadyExists() {
        QueueManager queueManager = new InMemoryQueueManager();

        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.reliable());

        assertThatThrownBy(() ->
                queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.memoryOnlyNoConsumerAck())
        ).isInstanceOf(QueueAlreadyExistsException.class);
    }

    @Test
    void resolve_shouldReturnQueuesMatchedByPattern() {
        QueueManager queueManager = new InMemoryQueueManager();

        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.reliable());
        queueManager.declare(QueuePath.of("orders.deleted"), TestQueueSettings.reliable());
        queueManager.declare(QueuePath.of("payments.created"), TestQueueSettings.reliable());

        assertThat(queueManager.resolve("orders.*"))
                .extracting(BrokerQueue::name)
                .containsExactlyInAnyOrder("orders.created", "orders.deleted");
    }

    @Test
    void delete_shouldRemoveQueue() {
        QueueManager queueManager = new InMemoryQueueManager();

        QueuePath path = QueuePath.of("orders.created");

        queueManager.declare(path, TestQueueSettings.reliable());
        queueManager.delete(path);

        assertThat(queueManager.find("orders.created")).isEmpty();
    }

    @Test
    void require_shouldThrowWhenQueueDoesNotExist() {
        QueueManager queueManager = new InMemoryQueueManager();

        assertThatThrownBy(() -> queueManager.require("missing.queue"))
                .isInstanceOf(QueueNotFoundException.class);
    }
}