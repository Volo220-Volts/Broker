package com.volo.broker.broker.command.handler;

import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.model.DeleteQueueCommand;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.validation.QueuePathValidator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteQueueCommandHandlerTest {

    private final QueueManager queueManager = new InMemoryQueueManager();
    private final QueuePathValidator queuePathValidator = new QueuePathValidator();

    private final DeleteQueueCommandHandler handler = new DeleteQueueCommandHandler(
            queueManager,
            queuePathValidator
    );

    @Test
    void handle_shouldDeleteExistingQueue() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        var result = handler.handle(new DeleteQueueCommand("orders.created"));

        assertThat(result.success()).isTrue();
        assertThat(queueManager.find("orders.created")).isEmpty();
    }

    @Test
    void handle_shouldReturnQueueNotFoundWhenQueueDoesNotExist() {
        var result = handler.handle(new DeleteQueueCommand("orders.created"));

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.QUEUE_NOT_FOUND);
    }

    @Test
    void handle_shouldRejectInvalidQueuePath() {
        var result = handler.handle(new DeleteQueueCommand("orders.#"));

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.INVALID_REQUEST);
    }
}