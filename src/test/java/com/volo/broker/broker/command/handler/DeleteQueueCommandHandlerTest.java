package com.volo.broker.broker.command.handler;

import com.volo.broker.broker.command.model.DeleteQueueCommand;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.queue.exception.QueueNotFoundException;
import com.volo.broker.broker.services.QueueDeclarationService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DeleteQueueCommandHandlerTest {

    private final QueueDeclarationService queueDeclarationService =
            mock(QueueDeclarationService.class);

    private final DeleteQueueCommandHandler handler =
            new DeleteQueueCommandHandler(queueDeclarationService);

    @Test
    void handle_shouldDeleteQueue() {
        DeleteQueueCommand command = new DeleteQueueCommand("orders.created");

        var result = handler.handle(command);

        assertThat(result.success()).isTrue();

        verify(queueDeclarationService).delete("orders.created");
    }

    @Test
    void handle_shouldReturnQueueNotFoundWhenServiceThrowsQueueNotFound() {
        DeleteQueueCommand command = new DeleteQueueCommand("orders.created");

        doThrow(new QueueNotFoundException("Queue does not exist: orders.created"))
                .when(queueDeclarationService)
                .delete(command.queuePath());

        var result = handler.handle(command);

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.QUEUE_NOT_FOUND);
        assertThat(result.errorMessage()).contains("Queue does not exist");
    }

    @Test
    void handle_shouldReturnInvalidRequestWhenServiceThrowsIllegalArgumentException() {
        DeleteQueueCommand command = new DeleteQueueCommand("orders.*");

        doThrow(new IllegalArgumentException("queue path must not contain wildcards"))
                .when(queueDeclarationService)
                .delete(command.queuePath());

        var result = handler.handle(command);

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.INVALID_REQUEST);
        assertThat(result.errorMessage()).contains("wildcards");
    }

    @Test
    void handle_shouldReturnInternalErrorWhenUnexpectedExceptionOccurs() {
        DeleteQueueCommand command = new DeleteQueueCommand("orders.created");

        doThrow(new RuntimeException("db error"))
                .when(queueDeclarationService)
                .delete(command.queuePath());

        var result = handler.handle(command);

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.INTERNAL_ERROR);
    }
}