package com.volo.broker.broker.command.handler;

import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.model.DeclareQueueCommand;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.queue.exception.QueueAlreadyExistsException;
import com.volo.broker.broker.services.QueueDeclarationService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DeclareQueueCommandHandlerTest {

    private final QueueDeclarationService queueDeclarationService =
            mock(QueueDeclarationService.class);

    private final DeclareQueueCommandHandler handler =
            new DeclareQueueCommandHandler(queueDeclarationService);

    @Test
    void handle_shouldDeclareQueue() {
        DeclareQueueCommand command = new DeclareQueueCommand(
                "orders.created",
                TestQueueSettings.reliable()
        );

        var result = handler.handle(command);

        assertThat(result.success()).isTrue();

        verify(queueDeclarationService).declare(
                command.queuePath(),
                command.settings()
        );
    }

    @Test
    void handle_shouldUseDefaultSettingsWhenSettingsAreNull() {
        DeclareQueueCommand command = new DeclareQueueCommand(
                "orders.created",
                null
        );

        var result = handler.handle(command);

        assertThat(result.success()).isTrue();

        verify(queueDeclarationService).declare(
                "orders.created",
                null
        );
    }

    @Test
    void handle_shouldReturnQueueAlreadyExistsWhenServiceThrowsQueueAlreadyExists() {
        DeclareQueueCommand command = new DeclareQueueCommand(
                "orders.created",
                TestQueueSettings.reliable()
        );

        doThrow(new QueueAlreadyExistsException("Queue already exists: orders.created"))
                .when(queueDeclarationService)
                .declare(command.queuePath(), command.settings());

        var result = handler.handle(command);

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.QUEUE_ALREADY_EXISTS);
        assertThat(result.errorMessage()).contains("Queue already exists");
    }

    @Test
    void handle_shouldReturnInvalidRequestWhenServiceThrowsIllegalArgumentException() {
        DeclareQueueCommand command = new DeclareQueueCommand(
                "orders.*",
                TestQueueSettings.reliable()
        );

        doThrow(new IllegalArgumentException("queue path must not contain wildcards"))
                .when(queueDeclarationService)
                .declare(command.queuePath(), command.settings());

        var result = handler.handle(command);

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.INVALID_REQUEST);
        assertThat(result.errorMessage()).contains("wildcards");
    }

    @Test
    void handle_shouldReturnInternalErrorWhenUnexpectedExceptionOccurs() {
        DeclareQueueCommand command = new DeclareQueueCommand(
                "orders.created",
                TestQueueSettings.reliable()
        );

        doThrow(new RuntimeException("db error"))
                .when(queueDeclarationService)
                .declare(command.queuePath(), command.settings());

        var result = handler.handle(command);

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.INTERNAL_ERROR);
    }
}