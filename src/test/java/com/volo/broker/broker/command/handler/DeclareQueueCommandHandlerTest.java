package com.volo.broker.broker.command.handler;

import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.model.DeclareQueueCommand;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.validation.QueuePathValidator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeclareQueueCommandHandlerTest {

    private final QueueManager queueManager = new InMemoryQueueManager();
    private final QueuePathValidator queuePathValidator = new QueuePathValidator();

    private final DeclareQueueCommandHandler handler = new DeclareQueueCommandHandler(
            queueManager,
            queuePathValidator
    );

    @Test
    void handle_shouldDeclareQueueWithSettings() {
        var result = handler.handle(new DeclareQueueCommand(
                "orders.created",
                TestQueueSettings.reliable()
        ));

        assertThat(result.success()).isTrue();

        var queue = queueManager.require("orders.created");

        assertThat(queue.name()).isEqualTo("orders.created");
        assertThat(queue.settings().persistent()).isTrue();
        assertThat(queue.settings().consumerAckEnabled()).isTrue();
    }

    @Test
    void handle_shouldUseDefaultSettingsWhenSettingsAreNull() {
        var result = handler.handle(new DeclareQueueCommand(
                "orders.created",
                null
        ));

        assertThat(result.success()).isTrue();

        var queue = queueManager.require("orders.created");

        assertThat(queue.settings().persistent()).isTrue();
        assertThat(queue.settings().publishAckEnabled()).isTrue();
    }

    @Test
    void handle_shouldRejectDuplicateQueueBecauseSettingsAreImmutable() {
        handler.handle(new DeclareQueueCommand(
                "orders.created",
                TestQueueSettings.reliable()
        ));

        var result = handler.handle(new DeclareQueueCommand(
                "orders.created",
                TestQueueSettings.memoryOnlyNoConsumerAck()
        ));

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.QUEUE_ALREADY_EXISTS);

        var queue = queueManager.require("orders.created");

        assertThat(queue.settings().persistent()).isTrue();
    }

    @Test
    void handle_shouldRejectInvalidQueuePath() {
        var result = handler.handle(new DeclareQueueCommand(
                "orders.*",
                TestQueueSettings.reliable()
        ));

        assertThat(result.success()).isFalse();
        assertThat(result.errorCode()).isEqualTo(BrokerErrorCode.INVALID_REQUEST);
    }
}