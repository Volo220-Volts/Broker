package com.volo.broker.broker.services;

import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.exception.QueueAlreadyExistsException;
import com.volo.broker.broker.queue.exception.QueueNotFoundException;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.validation.QueuePathValidator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class QueueDeclarationServiceTest {

    private final QueueManager queueManager = new InMemoryQueueManager();
    private final QueuePathValidator queuePathValidator = new QueuePathValidator();
    private final QueuePersistenceService persistenceService =
            mock(QueuePersistenceService.class);

    private final QueueDeclarationService service = new QueueDeclarationService(
            queueManager,
            queuePathValidator,
            persistenceService
    );

    @Test
    void declare_shouldCreateQueueAndPersistSettings() {
        var queue = service.declare(
                "orders.created",
                TestQueueSettings.reliable()
        );

        assertThat(queue.queuePath()).isEqualTo("orders.created");
        assertThat(queue.settings().persistent()).isTrue();

        assertThat(queueManager.find("orders.created")).isPresent();

        verify(persistenceService).saveQueueIfDurable(queue);
    }

    @Test
    void declare_shouldUseDefaultSettingsWhenSettingsAreNull() {
        var queue = service.declare(
                "orders.created",
                null
        );

        assertThat(queue.settings().persistent()).isTrue();
        assertThat(queue.settings().publishAckEnabled()).isTrue();

        verify(persistenceService).saveQueueIfDurable(queue);
    }

    @Test
    void declare_shouldThrowWhenQueueAlreadyExists() {
        service.declare(
                "orders.created",
                TestQueueSettings.reliable()
        );

        assertThatThrownBy(() -> service.declare(
                "orders.created",
                TestQueueSettings.memoryOnlyNoConsumerAck()
        )).isInstanceOf(QueueAlreadyExistsException.class);
    }

    @Test
    void declare_shouldRejectInvalidQueuePath() {
        assertThatThrownBy(() -> service.declare(
                "orders.*",
                TestQueueSettings.reliable()
        )).isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(persistenceService);
    }

    @Test
    void declare_shouldRollbackRuntimeQueueWhenPersistenceFails() {
        doThrow(new RuntimeException("persistence failed"))
                .when(persistenceService)
                .saveQueueIfDurable(any());

        assertThatThrownBy(() -> service.declare(
                "orders.created",
                TestQueueSettings.reliable()
        )).isInstanceOf(RuntimeException.class);

        assertThat(queueManager.find("orders.created")).isEmpty();
    }

    @Test
    void delete_shouldRemoveRuntimeQueueAndPersistentState() {
        queueManager.declare(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        service.delete("orders.created");

        assertThat(queueManager.find("orders.created")).isEmpty();

        verify(persistenceService).deleteQueue("orders.created");
    }

    @Test
    void delete_shouldThrowWhenQueueDoesNotExist() {
        assertThatThrownBy(() -> service.delete("orders.created"))
                .isInstanceOf(QueueNotFoundException.class);

        verifyNoInteractions(persistenceService);
    }

    @Test
    void delete_shouldRejectInvalidQueuePath() {
        assertThatThrownBy(() -> service.delete("orders.*"))
                .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(persistenceService);
    }
}
