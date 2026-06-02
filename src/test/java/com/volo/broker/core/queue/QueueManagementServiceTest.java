package com.volo.broker.core.queue;

import com.volo.broker.broker.command.model.QueueOperationResult;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManagementService;
import com.volo.broker.broker.router.RouteTree;
import com.volo.broker.broker.validation.QueuePathValidator;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class QueueManagementServiceTest {

    @Test
    void shouldDeclareAndDeleteQueue() {
        InMemoryQueueManager queueManager = new InMemoryQueueManager();
        RouteTree routeTree = new RouteTree();
        QueuePathValidator validator = new QueuePathValidator();
        QueueManagementService service = new QueueManagementService(queueManager, routeTree, validator);

        QueueOperationResult declareResult = service.declareQueue("something.else.queue-name");
        assertTrue(declareResult.success());
        assertTrue(queueManager.getQueue("something.else.queue-name").isPresent());
        assertEquals(Set.of("something.else.queue-name"), routeTree.resolve("something.else.*"));

        QueueOperationResult deleteResult = service.deleteQueue("something.else.queue-name");
        assertTrue(deleteResult.success());
        assertTrue(queueManager.getQueue("something.else.queue-name").isEmpty());
        assertTrue(routeTree.resolve("something.else.*").isEmpty());
    }

    @Test
    void shouldRejectInvalidQueuePathDuringDeclare() {
        InMemoryQueueManager queueManager = new InMemoryQueueManager();
        RouteTree routeTree = new RouteTree();
        QueuePathValidator validator = new QueuePathValidator();
        QueueManagementService service = new QueueManagementService(queueManager, routeTree, validator);

        QueueOperationResult declareResult = service.declareQueue("bad+queue.path");

        assertFalse(declareResult.success());
        assertEquals("queue path may contain only letters, digits, dots, underscores and hyphens", declareResult.errorMessage());
        assertNotNull(declareResult.errorCode());
    }
}
