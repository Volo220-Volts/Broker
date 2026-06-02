package com.volo.broker.core.command.handler;

import com.volo.broker.broker.command.handler.PublishCommandHandler;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.DeliveryGuarantee;
import com.volo.broker.broker.message.MessageIdGenerator;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.router.RouteTree;
import com.volo.broker.broker.validation.QueuePathValidator;
import com.volo.broker.persistence.MessageStore;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PublishCommandHandlerTest {

    @Test
    void shouldPublishToWildcardResolvedQueue() {
        QueueManager queueManager = new InMemoryQueueManager();
        RouteTree routeTree = new RouteTree();
        QueuePathValidator queuePathValidator = new QueuePathValidator();
        routeTree.attachQueue(QueuePath.of("something.else.queue-name"));
        queueManager.createQueue("something.else.queue-name");

        MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.save(eq("something.else.queue-name"), any())).thenAnswer(invocation -> invocation.getArgument(1));

        QueueDispatchScheduler dispatchScheduler = mock(QueueDispatchScheduler.class);
        MessageIdGenerator idGenerator = new MessageIdGenerator() {
            private long counter;

            @Override
            public long nextId() {
                return ++counter;
            }
        };

        PublishCommandHandler handler = new PublishCommandHandler(
                queueManager,
                messageStore,
                dispatchScheduler,
                idGenerator,
                routeTree,
                queuePathValidator
        );

        PublishCommand command = new PublishCommand(
                "client-1",
                "default",
                "something.else.*",
                "payload".getBytes(),
                Map.of("x-test", "true"),
                DeliveryGuarantee.AT_LEAST_ONCE
        );

        PublishResult result = handler.handle(command);

        assertTrue(result.accepted());
        assertEquals(1, queueManager.size("something.else.queue-name"));
        verify(dispatchScheduler).signal("something.else.queue-name");
    }

    @Test
    void shouldCreateQueueWhenPublishingExactPath() {
        QueueManager queueManager = new InMemoryQueueManager();
        RouteTree routeTree = new RouteTree();
        QueuePathValidator queuePathValidator = new QueuePathValidator();

        MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.save(eq("something.new.queue"), any())).thenAnswer(invocation -> invocation.getArgument(1));

        QueueDispatchScheduler dispatchScheduler = mock(QueueDispatchScheduler.class);
        MessageIdGenerator idGenerator = new MessageIdGenerator() {
            private long counter;

            @Override
            public long nextId() {
                return ++counter;
            }
        };

        PublishCommandHandler handler = new PublishCommandHandler(
                queueManager,
                messageStore,
                dispatchScheduler,
                idGenerator,
                routeTree,
                queuePathValidator
        );

        PublishCommand command = new PublishCommand(
                "client-2",
                "default",
                "something.new.queue",
                "payload".getBytes(),
                Map.of(),
                DeliveryGuarantee.AT_LEAST_ONCE
        );

        PublishResult result = handler.handle(command);

        assertTrue(result.accepted());
        assertEquals(1, queueManager.size("something.new.queue"));
        assertTrue(routeTree.resolve("something.new.queue").contains("something.new.queue"));
        verify(dispatchScheduler).signal("something.new.queue");
    }
}
