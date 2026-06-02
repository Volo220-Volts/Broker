package com.volo.broker.domain.router;

import org.junit.jupiter.api.Test;

import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.router.RouteTree;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RouteTreeTest {

    @Test
    void shouldResolveExactQueuePath() {
        RouteTree routeTree = new RouteTree();
        routeTree.attachQueue(QueuePath.of("something.else.queue-name"));

        Set<String> matches = routeTree.resolve("something.else.queue-name");

        assertEquals(Set.of("something.else.queue-name"), matches);
    }

    @Test
    void shouldResolveStarWildcard() {
        RouteTree routeTree = new RouteTree();
        routeTree.attachQueue(QueuePath.of("something.else.queue-name"));
        routeTree.attachQueue(QueuePath.of("something.else.other-name"));

        Set<String> matches = routeTree.resolve("something.else.*");

        assertEquals(Set.of("something.else.queue-name", "something.else.other-name"), matches);
    }

    @Test
    void shouldResolveHashWildcard() {
        RouteTree routeTree = new RouteTree();
        routeTree.attachQueue(QueuePath.of("something.else.queue-name"));
        routeTree.attachQueue(QueuePath.of("something.else.other-name"));
        routeTree.attachQueue(QueuePath.of("something.more.queue-name"));

        Set<String> matches = routeTree.resolve("something.#");

        assertEquals(
                Set.of("something.else.queue-name", "something.else.other-name", "something.more.queue-name"),
                matches
        );
    }

    @Test
    void shouldDetachQueueFromRouteTree() {
        RouteTree routeTree = new RouteTree();
        QueuePath queuePath = QueuePath.of("something.else.queue-name");
        routeTree.attachQueue(queuePath);

        routeTree.detachQueue(queuePath);

        assertTrue(routeTree.resolve("something.else.*").isEmpty());
    }
}
