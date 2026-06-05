package com.volo.broker.broker.router;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RouteTreeTest {

    @Test
    void resolve_shouldMatchExactQueuePath() {
        RouteTree routeTree = new RouteTree();

        routeTree.attachQueue(QueuePath.of("orders.created"));

        assertThat(routeTree.resolve("orders.created"))
                .containsExactly("orders.created");
    }

    @Test
    void resolve_shouldMatchSingleSegmentWildcard() {
        RouteTree routeTree = new RouteTree();

        routeTree.attachQueue(QueuePath.of("orders.created"));
        routeTree.attachQueue(QueuePath.of("orders.deleted"));
        routeTree.attachQueue(QueuePath.of("orders.payment.failed"));

        assertThat(routeTree.resolve("orders.*"))
                .containsExactlyInAnyOrder("orders.created", "orders.deleted");
    }

    @Test
    void resolve_shouldMatchRecursiveWildcard() {
        RouteTree routeTree = new RouteTree();

        routeTree.attachQueue(QueuePath.of("orders.created"));
        routeTree.attachQueue(QueuePath.of("orders.payment.failed"));
        routeTree.attachQueue(QueuePath.of("payments.created"));

        assertThat(routeTree.resolve(("orders.#")))
                .containsExactlyInAnyOrder(
                        "orders.created",
                        "orders.payment.failed"
                );
    }

    @Test
    void detachQueue_shouldRemoveQueueFromRouteTree() {
        RouteTree routeTree = new RouteTree();

        QueuePath queuePath = QueuePath.of("orders.created");

        routeTree.attachQueue(queuePath);
        routeTree.detachQueue(queuePath);

        assertThat(routeTree.resolve(("orders.created")))
                .isEmpty();
    }
}