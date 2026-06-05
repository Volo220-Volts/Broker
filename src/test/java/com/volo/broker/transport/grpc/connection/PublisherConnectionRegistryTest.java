package com.volo.broker.transport.grpc.connection;

import com.custombroker.grpc.PublisherResponse;
import com.volo.broker.TestStreamObserver;
import com.volo.broker.transport.grpc.SafeGrpcObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PublisherConnectionRegistryTest {

    @Test
    void register_shouldStoreConnection() {
        PublisherConnectionRegistry registry = new PublisherConnectionRegistry();

        PublisherConnection connection = new PublisherConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<PublisherResponse>())
        );

        registry.register(connection);

        assertThat(registry.find(connection.connectionId()))
                .contains(connection);
    }

    @Test
    void unregister_shouldRemoveConnection() {
        PublisherConnectionRegistry registry = new PublisherConnectionRegistry();

        PublisherConnection connection = new PublisherConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<PublisherResponse>())
        );

        registry.register(connection);
        registry.unregister("publisher-1");

        assertThat(registry.find("publisher-1"))
                .isEmpty();
    }

    @Test
    void activeConnections_shouldReturnRegisteredConnections() {
        PublisherConnectionRegistry registry = new PublisherConnectionRegistry();

        PublisherConnection first = new PublisherConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<PublisherResponse>())
        );

        PublisherConnection second = new PublisherConnection(
                new SafeGrpcObserver<>(new TestStreamObserver<PublisherResponse>())
        );

        registry.register(first);
        registry.register(second);

        assertThat(registry.activeConnections())
                .containsExactlyInAnyOrder(first, second);
    }
}